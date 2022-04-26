const std = @import("std");
const assert = std.debug.assert;
const net = std.x.net;
const fmt = std.fmt;
const mem = std.mem;
const time = std.time;
const os = std.os;
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const Parser = @import("Parser.zig");
pub const Msg = Parser.Msg;
const Op = Parser.Op;
const Info = Parser.Info;
const OpErr = Parser.OpErr;

const RingBuffer = @import("RingBuffer.zig").RingBuffer;

// constants
const conn_read_buffer_size = 32768; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/nats.go#L479
const max_args_len = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
const op_read_buffer_size = 1024;
const cr_lf = "\r\n";
const connect_op = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
const pong_op = "PONG\r\n";
const ping_op = "PING\r\n";
const inbox_prefix = "_INBOX.";

const log = std.log.scoped(.nats);

pub fn connect(alloc: Allocator) !*Conn {
    return try Conn.connect(alloc);
}

var connToCloseOnTerm: ?*Conn = null;

fn sigCloseHandler(_: c_int) callconv(.C) void {
    if (connToCloseOnTerm) |nc| {
        nc.close() catch {};
    }
}

// catch sigint and close provided nc
pub fn closeOnTerm(nc: *Conn) !void {
    connToCloseOnTerm = nc;
    try os.sigaction(os.SIG.INT, &os.Sigaction{
        .handler = .{ .handler = sigCloseHandler },
        .mask = os.empty_sigset,
        .flags = 0,
    }, null);
}

const Error = error{
    HandshakeFailed,
    ServerError,
    EOF, // when no bytes are received on socket
    NotOpenForReading, // when socket is closed
    RequestTimeout,
    MaxPayloadExceeded,
    ConnectionResetByPeer,
};

pub const Conn = struct {
    const Status = enum {
        disconnected,
        connected,
        closing,
        //closed,
    };
    const ReadBuffer = RingBuffer(Op, op_read_buffer_size);
    const Self = @This();

    alloc: Allocator,
    client: net.tcp.Client,
    scratch: [max_args_len]u8 = undefined,
    ssid: u64 = 0,
    subs: std.AutoHashMap(u64, Subscription),
    op_builder: OpBuilder = OpBuilder{},
    err: ?anyerror = null,
    err_op: ?OpErr = null,
    info: ?Info = null,
    status: Status = .disconnected,
    mut: Thread.Mutex = Thread.Mutex{},
    write_mut: Thread.Mutex = Thread.Mutex{},
    read_buffer: ReadBuffer = ReadBuffer{},
    reader_trd: Thread = undefined,
    max_payload: u64 = 0,

    fn connect(alloc: Allocator) !*Conn {
        const addr = net.ip.Address.initIPv4(try std.x.os.IPv4.parse("127.0.0.1"), 4222);
        const client = try net.tcp.Client.init(.ip, .{ .close_on_exec = true });
        try client.connect(addr);
        errdefer client.deinit();

        var conn = try alloc.create(Self);
        conn.* = Conn{
            .client = client,
            .alloc = alloc,
            .subs = std.AutoHashMap(u64, Subscription).init(alloc),
        };
        errdefer conn.deinit();

        try conn.connectHandshake();
        conn.reader_trd = try Thread.spawn(.{}, Self.reader, .{conn});
        return conn;
    }

    fn connectHandshake(self: *Self) !void {
        var parser = Parser.init(self.alloc);
        defer parser.deinit();
        var buf: [max_args_len]u8 = undefined;

        // expect INFO at start
        var offset = try self.client.read(buf[0..], 0);
        debugConnIn(buf[0..offset]);
        var op = try parser.readOp(buf[0..offset]);
        if (op != .info) {
            return Error.HandshakeFailed;
        }
        self.onInfo(op.info);

        // send CONNECT, PING
        _ = try self.pubOp(connect_op);
        _ = try self.pubOp(ping_op);

        // expect PONG
        offset = try self.client.read(buf[0..], 0);
        debugConnIn(buf[0..offset]);
        op = try parser.readOp(buf[0..offset]);
        if (op != .pong) {
            return Error.HandshakeFailed;
        }
        self.status = .connected;
    }

    fn reader(self: *Self) void {
        self.readLoop() catch |err| {
            if (!(self.isClosed() and (err == Error.ConnectionResetByPeer or err == Error.EOF))) {
                self.err = err;
                log.err("reader error: {}", .{err});
            }
        };
        self.read_buffer.close();
    }

    fn isClosed(self: *Self) bool {
        self.mut.lock();
        defer self.mut.unlock();
        return self.status == .closing;
    }

    fn readLoop(self: *Self) !void {
        var parser = Parser.init(self.alloc);
        defer parser.deinit();

        var buf: [conn_read_buffer_size]u8 = undefined;
        while (true) {
            var bytes_read = try self.client.read(buf[0..], 0);
            if (bytes_read == 0) {
                if (self.status == .closing) {
                    return;
                }
                return Error.EOF;
            }
            debugConnIn(buf[0..bytes_read]);
            try parser.push(buf[0..bytes_read]);
            while (try parser.next()) |op| {
                self.read_buffer.put(op);
            }
        }
    }

    pub fn read(self: *Self) ?Msg {
        while (self.read_buffer.get()) |op| {
            switch (op) {
                .info => |info| self.onInfo(info),
                .err => |err| self.onErr(err),
                .msg => |msg| return msg,
                .ping => self.onPing(),
                .pong => {},
                .ok => {},
                else => unreachable,
            }
        }
        return null;
    }

    fn onInfo(self: *Self, info: Info) void {
        self.mut.lock();
        defer self.mut.unlock();
        if (self.info) |in| {
            in.deinit(self.alloc);
        }
        self.info = info;
        self.max_payload = info.max_payload;
    }

    fn onErr(self: *Self, err: OpErr) void {
        self.mut.lock();
        defer self.mut.unlock();
        self.err = Error.ServerError;
        if (self.err_op) |eo| {
            eo.deinit(self.alloc);
        }
        log.warn("server ERR: {s}", .{err.desc});
        self.err_op = err;
    }

    fn onPing(self: *Self) void {
        self.pubOp(pong_op) catch {};
    }

    fn pubOp(self: *Self, buf: []const u8) !void {
        {
            self.write_mut.lock();
            defer self.write_mut.unlock();
            _ = try self.client.write(buf, 0);
        }
        debugConnOut(buf);
    }

    fn pubMsg(self: *Self, op_buf: []const u8, data: []const u8) !void {
        {
            if (data.len > self.max_payload) {
                return Error.MaxPayloadExceeded;
            }
            self.write_mut.lock();
            defer self.write_mut.unlock();
            _ = try self.client.write(op_buf, 0);
            _ = try self.client.write(data, 0);
            _ = try self.client.write(cr_lf, 0);
        }
        debugConnOut(op_buf);
        debugConnOut(data);
    }

    // publish message data on the subject
    // data is not copied
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        var op_buf = self.op_builder.pubOp(subject, data.len);
        try self.pubMsg(op_buf, data);
    }

    // subscribes handler to the subject
    // returns subscription id for use in unSubscribe
    pub fn subscribe(self: *Self, subject: []const u8) !u64 {
        self.mut.lock();
        self.ssid += 1;
        var sid = self.ssid;
        var sub = Subscription{
            .sid = sid,
        };
        {
            defer self.mut.unlock();
            try self.subs.put(sid, sub);
        }
        errdefer {
            self.mut.lock();
            defer self.mut.unlock();
            _ = self.subs.remove(sid);
        }
        try self.pubOp(self.op_builder.sub(subject, sid));
        return sid;
    }

    // use subscription id from subscribe to stop receiving messages
    pub fn unsubscribe(self: *Self, sid: u64) !void {
        if (self.isClosed()) {
            return;
        }
        try self.pubOp(self.op_builder.unsub(sid));
        self.mut.lock();
        defer self.mut.unlock();
        _ = self.subs.remove(sid);

    }

    // pub fn request(self: *Self, subject: []const u8, data: []const u8) !Msg {
    //     var reply = "_INBOX.foo";
    //     var sid = try self.subscribe(reply, &MsgHandler.init(self));
    //     var req = Request{
    //         .frame = @frame(),
    //         .msg = undefined,
    //     };
    //     try self.requests.put(sid, &req);
    //     var op_buf = self.op_builder.req(subject, reply, data.len);
    //     try self.pubMsg(op_buf, data);
    //     suspend {

    //         // var loop = std.event.Loop.instance.?;
    //         // try loop.runDetached(self.alloc, timeoutRequest, .{self, sid});
    //     }
    //     var opt_req = self.requests.get(sid);
    //     if (opt_req) |r| {
    //         try self.unSubscribe(sid);
    //         _ = self.requests.remove(sid);
    //         return r.msg;
    //     }
    //     return Error.RequestTimeout;
    // }

    // fn timeoutRequest(self: *Self, sid: u64) void {
    //     time.sleep(1000 * time.ns_per_ms);
    //     if (self.requests.get(sid)) |req| {
    //         self.unSubscribe(sid) catch {};
    //         _ = self.requests.remove(sid);
    //         log.debug("timeout request {d}", .{sid});
    //         resume req.frame;
    //     } else {
    //         log.debug("nothing to timeout request {d}", .{sid});
    //     }
    // }

    // fn onMsg(self: *Self, msg: Msg) void {
    //     var opt_req = self.requests.get(msg.sid);
    //     if (opt_req) |req| {
    //         req.msg = msg;
    //         resume req.frame;
    //     } else {
    //         log.warn("no request found for sid: {d}", .{msg.sid});
    //     }
    // }

    pub fn close(self: *Self) !void {
        self.status = .closing;
        try self.client.shutdown(.both);
        Thread.join(self.reader_trd);
    }

    pub fn deinit(self: *Self) void {
        if (self.status == .connected) {
            self.close() catch {};
        }
        while (self.read_buffer.get()) |op| {
            op.deinit(self.alloc);
        }
        self.subs.deinit();
        if (self.info) |in| {
            in.deinit(self.alloc);
        }
        if (self.err_op) |eo| {
            eo.deinit(self.alloc);
        }
        self.alloc.destroy(self);
    }
};

const Subscription = struct {
    sid: u64,
};

const OpBuilder = struct {
    scratch: [max_args_len]u8 = undefined,

    const Self = @This();
    fn unsub(self: *Self, sid: u64) []const u8 {
        var buf = self.scratch[0..];
        mem.copy(u8, buf[0..], "UNSUB ");
        var offset: usize = 5;
        mem.copy(u8, buf[offset..], " ");
        offset += 1;
        offset += fmt.formatIntBuf(self.scratch[offset..], sid, 10, .lower, .{});
        mem.copy(u8, buf[offset..], cr_lf);
        offset += 2;
        return buf[0..offset];
    }

    fn sub(self: *Self, subject: []const u8, sid: u64) []const u8 {
        var buf = self.scratch[0..];
        mem.copy(u8, buf[0..], "SUB ");
        var offset: usize = 4;
        mem.copy(u8, buf[offset..], subject);
        offset += subject.len;
        mem.copy(u8, buf[offset..], " ");
        offset += 1;
        offset += fmt.formatIntBuf(self.scratch[offset..], sid, 10, .lower, .{});
        mem.copy(u8, buf[offset..], cr_lf);
        offset += 2;
        return buf[0..offset];
    }

    fn pubOp(self: *Self, subject: []const u8, size: u64) []const u8 {
        var buf = self.scratch[0..];
        mem.copy(u8, buf[0..], "PUB ");
        var offset: usize = 4;
        mem.copy(u8, buf[offset..], subject);
        offset += subject.len;
        mem.copy(u8, buf[offset..], " ");
        offset += 1;
        offset += fmt.formatIntBuf(self.scratch[offset..], size, 10, .lower, .{});
        mem.copy(u8, buf[offset..], cr_lf);
        offset += 2;
        return buf[0..offset];
    }

    fn req(self: *Self, subject: []const u8, reply: []const u8, size: u64) []const u8 {
        var buf = self.scratch[0..];
        mem.copy(u8, buf[0..], "PUB ");
        var offset: usize = 4;
        mem.copy(u8, buf[offset..], subject);
        offset += subject.len;
        mem.copy(u8, buf[offset..], " ");
        offset += 1;
        mem.copy(u8, buf[offset..], reply);
        offset += reply.len;
        mem.copy(u8, buf[offset..], " ");
        offset += 1;
        offset += fmt.formatIntBuf(self.scratch[offset..], size, 10, .lower, .{});
        mem.copy(u8, buf[offset..], cr_lf);
        offset += 2;
        return buf[0..offset];
    }
};

const expectEqualStrings = std.testing.expectEqualStrings;

test "operation builder" {
    var ob = OpBuilder{};

    try expectEqualStrings("UNSUB 1234\r\n", ob.unsub(1234));
    try expectEqualStrings("SUB foo.bar 4567\r\n", ob.sub("foo.bar", 4567));
    try expectEqualStrings("PUB foo.bar 8901\r\n", ob.pubOp("foo.bar", 8901));
    try expectEqualStrings("PUB foo.bar reply 2345\r\n", ob.req("foo.bar", "reply", 2345));
}

fn debugConnIn(buf: []const u8) void {
    logProtocolOp(">", buf);
}

fn debugConnOut(buf: []const u8) void {
    logProtocolOp("<", buf);
}

fn logProtocolOp(prefix: []const u8, buf: []const u8) void {
    if (buf.len == 0) {
        return;
    }
    var b = buf[0..];
    if (buf[buf.len - 1] == '\n') {
        b = buf[0 .. buf.len - 1];
    }
    log.debug("{s} {s}", .{ prefix, b });
}
