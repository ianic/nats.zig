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
const max_reconnect: u8 = 60;
const reconnect_wait = time.ns_per_s * 2;

const log = std.log.scoped(.nats);

pub const ConnectOptions = struct {
    alloc: Allocator,
    host: []const u8 = "localhost",
    port: u16 = 4222,
};

pub fn connect(opt: ConnectOptions) !*Conn {
    try ignoreSigPipe();
    return try Conn.connect(opt);
}

fn ignoreSigPipe() !void {
    // write to the closed socket raises signal (which closes app by default)
    // instead of returning error
    // by ignoring we got error on socket write
    var act = os.Sigaction{
        .handler = .{ .sigaction = os.SIG.IGN },
        .mask = os.empty_sigset,
        .flags = 0,
    };
    try os.sigaction(os.SIG.PIPE, &act, null);
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
    ClientNotConnected,
};

pub const Conn = struct {
    const Status = enum {
        disconnected,
        connected,
        closing,
    };
    const ReadBuffer = RingBuffer(Op, op_read_buffer_size);
    const Self = @This();

    opt: ConnectOptions,
    alloc: Allocator,
    net_cli: ?net.tcp.Client,
    subs: *Subscriptions,
    op_builder: OpBuilder = OpBuilder{},
    err: ?anyerror = null,
    err_op: ?OpErr = null,
    info: ?Info = null,
    status: Status = .disconnected,
    write_mut: Thread.Mutex = Thread.Mutex{},
    read_buffer: ReadBuffer = ReadBuffer{},
    reader_trd: Thread = undefined,
    max_payload: u64 = 0,

    fn connect(opt: ConnectOptions) !*Conn {
        var alloc = opt.alloc;

        var conn = try alloc.create(Self);
        conn.* = Conn{
            .opt = opt,
            .alloc = alloc,
            .net_cli = null,
            .subs = try Subscriptions.init(alloc),
        };
        errdefer conn.deinit();

        try conn.tcpConnect();
        try conn.connectHandshake();
        conn.reader_trd = try Thread.spawn(.{}, Self.reader, .{conn});
        return conn;
    }

    fn tcpConnect(self: *Self) !void {
        const addr = net.ip.Address.initIPv4(try std.x.os.IPv4.parse("127.0.0.1"), 4222);
        const client = try net.tcp.Client.init(.ip, .{ .close_on_exec = true });
        try client.connect(addr);
        errdefer client.deinit();
        self.net_cli = client;
    }

    fn reconnectLoop(self: *Self) !void {
        @atomicStore(Status, &self.status, .disconnected, .SeqCst);
        self.netClose();

        var no: u8 = 0;
        while (true) {
            self.reconnect() catch |err| {
                self.netClose();
                log.err("reconnect {d}, error: {}", .{ no, err });
                no += 1;
                if (no == max_reconnect) {
                    return err;
                }
                time.sleep(reconnect_wait);
                continue;
            };
            return;
        }
    }

    fn reconnect(self: *Self) anyerror!void {
        try self.tcpConnect();
        try self.connectHandshake();
        try self.resubscribe();
    }

    fn resubscribe(self: *Self) !void {
        var iter = self.subs.iterator();
        while (iter.next()) |s| {
            try self.write(self.op_builder.sub(s.subject, s.sid), null);
        }
    }

    fn netRead(self: *Self, buf: []u8) !usize {
        if (self.net_cli) |c| {
            return try c.read(buf, 0);
        }
        return Error.ClientNotConnected;
    }

    fn netWrite(self: *Self, buf: []const u8) !void {
        if (self.net_cli) |c| {
            _ = c.write(buf, 0) catch |err| {
                log.err("publish: {}", .{err});
                self.netClose();
            };
            return;
        }
        return Error.ClientNotConnected;
    }

    fn netClose(self: *Self) void {
        if (self.net_cli) |c| {
            c.shutdown(.both) catch {};
        }
    }

    fn connectHandshake(self: *Self) anyerror!void {
        var parser = Parser.init(self.alloc);
        defer parser.deinit();
        var buf: [max_args_len]u8 = undefined;

        // expect INFO at start
        var offset = try self.netRead(buf[0..]);
        debugConnIn(buf[0..offset]);
        var op = try parser.readOp(buf[0..offset]);
        if (op != .info) {
            return Error.HandshakeFailed;
        }
        self.onInfo(op.info);

        // send CONNECT, PING
        _ = try self.netWrite(connect_op);
        debugConnOut(connect_op);
        _ = try self.netWrite(ping_op);
        debugConnOut(ping_op);

        // expect PONG
        offset = try self.netRead(buf[0..]);
        debugConnIn(buf[0..offset]);
        op = try parser.readOp(buf[0..offset]);
        if (op != .pong) {
            return Error.HandshakeFailed;
        }
        @atomicStore(Status, &self.status, .connected, .SeqCst);
    }

    fn reader(self: *Self) void {
        while (true) {
            self.readLoop() catch |err| {
                if (!self.isClosing()) {
                    log.err("read loop error: {}", .{err});
                }
            };

            if (self.isClosing()) {
                break;
            }
            self.reconnectLoop() catch |err| {
                log.err("reconnect failed {}", .{err});
                break;
            };
        }
        self.read_buffer.close();
    }

    fn readLoop(self: *Self) !void {
        var parser = Parser.init(self.alloc);
        defer parser.deinit();

        var buf: [conn_read_buffer_size]u8 = undefined;
        while (true) {
            var bytes_read = try self.netRead(buf[0..]);
            if (bytes_read == 0) {
                return Error.EOF;
            }
            debugConnIn(buf[0..bytes_read]);
            try parser.push(buf[0..bytes_read]);
            while (try parser.next()) |op| {
                self.read_buffer.put(op);
            }
        }
    }

    pub fn isClosing(self: *Self) bool {
        return @atomicLoad(Status, &self.status, .SeqCst) == .closing;
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
        if (self.info) |in| {
            in.deinit(self.alloc);
        }
        self.info = info;
        self.max_payload = info.max_payload;
    }

    fn onErr(self: *Self, err: OpErr) void {
        self.err = Error.ServerError;
        if (self.err_op) |eo| {
            eo.deinit(self.alloc);
        }
        log.warn("server ERR: {s}", .{err.desc});
        self.err_op = err;
    }

    fn onPing(self: *Self) void {
        self.write(pong_op, null) catch {};
    }

    fn write(self: *Self, op_buf: []const u8, data: ?[]const u8) !void {
        if (data) |d| {
            if (d.len > self.max_payload) {
                return Error.MaxPayloadExceeded;
            }
        }
        {
            self.write_mut.lock();
            defer self.write_mut.unlock();

            _ = try self.netWrite(op_buf);
            if (data) |d| {
                _ = try self.netWrite(d);
                _ = try self.netWrite(cr_lf);
            }
        }
        debugConnOut(op_buf);
        if (data) |d| {
            debugConnOut(d);
        }
    }

    // publish message data on the subject
    // data is not copied
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        var op_buf = self.op_builder.pubOp(subject, data.len);
        try self.write(op_buf, data);
    }

    // subscribes handler to the subject
    // returns subscription id for use in unsubscribe
    pub fn subscribe(self: *Self, subject: []const u8) !u64 {
        var sid = try self.subs.put(subject);
        errdefer {
            self.subs.remove(sid);
        }
        try self.write(self.op_builder.sub(subject, sid), null);
        return sid;
    }

    // use subscription id from subscribe to stop receiving messages
    pub fn unsubscribe(self: *Self, sid: u64) !void {
        try self.write(self.op_builder.unsub(sid), null);
        self.subs.remove(sid);
    }

    pub fn close(self: *Self) !void {
        @atomicStore(Status, &self.status, .closing, .SeqCst);
        self.netClose();
        Thread.join(self.reader_trd);
    }

    pub fn deinit(self: *Self) void {
        if (self.status == .connected) {
            self.close() catch {};
        }
        if (self.net_cli) |c| {
            c.deinit();
        }
        self.read_buffer.close();
        while (self.read_buffer.get()) |op| {
            op.deinit(self.alloc);
        }
        if (self.info) |in| {
            in.deinit(self.alloc);
        }
        if (self.err_op) |eo| {
            eo.deinit(self.alloc);
        }
        self.subs.deinit();
        self.alloc.destroy(self);
    }
};

const Subscription = struct {
    sid: u64,
    subject: []u8,

    const Self = @This();
    fn deinit(self: *Self, alloc: Allocator) void {
        alloc.free(self.subject);
    }
};

const SubsMap = std.AutoHashMap(u64, Subscription);

const Subscriptions = struct {
    sid: u64 = 0,
    alloc: Allocator,
    subs: std.AutoHashMap(u64, Subscription),
    mut: Thread.Mutex = Thread.Mutex{},
    const Self = @This();

    fn init(alloc: Allocator) !*Self {
        var ss = try alloc.create(Self);
        ss.* = .{
            .alloc = alloc,
            .subs = SubsMap.init(alloc),
        };
        return ss;
    }

    fn deinit(self: *Self) void {
        var iter = self.iterator();
        while (iter.next()) |s| {
            s.deinit(self.alloc);
        }
        self.subs.deinit();
        self.alloc.destroy(self);
    }

    fn put(self: *Self, subject: []const u8) !u64 {
        self.mut.lock();
        defer self.mut.unlock();

        self.sid += 1;
        var sid = self.sid;

        const subject_copy = try self.alloc.alloc(u8, subject.len);
        mem.copy(u8, subject_copy, subject);
        var sub = Subscription{
            .subject = subject_copy,
            .sid = sid,
        };
        try self.subs.put(sid, sub);
        return sid;
    }

    fn remove(self: *Self, sid: u64) void {
        self.mut.lock();
        defer self.mut.unlock();

        if (self.subs.getPtr(sid)) |s| {
            s.deinit(self.alloc);
            _ = self.subs.remove(sid);
        }
    }

    fn iterator(self: *Self) SubsMap.ValueIterator {
        self.mut.lock();
        defer self.mut.unlock();

        return self.subs.valueIterator();
    }
};

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

test "subscriptions put/remove" {
    var ss = try Subscriptions.init(std.testing.allocator);
    defer ss.deinit();

    var id1 = try ss.put("foo");
    var id2 = try ss.put("bar");
    var id3 = try ss.put("jozo");

    try expectEqual(@intCast(u64, 1), id1);
    try expectEqual(@intCast(u64, 2), id2);
    try expectEqual(@intCast(u64, 3), id3);
    try expectEqual(@intCast(u32, 3), ss.subs.count());

    ss.remove(id3);
    try expectEqual(@intCast(u32, 2), ss.subs.count());

    var i = ss.iterator();
    while (i.next()) |s| {
        if (!(std.mem.eql(u8, "foo", s.subject) or std.mem.eql(u8, "bar", s.subject))) {
            unreachable;
        }
    }
}

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
