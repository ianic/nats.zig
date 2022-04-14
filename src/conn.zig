const std = @import("std");
const assert = std.debug.assert;
const net = std.net;
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const Parser = @import("Parser.zig");
pub const Msg = Parser.Msg;
const Op = Parser.Op;
const Info = Parser.Info;
const OpErr = Parser.OpErr;

// testing imports
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const expectError = testing.expectError;

// constants
const conn_read_buffer_size = 32768; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/nats.go#L479
const max_args_len = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
const cr_lf = "\r\n";
const connect_op = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
const pong_op = "PONG\r\n";
const ping_op = "PING\r\n";
const log = std.log.scoped(.nats);

pub fn connect(alloc: Allocator) !*Conn {
    return try Conn.connect(alloc);
}

const Error = error{
    HandshakeFailed,
    ServerError,
    EOF, // when no bytes are received on socket
    NotOpenForReading, // when socket is closed
};

pub const Conn = struct {
    const Status = enum {
        disconnected,
        connected,
        closed,
    };

    alloc: Allocator,
    stream: net.Stream,
    scratch: [max_args_len]u8 = undefined,
    ssid: u64 = 0,
    subs: std.AutoHashMap(u64, Subscription),
    op_builder: OpBuilder = OpBuilder{},
    err: ?anyerror = null,
    err_op: ?OpErr = null,
    info: ?Info = null,
    status: Status = .disconnected,
    writeLock: std.event.Lock = .{},
    subsLock: std.event.RwLock = std.event.RwLock.init(),

    const Self = @This();

    fn connect(alloc: Allocator) !*Conn {
        const addr = try net.Address.parseIp4("127.0.0.1", 4222);
        const stream = try net.tcpConnectToAddress(addr);
        errdefer stream.close();

        var conn = try alloc.create(Self);
        conn.* = Conn{
            .stream = stream,
            .alloc = alloc,
            .subs = std.AutoHashMap(u64, Subscription).init(alloc),
        };
        errdefer conn.deinit();

        try conn.connectHandshake();
        return conn;
    }

    fn connectHandshake(self: *Self) !void {
        var parser = Parser.init(self.alloc);
        defer parser.deinit();
        var buf: [max_args_len]u8 = undefined;

        // expect INFO at start
        var offset = try self.stream.read(buf[0..]);
        debugConnIn(buf[0..offset]);
        var op = try parser.readOp(buf[0..offset]);
        if (op != .info) {
            return Error.HandshakeFailed;
        }
        self.info = op.info;

        // send CONNECT, PING
        _ = try self.sendOp(connect_op);
        _ = try self.sendOp(ping_op);

        // expect PONG
        offset = try self.stream.read(buf[0..]);
        debugConnIn(buf[0..offset]);
        op = try parser.readOp(buf[0..offset]);
        if (op != .pong) {
            return Error.HandshakeFailed;
        }
        self.status = .connected;
    }

    pub fn run(self: *Conn) !void {
        self.readLoop() catch |err| {
            if (err == Error.NotOpenForReading and self.status == .closed) {
                // this is expected error after closing stream in Conn.close()
                return;
            }
            if (self.err_op != null) {
                log.warn("run exit with last server ERR: {s}", .{self.err_op.?.desc});
            }
            log.warn("run error: {s}", .{err});
            return;
        };
    }

    fn readLoop(self: *Conn) !void {
        var parser = Parser.init(self.alloc);
        defer parser.deinit();

        var buf: [conn_read_buffer_size]u8 = undefined;
        while (true) {
            var bytes_read = try self.stream.read(buf[0..]);
            if (bytes_read == 0) {
                return Error.EOF;
            }
            debugConnIn(buf[0..bytes_read]);
            try parser.push(buf[0..bytes_read]);
            while (try parser.next()) |op| {
                // TODO rethink exit
                //errdefer self.stream.close();
                try self.onOp(op);
            }
        }
    }

    fn onOp(self: *Self, op: Op) !void {
        switch (op) {
            .info => |info| {
                if (self.info) |in| {
                    in.deinit(self.alloc);
                }
                self.info = info;
            },
            .msg => |msg| {
                const lock = self.subsLock.acquireRead();
                var opt_sub = self.subs.get(msg.sid);
                lock.release();

                if (opt_sub) |sub| {
                    sub.handler.onMsg(msg);
                } else {
                    std.log.warn("no subscription for sid: {d}", .{msg.sid});
                }
                msg.deinit(self.alloc);
            },
            .ok => {},
            .err => |e| {
                self.err = Error.ServerError;
                if (self.err_op) |eo| {
                    eo.deinit(self.alloc);
                }
                log.warn("server ERR: {s}", .{e.desc});
                self.err_op = e;
            },
            .ping => try self.sendOp(pong_op),
            else => {},
        }
    }

    fn sendOp(self: *Self, buf: []const u8) !void {
        debugConnOut(buf);
        _ = try self.stream.write(buf);
    }

    fn sendPayload(self: *Self, buf: []const u8) !void {
        debugConnOut(buf);
        _ = try self.stream.write(buf);
        _ = try self.stream.write(cr_lf);
    }

    // publish message data on the subject
    pub fn publish(self: *Self, subject: []const u8, data: []const u8) !void {
        var lock = self.writeLock.acquire();
        defer lock.release();

        try self.sendOp(self.op_builder.pubOp(subject, data.len));
        try self.sendPayload(data);
    }

    // subscribes handler to the subject
    // returns subscription id for use in unSubscribe
    pub fn subscribe(self: *Self, subject: []const u8, handler: *MsgHandler) !u64 {
        self.ssid += 1;
        var sid = self.ssid;
        var sub = Subscription{
            .handler = handler,
        };
        {
            var lock = self.subsLock.acquireWrite();
            defer lock.release();
            try self.subs.put(sid, sub);
        }
        errdefer {
            var lock = self.subsLock.acquireWrite();
            _ = self.subs.remove(sid);
            lock.release();
        }
        try self.sendOp(self.op_builder.sub(subject, sid));
        return sid;
    }

    // use subscription id from subscribe to stop receiving messages
    pub fn unSubscribe(self: *Self, sid: u64) !void {
        try self.sendOp(self.op_builder.unsub(sid));
        const lock = self.subsLock.acquireWrite();
        _ = self.subs.remove(sid);
        lock.release();
    }

    pub fn close(self: *Self) void {
        self.status = .closed;
        self.stream.close();
    }

    pub fn deinit(self: *Self) void {
        self.subsLock.deinit();
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
    handler: *MsgHandler,
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
};

test "operation builder" {
    var ob = OpBuilder{};

    try expectEqualStrings("UNSUB 1234\r\n", ob.unsub(1234));
    try expectEqualStrings("SUB foo.bar 4567\r\n", ob.sub("foo.bar", 4567));
    try expectEqualStrings("PUB foo.bar 8901\r\n", ob.pubOp("foo.bar", 8901));
}

// ref: https://revivalizer.xyz/post/the-missing-zig-polymorphism-reference/
pub const MsgHandler = struct {
    ptr: usize = undefined,
    v_table: struct {
        onMsg: fn (self: *MsgHandler, msg: Msg) void,
    } = undefined,

    pub fn onMsg(self: *MsgHandler, msg: Msg) void {
        return self.v_table.onMsg(self, msg);
    }

    pub fn init(pointer: anytype) MsgHandler {
        const Ptr = @TypeOf(pointer);
        assert(@typeInfo(Ptr) == .Pointer); // Must be a pointer
        assert(@typeInfo(Ptr).Pointer.size == .One); // Must be a single-item pointer
        assert(@typeInfo(@typeInfo(Ptr).Pointer.child) == .Struct); // Must point to a struct

        return MsgHandler{ .ptr = @ptrToInt(pointer), .v_table = .{
            .onMsg = struct {
                fn onMsg(self: *MsgHandler, msg: Msg) void {
                    return @intToPtr(Ptr, self.ptr).onMsg(msg);
                }
            }.onMsg,
        } };
    }
};

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
