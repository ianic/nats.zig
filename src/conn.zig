const std = @import("std");
const assert = std.debug.assert;
const net = std.net;
const fmt = std.fmt;
const mem = std.mem;
const event = std.event;
const Allocator = std.mem.Allocator;

const Parser = @import("Parser.zig");
const Op = @import("Parser.zig").Op;
pub const Msg = @import("Parser.zig").Msg;
const Info = @import("Parser.zig").Info;
const OpErr = @import("Parser.zig").OpErr;

// testing imports
const testing = std.testing;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

// constants
const conn_read_buffer_size = 32768; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/nats.go#L479
const max_args_len = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
const cr_lf = "\r\n";
const connect_op = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
const pong_op = "PONG\r\n";
const ping_op = "PING\r\n";

pub fn connect(alloc: Allocator, loop: *event.Loop) !*Conn {
    return try Conn.connect(alloc, loop);
}

const log = std.log.scoped(.nats);
fn debugConnIn(buf: []const u8) void {
    if (buf[buf.len - 1] == '\n') {
        log.debug("> {s}", .{buf[0 .. buf.len - 1]});
    } else {
        log.debug("> {s}", .{buf[0..]});
    }
}
fn debugConnOut(buf: []const u8) void {
    if (buf[buf.len - 1] == '\n') {
        log.debug("< {s}", .{buf[0 .. buf.len - 1]});
    } else {
        log.debug("< {s}", .{buf[0..]});
    }
}

const ConnectError = error{
    HandshakeFailed,
    ServerError,
};

pub const Conn = struct {
    alloc: Allocator,
    stream: net.Stream,
    scratch: [max_args_len]u8 = undefined,
    ssid: u64 = 0,
    subs: std.AutoHashMap(u64, Subscription),
    op_builder: OpBuilder = OpBuilder{},
    err: ?anyerror = null,
    err_op: ?OpErr = null,
    info: ?Info = null,

    const Self = @This();

    pub fn connect(alloc: Allocator, loop: *event.Loop) !*Conn {
        if (!std.io.is_async) @compileError("Can't use in non-async mode!");

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
        try loop.runDetached(alloc, Conn.run, .{conn});
        return conn;
    }

    fn connectHandshake(self: *Self) !void {
        var parser = Parser.init(self.alloc);
        var buf: [max_args_len]u8 = undefined;

        // expect INFO at start
        var offset = try self.stream.read(buf[0..]);
        debugConnIn(buf[0..offset]);
        var op = try parser.readOp(buf[0..offset]);
        if (op != .info) {
            return ConnectError.HandshakeFailed;
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
            return ConnectError.HandshakeFailed;
        }
    }

    pub fn run(self: *Conn) void {
        self.readLoop() catch |err| {
            log.debug("run error {s}", .{err});
            self.err = err;
        };
    }

    fn readLoop(self: *Conn) !void {
        var parser = Parser.init(self.alloc);

        var buf: [conn_read_buffer_size]u8 = undefined;
        while (true) {
            var bytes_read = try self.stream.read(buf[0..]);
            if (bytes_read == 0) {
                return;
            }
            debugConnIn(buf[0..bytes_read]);
            try parser.push(buf[0..bytes_read]);
            while (try parser.next()) |op| {
                // TODO rethink exit
                //errdefer self.stream.close();
                try self.onOpOpt(op);
            }
        }
    }

    fn onOpOpt(self: *Self, op: Op) !void {
        switch (op) {
            .info => |info| {
                if (self.info) |in| {
                    in.deinit(self.alloc);
                }
                self.info = info;
            },
            .msg => |msg| {
                if (self.subs.get(msg.sid)) |sub| {
                    sub.handler.onMsg(msg);
                } else {
                    std.log.warn("no subscription for sid: {d}", .{msg.sid});
                }
                msg.deinit(self.alloc);
            },
            .ok => {},
            .err => |e| {
                self.err = ConnectError.ServerError;
                if (self.err_op) |eo| {
                    eo.deinit(self.alloc);
                }
                self.err_op = e;
            },
            .ping => nosuspend {
                try self.sendOp(pong_op);
            },
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
        try self.subs.put(sid, sub);
        errdefer _ = self.subs.remove(sid);
        try self.sendOp(self.op_builder.sub(subject, sid));
        return sid;
    }

    // use subscription id from subscribe to stop receiving messages
    pub fn unSubscribe(self: *Self, sid: u64) !void {
        nosuspend {
            try self.sendOp(self.op_builder.unsub(sid));
            _ = self.subs.remove(sid);
        }
    }

    pub fn close(self: *Self) void {
        self.stream.close();
    }

    pub fn deinit(self: *Self) void {
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
