const std = @import("std");
const print = std.debug.print;
const assert = std.debug.assert;
const net = std.net;
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;

// testing imports
const testing = std.testing;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

// constants
const conn_read_buffer_size = 4096;
const max_args_len = 4096;
const empty_str = ([_]u8{})[0..]; // TODO check this empty string idea, trying to avoid undefined
const cr_lf = "\r\n";
const connect_op = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
const pong_op = "PONG\r\n";
const ping_op = "PING\r\n";

pub fn connect(alloc: Allocator) !Conn {
    return try Conn.connect(alloc);
}

pub fn run(conn: *Conn) void {
    conn.run();
}

const log = std.log.scoped(.nats);
fn debugConnIn(buf: []const u8) void {
    if (buf[buf.len-1] == '\n') {
        log.debug("> {s}", .{buf[0..buf.len-1]});
    } else {
        log.debug("> {s}", .{buf[0..]});
    }
}
fn debugConnOut(buf: []const u8) void {
    if (buf[buf.len-1] == '\n') {
        log.debug("< {s}", .{buf[0..buf.len-1]});
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
    subs: std.AutoHashMap(u64, Subscription) = undefined,
    op_builder: OpBuilder = OpBuilder{},
    err: anyerror = undefined,
    err_op: ?OpErr = null,
    info: Info = undefined,

    const Self = @This();

    pub fn connect(alloc: Allocator) !Conn {
        const addr = try net.Address.parseIp4("127.0.0.1", 4222);
        const stream = try net.tcpConnectToAddress(addr);

        var conn = Conn{
            .stream = stream,
            .alloc = alloc,
        };
        errdefer stream.close();
        try conn.connectHandshake();
        conn.subs = std.AutoHashMap(u64, Subscription).init(alloc);
        return conn;
    }

    fn connectHandshake(self: *Self) !void {
        var handler = struct {
            got_info: bool = false,
            got_ok: bool = false,
            got_pong: bool = false,
            got_ping: bool = false,
            info: Info = undefined,
            fn onOp(h: *@This(), op: Op) void {
                switch (op) {
                    .info => |info| {
                        h.got_info = true;
                        h.info = info;
                    },
                    .ok => h.got_ok = true,
                    .ping => h.got_ping = true,
                    .pong => h.got_pong = true,
                    else => {
                        op.deinit();
                    },
                }
            }
        }{};
        var parser = Parser.init(self.alloc, &OpHandler.init(&handler));
        var buf: [max_args_len]u8 = undefined;

        // expect INFO at start
        var offset = try self.stream.read(buf[0..]);
        debugConnIn(buf[0..offset]);
        try parser.parse(buf[0..offset]);
        if (!handler.got_info) {
            return ConnectError.HandshakeFailed;
        }
        self.info = handler.info;

        // send CONNECT, PING
        _ = try self.sendOp(connect_op);
        _ = try self.sendOp(ping_op);

        // expect PONG
        offset = try self.stream.read(buf[0..]);
        debugConnIn(buf[0..offset]);
        try parser.parse(buf[0..offset]);
        if (!handler.got_pong) {
            return ConnectError.HandshakeFailed;
        }
    }

    pub fn run(self: *Conn) void {
        self.loop() catch |err| {
            self.err = err;
        };
    }

    fn loop(self: *Conn) !void {
        var parser = Parser.init(self.alloc, &OpHandler.init(self));

        var i: usize = 0;
        var buf: [conn_read_buffer_size]u8 = undefined;
        while (true) : (i += 1) {
            var bytes_read = try self.stream.read(buf[0..]);
            if (bytes_read == 0) {
                return;
            }
            debugConnIn(buf[0..bytes_read]);
            try parser.parse(buf[0..bytes_read]);
        }
    }

    fn onOp(self: *Self, op: Op) void {
        self.onOpOpt(op) catch |err| {
            self.err = err;
            self.stream.close(); // TODO is this stopping the loop
        };
    }

    fn onOpOpt(self: *Self, op: Op) !void {
        switch (op) {
            .info => |info| {
                self.info.deinit();
                self.info = info;
            },
            .msg => |msg| {
                if (self.subs.get(msg.sid)) |sub| {
                    sub.handler.onMsg(msg);
                } else {
                    std.log.warn("no subscription for sid: {d}", .{msg.sid});
                }
                msg.deinit();
            },
            .ok => {},
            .err => |e| {
                self.err = ConnectError.ServerError;
                if (self.err_op) |eo| {
                    eo.deinit();
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
        //self.stream.close();
        self.info.deinit();
        self.subs.deinit();
        if (self.err_op) |eo| {
            eo.deinit();
        }
    }
};

const Subscription = struct {
    handler: *MsgHandler,
};

// protocol operations
// ref: https://docs.nats.io/reference/reference-protocols/nats-protocol#protocol-messages
const OpName = enum {
    // sent by server
    info,
    msg,
    ok,
    err,
    // sent by client
    connect,
    pub_op, // pub is reserved word
    sub,
    unsub,
    // sent by both client and server
    ping,
    pong,
    // unsupported
    hmsg,
};

const Op = union(OpName) {
    info: Info,
    msg: Msg,
    ok,
    err: OpErr,

    connect,
    pub_op,
    sub,
    unsub,

    ping,
    pong,

    hmsg,

    fn deinit(op: Op) void {
        switch (op) {
            .info => op.info.deinit(),
            .msg => op.msg.deinit(),
            .err => op.err.deinit(),
            else => {},
        }
    }
};

const OpParseError = error{
    UnexpectedToken,
    MissingArguments,
    UnexpectedMsgArgs,
    ArgsTooLong,
};

const ParserState = enum {
    start,
    i,
    in,
    inf,
    info,
    info_, // info operation and space 'INFO '
    info_args, // info operation arguments
    p,
    pi,
    pin,
    ping,
    po,
    pon,
    pong,
    minus,
    e,
    er,
    err,
    err_,
    err_args,
    m,
    ms,
    msg,
    msg_,
    msg_args,
    msg_payload,
    plus,
    o,
    ok,
};

// reference implementation: https://github.com/nats-io/nats.go/blob/8af932f2076b3cab1a9a2f5aa2d9b59de2f1db6b/parser.go#L434
fn parseMsgArgs(alloc: Allocator, buf: []const u8) !Msg {
    var parts: [4][]const u8 = .{empty_str} ** 4;
    var part_no: usize = 0;

    var start: usize = 0;
    var started: bool = false;
    for (buf) |b, i| {
        switch (b) {
            ' ', '\t', '\r', '\n' => {
                if (started) {
                    if (part_no > 3) {
                        return OpParseError.UnexpectedMsgArgs;
                    }
                    parts[part_no] = buf[start..i];
                    part_no += 1;
                    started = false;
                }
            },
            else => {
                if (!started) {
                    started = true;
                    start = i;
                }
            },
        }
    }
    if (started) {
        if (part_no > 3) {
            return OpParseError.UnexpectedMsgArgs;
        }
        parts[part_no] = buf[start..];
        part_no += 1;
    }

    if (part_no == 3) {
        var sid = try fmt.parseUnsigned(u64, parts[1], 10);
        var size = try fmt.parseUnsigned(u64, parts[2], 10);
        return Msg{
            .subject = try alloc.dupe(u8, parts[0]),
            .sid = sid,
            .reply = null,
            .size = size,
            .alloc = alloc,
        };
    }
    if (part_no == 4) {
        var sid = try fmt.parseUnsigned(u64, parts[1], 10);
        var size = try fmt.parseUnsigned(u64, parts[3], 10);
        var subject = try alloc.dupe(u8, parts[0]);
        errdefer alloc.free(subject);
        return Msg{
            .subject = subject,
            .sid = sid,
            .reply = try alloc.dupe(u8, parts[2]),
            .size = size,
            .alloc = alloc,
        };
    }
    return OpParseError.UnexpectedMsgArgs;
}

test "parse msg args" {
    var ma = try parseMsgArgs(testing.allocator, "foo.bar 9 11");
    try expect(mem.eql(u8, ma.subject, "foo.bar"));
    try expect(ma.reply == null);
    try expectEqual(ma.sid, 9);
    try expectEqual(ma.size, 11);
    ma.deinit();

    ma = try parseMsgArgs(testing.allocator, "bar.foo 10 INBOX.34 12");
    try expect(mem.eql(u8, ma.subject, "bar.foo"));
    try expect(mem.eql(u8, ma.reply.?, "INBOX.34"));
    try expectEqual(ma.sid, 10);
    try expectEqual(ma.size, 12);
    ma.deinit();

    ma = try parseMsgArgs(testing.allocator, "bar.foo.bar    11\tINBOX.35           13 ");
    try expect(mem.eql(u8, ma.subject, "bar.foo.bar"));
    try expect(mem.eql(u8, ma.reply.?, "INBOX.35"));
    try expectEqual(ma.sid, 11);
    try expectEqual(ma.size, 13);
    ma.deinit();

    const cases = [_][]const u8{
        "bar.foo 10 INBOX.34 extra.arg 12", // too many arguments
        "bar.foo 12", // too few arguments
    };
    for (cases) |case| {
        var err = parseMsgArgs(testing.allocator, case);
        try expectError(OpParseError.UnexpectedMsgArgs, err);
    }

    const not_a_number_cases = [_][]const u8{
        "foo.bar 9 size_not_a_number",
        "bar.foo sid_not_a_number, 11",
    };
    for (not_a_number_cases) |case| {
        var err = parseMsgArgs(testing.allocator, case);
        try expectError(fmt.ParseIntError.InvalidCharacter, err);
    }
}

pub const Msg = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    size: u64,
    payload: ?[]const u8 = null,
    alloc: Allocator,

    const Self = @This();

    fn deinit(self: Self) void {
        self.alloc.free(self.subject);
        if (self.reply != null) {
            self.alloc.free(self.reply.?);
        }
        if (self.payload != null) {
            self.alloc.free(self.payload.?);
        }
    }

    pub fn data(self: Self) []const u8 {
        if (self.payload) |p| {
            return p;
        } else {
            return empty_str;
        }
    }
};

const Info = struct {
    const Args = struct {
        // ref: https://docs.nats.io/reference/reference-protocols/nats-protocol#info
        // https://github.com/nats-io/nats.go/blob/e076b0dcab3193b8d7cf41c1b747355ad1302170/nats.go#L687
        server_id: []u8 = empty_str,
        server_name: []u8 = empty_str,
        proto: u32 = 1,
        version: []u8 = empty_str,
        host: []u8 = empty_str,
        port: u32 = 4222,

        headers: bool = false,
        auth_required: bool = false,
        tls_required: bool = false,
        tls_available: bool = false,

        max_payload: u64 = 1048576,
        client_id: u64 = 0,
        client_ip: []u8 = empty_str,

        nonce: []u8 = empty_str,
        cluster: []u8 = empty_str,
        connect_urls: [][]u8 = ([_][]u8{})[0..],

        ldm: bool = false,
        jetstream: bool = false,
    };

    args: Args,
    alloc: Allocator,

    fn jsonParse(alloc: Allocator, buf: []const u8) !Info {
        // fixing error: evaluation exceeded 1000 backwards branches
        // ref: https://github.com/ziglang/zig/issues/9728
        @setEvalBranchQuota(1024 * 8);
        var stream = std.json.TokenStream.init(buf);
        var args = try std.json.parse(Args, &stream, .{
            .allocator = alloc,
            .ignore_unknown_fields = true,
            //.allow_trailing_data = true,
        });
        return Info{
            .alloc = alloc,
            .args = args,
        };
    }

    fn deinit(self: Info) void {
        std.json.parseFree(Args, self.args, .{
            .allocator = self.alloc,
        });
    }
};

const OpErr = struct {
    desc: []const u8,
    alloc: Allocator,

    fn deinit(self: OpErr) void {
        self.alloc.free(self.desc);
    }
};

const test_info_op =
    \\{"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
;

test "decode server info operation JSON args into ServerInfo struct" {
    var alloc = std.testing.allocator;
    var si = try Info.jsonParse(alloc, test_info_op);
    defer si.deinit();

    try assertInfoArgs(si.args);
}

fn assertInfoArgs(args: Info.Args) !void {
    try expect(mem.eql(u8, "id", args.server_id));
    try expect(mem.eql(u8, "name", args.server_name));
    try expect(mem.eql(u8, "2.8.0", args.version));
    try expect(mem.eql(u8, "127.0.0.1", args.client_ip));
    try expect(mem.eql(u8, empty_str, args.nonce));
    try expectEqual(args.port, 4222);
    try expectEqual(args.proto, 1);
    try expectEqual(args.max_payload, 123456);
    try expectEqual(args.client_id, 53);
    try expect(args.headers);
    try expect(args.jetstream);

    try expectEqual(args.connect_urls.len, 3);
    try expect(mem.eql(u8, args.connect_urls[0], "10.0.0.184:4333"));
    try expect(mem.eql(u8, args.connect_urls[1], "192.168.129.1:4333"));
    try expect(mem.eql(u8, args.connect_urls[2], "192.168.192.1:4333"));
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

//// Parser

// ref: https://revivalizer.xyz/post/the-missing-zig-polymorphism-reference/
pub const OpHandler = struct {
    ptr: usize = undefined,
    v_table: struct {
        onOp: fn (self: *OpHandler, op: Op) void,
    } = undefined,

    pub fn onOp(self: *OpHandler, op: Op) void {
        return self.v_table.onOp(self, op);
    }

    pub fn init(pointer: anytype) OpHandler {
        const Ptr = @TypeOf(pointer);
        assert(@typeInfo(Ptr) == .Pointer); // Must be a pointer
        assert(@typeInfo(Ptr).Pointer.size == .One); // Must be a single-item pointer
        assert(@typeInfo(@typeInfo(Ptr).Pointer.child) == .Struct); // Must point to a struct

        return OpHandler{ .ptr = @ptrToInt(pointer), .v_table = .{
            .onOp = struct {
                fn onOp(self: *OpHandler, op: Op) void {
                    return @intToPtr(Ptr, self.ptr).onOp(op);
                }
            }.onOp,
        } };
    }
};

// reads operations from the underlying reader in chunks
// calls handler for each parsed operation
const Parser = struct {
    var scratch: [max_args_len]u8 = undefined;

    handler: *OpHandler,
    alloc: Allocator,
    state: ParserState = .start,

    args_buf: []u8 = scratch[0..],
    args_buf_len: usize = 0,

    payload_buf: []u8 = scratch[0..],
    payload_buf_len: usize = 0,
    payload_buf_owned: bool = false,

    payload_size: usize = 0,
    msg: ?Msg = null,

    const Self = @This();

    fn init(alloc: Allocator, handler: *OpHandler) Parser {
        return .{ .alloc = alloc, .handler = handler };
    }

    fn parse(self: *Self, buf: []const u8) !void {
        var args_start: usize = 0;
        var drop: usize = 0;

        var i: usize = 0;
        while (i < buf.len) : (i += 1) {
            var b = buf[i];
            switch (self.state) {
                .start => {
                    args_start = 0;
                    drop = 0;

                    switch (b) {
                        '\r', '\n' => {},
                        'I', 'i' => self.state = .i,
                        'P', 'p' => self.state = .p,
                        'M', 'm' => self.state = .m,
                        '-' => self.state = .minus,
                        '+' => self.state = .plus,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .p => {
                    switch (b) {
                        'I', 'i' => self.state = .pi,
                        'O', 'o' => self.state = .po,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .pi => {
                    switch (b) {
                        'N', 'n' => self.state = .pin,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .pin => {
                    switch (b) {
                        'G', 'g' => self.state = .ping,

                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .ping => {
                    switch (b) {
                        '\n' => {
                            self.handler.onOp(Op.ping);
                            self.state = .start;
                        },
                        else => {},
                    }
                },
                .po => {
                    switch (b) {
                        'N', 'n' => self.state = .pon,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .pon => {
                    switch (b) {
                        'G', 'g' => self.state = .pong,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .pong => {
                    switch (b) {
                        '\n' => {
                            self.handler.onOp(Op.pong);
                            self.state = .start;
                        },
                        else => {},
                    }
                },
                .i => {
                    switch (b) {
                        'N', 'n' => self.state = .in,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .in => {
                    switch (b) {
                        'F', 'f' => self.state = .inf,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .inf => {
                    switch (b) {
                        'O', 'o' => self.state = .info,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .info => {
                    switch (b) {
                        ' ', '\t' => self.state = .info_,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .info_ => {
                    switch (b) {
                        ' ', '\t' => {},
                        '\r', '\n' => return OpParseError.MissingArguments,
                        else => {
                            self.state = .info_args;
                            args_start = i;
                        },
                    }
                },
                .info_args => {
                    switch (b) {
                        '\r' => {
                            drop = 1;
                        },
                        '\n' => {
                            try self.onInfo(buf[args_start .. i - drop]);
                            self.state = .start;
                        },
                        else => {},
                    }
                },
                .minus => {
                    switch (b) {
                        'E', 'e' => self.state = .e,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .e => {
                    switch (b) {
                        'R', 'r' => self.state = .er,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .er => {
                    switch (b) {
                        'R', 'r' => self.state = .err,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .err => {
                    switch (b) {
                        ' ', '\t' => {
                            self.state = .err_;
                        },
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .err_ => {
                    switch (b) {
                        ' ', '\t' => {},
                        '\r', '\n' => return OpParseError.MissingArguments,
                        else => {
                            self.state = .err_args;
                            args_start = i;
                        },
                    }
                },
                .err_args => {
                    switch (b) {
                        '\r' => {
                            drop = 1;
                        },
                        '\n' => {
                            try self.onErr(buf[args_start .. i - drop]);
                            self.state = .start;
                        },
                        else => {},
                    }
                },
                .plus => {
                    switch (b) {
                        'O', 'o' => self.state = .o,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .o => {
                    switch (b) {
                        'K', 'k' => self.state = .ok,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .ok => {
                    switch (b) {
                        '\n' => {
                            self.handler.onOp(Op.ok);
                            self.state = .start;
                        },
                        else => {},
                    }
                },
                .m => {
                    switch (b) {
                        'S', 's' => self.state = .ms,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .ms => {
                    switch (b) {
                        'G', 'g' => self.state = .msg,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .msg => {
                    switch (b) {
                        ' ', '\t' => self.state = .msg_,
                        else => return OpParseError.UnexpectedToken,
                    }
                },
                .msg_ => {
                    switch (b) {
                        ' ', '\t' => {},
                        '\r', '\n' => return OpParseError.MissingArguments,
                        else => {
                            self.state = .msg_args;
                            args_start = i;
                        },
                    }
                },
                .msg_args => {
                    switch (b) {
                        '\r' => {
                            drop = 1;
                        },
                        '\n' => {
                            var msg = try parseMsgArgs(self.alloc, try self.getArgs(buf[args_start .. i - drop]));
                            self.msg = msg;
                            self.state = .msg_payload;
                            self.payload_size = msg.size;
                        },
                        else => {},
                    }
                },
                .msg_payload => {
                    if (buf.len >= i + self.payload_size) {
                        try self.onMsg(buf[i .. i + self.payload_size]);
                        i += self.payload_size - 1;
                        self.payload_size = 0;
                        self.state = .start;
                    } else {
                        // split buffer, save what we have so far
                        var pbuf = buf[i..];
                        try self.pushPayload(pbuf);
                        self.payload_size -= pbuf.len;
                        return;
                    }
                },
            }
        }

        // check for split buffer scenarios
        if (self.state == .info_args or self.state == .err_args or self.state == .msg_args) {
            try self.pushArgs(buf[args_start .. buf.len - drop]);
        }
    }

    fn onMsg(self: *Self, buf: []const u8) !void {
        var msg = self.msg.?;
        msg.payload = try self.getPayload(buf);
        self.handler.onOp(Op{ .msg = msg });
        self.msg = null;
    }

    fn onInfo(self: *Self, buf: []const u8) !void {
        var args = try self.getArgs(buf);
        var info = try Info.jsonParse(self.alloc, args);
        self.handler.onOp(Op{ .info = info });
    }

    fn onErr(self: *Self, buf: []const u8) !void {
        var desc = try self.alloc.dupe(u8, try self.getArgs(buf));
        var oe = OpErr{ .desc = desc, .alloc = self.alloc };
        self.handler.onOp(Op{ .err = oe });
    }

    fn getArgs(self: *Self, buf: []const u8) ![]const u8 {
        if (self.args_buf_len == 0) {
            return buf;
        }
        try self.pushArgs(buf);
        var acc = self.args_buf[0..self.args_buf_len];
        self.args_buf_len = 0;
        return acc;
    }

    fn pushArgs(self: *Self, buf: []const u8) !void {
        if (self.args_buf_len + buf.len > max_args_len) {
            return OpParseError.ArgsTooLong;
        }
        mem.copy(u8, self.args_buf[self.args_buf_len .. self.args_buf_len + buf.len], buf);
        self.args_buf_len += buf.len;
    }

    fn pushPayload(self: *Self, buf: []const u8) !void {
        var new_len = self.payload_buf_len + buf.len;
        if (new_len > self.payload_buf.len) {
            const dest = try self.alloc.alloc(u8, new_len);
            mem.copy(u8, dest, self.payload_buf);
            if (self.payload_buf_owned) {
                self.alloc.free(self.payload_buf);
            }
            self.payload_buf = dest;
            self.payload_buf_owned = true;
        }
        mem.copy(u8, self.payload_buf[self.payload_buf_len..new_len], buf);
        self.payload_buf_len = new_len;
    }

    fn getPayload(self: *Self, buf: []const u8) ![]const u8 {
        if (self.payload_buf_len > 0) {
            try self.pushPayload(buf);
            defer self.resetPayloadBuf();
            if (self.payload_buf_owned) {
                return self.payload_buf[0..self.payload_buf_len];
            }
            return try self.alloc.dupe(u8, self.payload_buf[0..self.payload_buf_len]);
        }
        return try self.alloc.dupe(u8, buf);
    }

    fn resetPayloadBuf(self: *Self) void {
        self.payload_buf = scratch[0..];
        self.payload_buf_len = 0;
        self.payload_buf_owned = false;
    }
};

test "parse INFO operation" {
    const valid = [_][]const u8{
        "INFO {\"proto\":0}\r\n",
        "info {\"proto\":1}\n",
        "iNfO {\"proto\":2}\r\n",
        "InFo {\"proto\":3}\n",
    };

    for (valid) |buf, i| {
        var op = try testParseBuf(buf);
        try expect(op.info.args.proto == i);
    }

    const unexpected_token = [_][]const u8{ "INFOO something\r\n", "INFO_ something\r\n", "INFOsomething\r\n", "-err\n" };
    for (unexpected_token) |buf| {
        var err = testParseBuf(buf);
        try expectError(OpParseError.UnexpectedToken, err);
    }

    const missing_arguments = [_][]const u8{
        "INFO \r\n",
        "INFO \n",
        "-err \n",
        "-err\t   \r",
    };
    for (missing_arguments) |buf| {
        var err = testParseBuf(buf);
        try expectError(OpParseError.MissingArguments, err);
    }
}

const TestOpHandler = struct {
    ops: std.ArrayList(Op),

    fn init() TestOpHandler {
        return .{
            .ops = std.ArrayList(Op).init(testing.allocator),
        };
    }

    const Self = @This();
    fn onOp(self: *Self, op: Op) void {
        self.ops.append(op) catch unreachable;
    }

    fn count(self: *Self) usize {
        return self.ops.items.len;
    }

    fn last(self: *Self) Op {
        return self.ops.items[self.ops.items.len - 1];
    }

    fn deinit(self: *Self) void {
        self.ops.deinit();
    }
};

fn testParseBuf(buf: []const u8) !Op {
    var handler: TestOpHandler = TestOpHandler.init();
    defer handler.deinit();
    var parser = Parser.init(testing.allocator, &OpHandler.init(&handler));
    try parser.parse(buf);
    return handler.last();
}

test "parse PING operation" {
    const pings = [_][]const u8{
        "PING\r\n",
        "ping\n",
        "ping     \n",
    };
    for (pings) |buf| {
        var op = try testParseBuf(buf);
        try expect(op == .ping);
    }
}

test "parse PONG operation" {
    const pongs = [_][]const u8{
        "PONG\r\n",
        "pong\n",
        "pong     \n",
    };
    for (pongs) |buf| {
        var op = try testParseBuf(buf);
        try expect(op == .pong);
    }
}

test "parse ERR operation" {
    const errs = [_][]const u8{
        "-ERR 'Stale Connection'\r\n",
        "-err 'Unknown Protocol Operation'\n",
        "-eRr\t'Permissions Violation for Subscription to foo.bar'\n",
    };
    for (errs) |buf, i| {
        var op = try testParseBuf(buf);
        try expect(op == .err);
        switch (i) {
            0 => try expect(mem.eql(u8, "'Stale Connection'", op.err.desc)),
            1 => try expect(mem.eql(u8, "'Unknown Protocol Operation'", op.err.desc)),
            2 => try expect(mem.eql(u8, "'Permissions Violation for Subscription to foo.bar'", op.err.desc)),
            else => {},
        }
        op.deinit();
    }
}

test "args line too long" {
    var handler: TestOpHandler = TestOpHandler.init();
    defer handler.deinit();
    var parser = Parser.init(testing.allocator, &OpHandler.init(&handler));

    var max_line = std.ArrayList(u8).init(testing.allocator);
    defer max_line.deinit();
    try max_line.appendSlice("-ERR ");
    try max_line.appendNTimes('a', max_args_len);
    try parser.parse(max_line.items); // force into split buffer
    try parser.parse(cr_lf);
    var op = handler.last();
    try expect(op == .err);
    op.deinit();

    try parser.parse(max_line.items);
    var err = parser.parse("a"); // one more
    try expectError(OpParseError.ArgsTooLong, err);

    var too_long = std.ArrayList(u8).init(testing.allocator);
    defer too_long.deinit();
    try too_long.appendSlice("-ERR ");
    try too_long.appendNTimes('a', max_args_len + 1);
    err = parser.parse(too_long.items);
    try expectError(OpParseError.ArgsTooLong, err);
}

test "split buffer scenarios" {
    var handler: TestOpHandler = TestOpHandler.init();
    defer handler.deinit();
    var parser = Parser.init(testing.allocator, &OpHandler.init(&handler));

    var sizes = [_]usize{ 2, 3, 5, 8, 13, 21, 34, 55, 89, 144 };
    for (sizes) |size| {
        try parser.parse("INFO "); // start info message

        // split long info args line into chunks of size
        var x: usize = 0;
        var y: usize = size;
        var buf = test_info_op;
        while (x < buf.len) {
            if (y > buf.len) {
                y = buf.len;
            }
            try parser.parse(buf[x..y]); // push chunk
            x = y;
            y += size;
        }

        try parser.parse(cr_lf); // finish
        var op = handler.last();
        try assertInfoArgs(op.info.args);
        op.deinit();
    }
}

test "MSG with different payload sizes" {
    var handler: TestOpHandler = TestOpHandler.init();
    defer handler.deinit();
    var parser = Parser.init(testing.allocator, &OpHandler.init(&handler));

    var buf: [1024]u8 = undefined;
    var i: usize = 1;
    while (i < 1024) : (i += 1) {
        // create message with i payload size
        try parser.parse("MSG subject 123 ");
        var offset = fmt.formatIntBuf(buf[0..], i, 10, .lower, .{});
        try parser.parse(buf[0..offset]);
        try parser.parse(cr_lf);
        try parser.parse(buf[0..i]);
        try parser.parse(cr_lf);

        // assert operation
        var op = handler.last();
        try expect(mem.eql(u8, "subject", op.msg.subject));
        try expectEqual(i, op.msg.size);
        try expectEqual(i, op.msg.payload.?.len);
        op.deinit();
    }
}
