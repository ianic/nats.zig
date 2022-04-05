const std = @import("std");
const print = std.debug.print;
const net = std.net;
const Allocator = std.mem.Allocator;
const bufferStream = std.io.fixedBufferStream;

pub fn main() !void {
    const addr = try net.Address.parseIp4("127.0.0.1", 4222);
    const stream = try net.tcpConnectToAddress(addr);
    defer stream.close();
    const rdr = stream.reader();

    const max_control_line_size = 4096;
    var buf: [max_control_line_size]u8 = undefined;
    var bytes = try rdr.read(&buf);

    print("got bytes: {d}\n", .{bytes});
    print("data: {s}\n", .{buf[0..bytes]});

    while (true) {
        bytes = try rdr.read(&buf);
        print("got bytes: {d}\n", .{bytes});
        print("data: {s}\n", .{buf[0..bytes]});
        if (bytes == 0) {
            break;
        }
    }
}

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
    info: struct { args: []const u8 },
    msg: struct { args: MsgArgs, payload: []const u8 },
    ok,
    err: struct { args: []const u8 },

    connect,
    pub_op,
    sub,
    unsub,

    ping,
    pong,

    hmsg,
};

// TODO: rethink this
const OpParseError = error{
    UnexpectedToken,
    MissingArguments,
    OutOfMemory,
    UnexpectedMsgArgs,
    ArgsTooLong,

    // parseInt
    Overflow,
    InvalidCharacter,

    TestUnexpectedResult,
    TestExpectedEqual,
};

const OpHandlerError = error{
    TestUnexpectedResult,
    TestExpectedEqual,
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
};

const max_args_len = 4096;

// reads operations from the underlying reader in chunks
// calls handler for each parsed operation
pub fn OpReader(
    comptime buffer_size: usize,
    comptime ReaderType: type,
    comptime OpHandlerType: type,
) type {
    return struct {
        parent: ReaderType,
        handler: OpHandlerType,
        state: ParserState = .start,
        alloc: Allocator,
        args_buf: [max_args_len]u8 = undefined,
        args_buf_len: usize = 0,
        payload_buf: ?std.ArrayList(u8) = null,
        payload_size: usize = 0,
        msg_args: ?MsgArgs = null,

        //const Error = ReaderType.ReadError || ParserError ;
        const Self = @This();

        pub fn loop(self: *Self) OpParseError!void {
            var scratch: [buffer_size]u8 = undefined;

            while (true) {
                var bytes_read = try self.parent.read(scratch[0..]);
                if (bytes_read == 0) {
                    return;
                }
                try self.parse(scratch[0..bytes_read]);
            }
        }

        fn parse(self: *Self, buf: []const u8) OpParseError!void {
            var args_start: usize = 0;
            var drop: usize = 0;

            var i: usize = 0;
            while (i < buf.len) : (i += 1) {
                var b = buf[i];
                switch (self.state) {
                    .start => {
                        args_start = 0;
                        drop = 0;
                        self.deinit_args();

                        switch (b) {
                            '\r', '\n' => {},
                            'I', 'i' => self.state = .i,
                            'P', 'p' => self.state = .p,
                            'M', 'm' => self.state = .m,
                            '-' => self.state = .minus,
                            else => {
                                //print("\nunexpected token '{s}' {d} {c}\n", .{buf, i, b});
                                return OpParseError.UnexpectedToken;
                            },

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
                                try self.handler.on_op(Op.ping);
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
                                try self.handler.on_op(Op.pong);
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
                                //print("\n[{d}..{d}]", .{ args_start, i - drop });
                                try self.on_info(buf[args_start .. i - drop]);
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
                                try self.on_err(buf[args_start .. i - drop]);
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
                                var ma = try parse_msg_args(self.alloc, try self.get_args(buf[args_start .. i - drop]));
                                self.msg_args = ma;
                                self.state = .msg_payload;
                                self.payload_size = ma.size;
                            },
                            else => {},
                        }
                    },
                    .msg_payload => {
                        if (buf.len >= i + self.payload_size) {
                            var payload = try self.get_payload(buf[i .. i + self.payload_size]);
                            try self.handler.on_op(Op{ .msg = .{
                                .args = self.msg_args.?,
                                .payload = payload,
                            } });
                            i += self.payload_size-1;
                            self.state = .start;
                        } else {
                            var pbuf = buf[i..];
                            try self.push_payload(pbuf);
                            self.payload_size -= pbuf.len;
                            return;
                        }
                    },
                }
            }

            // check for split buffer scenarios
            if (self.state == .info_args or self.state == .err_args or self.state == .msg_args) {
                try self.push_args(buf[args_start .. buf.len - drop]);
            }
        }

        fn on_info(self: *Self, buf: []const u8) OpParseError!void {
            try self.handler.on_op(Op{ .info = .{ .args = try self.get_args(buf) } });
        }

        fn on_err(self: *Self, buf: []const u8) OpParseError!void {
            try self.handler.on_op(Op{ .err = .{ .args = try self.get_args(buf) } });
        }

        fn get_args(self: *Self, buf: []const u8) ![]const u8 {
            if (self.args_buf_len == 0) {
                return buf;
            }
            try self.push_args(buf);
            var acc = self.args_buf[0..self.args_buf_len];
            self.args_buf_len = 0;
            return acc;
        }

        fn push_args(self: *Self, buf: []const u8) !void {
            if (self.args_buf_len+buf.len > max_args_len) {
                return OpParseError.ArgsTooLong;
            }
            mem.copy(u8, self.args_buf[self.args_buf_len..self.args_buf_len+buf.len], buf);
            self.args_buf_len += buf.len;
        }

        fn push_payload(self: *Self, buf: []const u8) !void {
            if (self.payload_buf) |*ab| {
                try ab.appendSlice(buf);
                return;
            }
            var ab = std.ArrayList(u8).init(self.alloc);
            try ab.appendSlice(buf);
            self.payload_buf = ab;
        }

        fn get_payload(self: *Self, buf: []const u8) ![]const u8 {
            if (self.payload_buf) |*ab| {
                try ab.appendSlice(buf);
                return ab.items;
            }
            return buf;
        }

        fn deinit_args(self: *Self) void {
            if (self.payload_buf) |*ab| {
                ab.deinit();
                self.payload_buf = null;
            }
        }

        pub fn deinit(self: *Self) void {
            self.deinit_args();
        }
    };
}
// reference implementation: https://github.com/nats-io/nats.go/blob/8af932f2076b3cab1a9a2f5aa2d9b59de2f1db6b/parser.go#L434
fn parse_msg_args(alloc: Allocator, buf: []const u8) !MsgArgs {
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

    //print("\nparts:{s}\n", .{parts});

    if (part_no == 3) {
        var sid = try std.fmt.parseUnsigned(u64, parts[1], 10);
        var size = try std.fmt.parseUnsigned(u64, parts[2], 10);
        return MsgArgs{
            .subject = try cp(alloc, parts[0]),
            .sid = sid,
            .reply = null,
            .size = size,
            .alloc = alloc,
        };
    }
    if (part_no == 4) {
        var sid = try std.fmt.parseUnsigned(u64, parts[1], 10);
        var size = try std.fmt.parseUnsigned(u64, parts[3], 10);
        var subject = try cp(alloc, parts[0]);
        errdefer alloc.free(subject);
        return MsgArgs{
            .subject = subject,
            .sid = sid,
            .reply = try cp(alloc, parts[2]),
            .size = size,
            .alloc = alloc,
        };
    }
    return OpParseError.UnexpectedMsgArgs;
}

fn cp(alloc: Allocator, src: []const u8) ![]u8 {
    const dest = try alloc.alloc(u8, src.len);
    mem.copy(u8, dest, src);
    return dest;
}

const MsgArgs = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    size: u64,
    alloc: Allocator,

    const Self = @This();

    fn deinit(self: Self) void {
        self.alloc.free(self.subject);
        if (self.reply != null) {
            self.alloc.free(self.reply.?);
        }
    }
};

pub fn opReader(
    alloc: Allocator,
    parent_stream: anytype,
    op_handler: anytype,
) OpReader(4096, @TypeOf(parent_stream), @TypeOf(op_handler)) {
    return .{
        .alloc = alloc,
        .parent = parent_stream,
        .handler = op_handler,
    };
}

pub fn tinyOpReader(
    alloc: Allocator,
    parent_stream: anytype,
    op_handler: anytype,
) OpReader(16, @TypeOf(parent_stream), @TypeOf(op_handler)) {
    return .{
        .alloc = alloc,
        .parent = parent_stream,
        .handler = op_handler,
    };
}

// TODO check this empty string idea, trying to avoid undefined
const empty_str = ([_]u8{})[0..];

const ServerInfo = struct {
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

    nonce: []u8 = ([_]u8{})[0..],
    cluster: []u8 = empty_str,
    connect_urls: [][]u8 = ([_][]u8{})[0..],

    ldm: bool = false,
    jetstream: bool = false,

    pub fn decode(alloc: Allocator, buf: []const u8) std.json.ParseError(ServerInfo)!ServerInfo {
        // fixing error: evaluation exceeded 1000 backwards branches
        // ref: https://github.com/ziglang/zig/issues/9728
        @setEvalBranchQuota(1024 * 8);
        var stream = std.json.TokenStream.init(buf);
        return try std.json.parse(ServerInfo, &stream, .{
            .allocator = alloc,
            .ignore_unknown_fields = true,
        });
    }

    pub fn deinit(self: ServerInfo, alloc: Allocator) void {
        std.json.parseFree(ServerInfo, self, .{
            .allocator = alloc,
        });
    }
};

const testing = std.testing;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const mem = std.mem;

test "parser info operation" {
    const Case = struct {
        buf: []const u8,
        args_buf_len: u32,
    };
    const valid = [_]Case{
        Case{
            .buf = "INFO 0123456789\r\n",
            .args_buf_len = 10,
        },
        Case{
            .buf = "info 01234567890\n",
            .args_buf_len = 11,
        },
        Case{
            .buf = "iNfO 012345678901\r\n",
            .args_buf_len = 12,
        },
    };

    for (valid) |v| {
        var op = try test_parse_line(v.buf);
        try expect(op.info.args.len == v.args_buf_len);
    }

    const invalid = [_][]const u8{ "INFOO something\r\n", "INFO_ something\r\n", "INFOsomething\r\n", "-err\n" };
    for (invalid) |buf| {
        var err = test_parse_line(buf);
        try expectError(OpParseError.UnexpectedToken, err);
    }

    const missing_arguments = [_][]const u8{
        "INFO \r\n",
        "INFO \n",
        "-err \n",
        "-err\t   \r",
    };
    for (missing_arguments) |buf| {
        var err = test_parse_line(buf);
        try expectError(OpParseError.MissingArguments, err);
    }
}

test "parse ping operation" {
    const pings = [_][]const u8{
        "PING\r\n",
        "ping\n",
        "ping     \n",
    };
    for (pings) |buf| {
        var op = try test_parse_line(buf);
        try expect(op == .ping);
    }
}

test "parse pong operation" {
    const pongs = [_][]const u8{
        "PONG\r\n",
        "pong\n",
        "pong     \n",
    };
    for (pongs) |buf| {
        var op = try test_parse_line(buf);
        try expect(op == .pong);
    }
}

test "parse err operation" {
    const errs = [_][]const u8{
        "-ERR 'Stale Connection'\r\n",
        "-err 'Unknown Protocol Operation'\n",
        "-eRr\t'Permissions Violation for Subscription to foo.bar'\n",
    };
    for (errs) |buf, i| {
        var op = try test_parse_line(buf);
        try expect(op == .err);
        switch (i) {
            0 => try expect(mem.eql(u8, "'Stale Connection'", op.err.args)),
            1 => try expect(mem.eql(u8, "'Unknown Protocol Operation'", op.err.args)),
            2 => try expect(mem.eql(u8, "'Permissions Violation for Subscription to foo.bar'", op.err.args)),
            else => {},
        }
    }
}

test "args line too long" {
    var max_line = std.ArrayList(u8).init(testing.allocator);
    defer max_line.deinit();
    try max_line.appendSlice("INFO ");
    try max_line.appendNTimes('a', max_args_len);
    try max_line.appendSlice("\r\n");
    var op = try test_parse_line(max_line.items);
    try expect(op == .info);

    var too_long = std.ArrayList(u8).init(testing.allocator);
    defer too_long.deinit();
    try too_long.appendSlice("INFO ");
    try too_long.appendNTimes('a', max_args_len+1);
    try too_long.appendSlice("\r\n");
    var err = test_parse_line(too_long.items);
    try expectError(OpParseError.ArgsTooLong, err);
}

test "decode server info operation JSON args into ServerInfo struct" {
    const buf =
        \\{"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
    ;

    var alloc = std.testing.allocator;
    var im = try ServerInfo.decode(alloc, buf);
    defer im.deinit(alloc);

    try expect(mem.eql(u8, "id", im.server_id));
    try expect(mem.eql(u8, "name", im.server_name));
    try expect(mem.eql(u8, "2.8.0", im.version));
    try expect(mem.eql(u8, "127.0.0.1", im.client_ip));
    try expect(mem.eql(u8, empty_str, im.nonce));
    try expectEqual(im.port, 4222);
    try expectEqual(im.proto, 1);
    try expectEqual(im.max_payload, 123456);
    try expectEqual(im.client_id, 53);
    try expect(im.headers);
    try expect(im.jetstream);

    try expectEqual(im.connect_urls.len, 3);
    try expect(mem.eql(u8, im.connect_urls[0], "10.0.0.184:4333"));
    try expect(mem.eql(u8, im.connect_urls[1], "192.168.129.1:4333"));
    try expect(mem.eql(u8, im.connect_urls[2], "192.168.192.1:4333"));
}

test "operation reader" {
    // opReader has buffer of 4096 bytes, big enough for whole stream of test data
    var opr = opReader(testing.allocator, bufferStream(test_info_ops), &assert_info_ops);
    defer opr.deinit();
    try opr.loop();
}

test "operation reader with buffer overflow" {
    // tinyOpReader has buffer of 16 bytes, overflow occures
    var opr = tinyOpReader(testing.allocator, bufferStream(test_info_ops), &assert_info_ops);
    defer opr.deinit();
    try opr.loop();
}

// define info operations for use in test
const test_info_ops =
    \\INFO {"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
    \\INFO foo
    \\INFO foo bar
    \\
;

// assert parsed operations based test_info_ops
var assert_info_ops = struct {
    no: usize = 0,
    const Self = @This();

    fn on_op(self: *Self, op: Op) OpHandlerError!void {
        switch (self.no) {
            0 => {
                try expectEqual(op.info.args[0], '{');
                try expectEqual(op.info.args[278], '}');
                try expectEqual(op.info.args.len, 279);
            },
            1 => {
                try expect(mem.eql(u8, op.info.args, "foo"));
            },
            2 => {
                try expect(mem.eql(u8, op.info.args, "foo bar"));
            },
            else => {},
        }
        self.no += 1;
    }
}{};

test "operation reader with buffer overflow for different buffer sizes" {
    var handler = struct {
        no: usize = 1,
        const Self = @This();

        fn on_op(self: *Self, op: Op) OpHandlerError!void {
            try expectEqual(self.no, op.info.args.len);
            self.no += 1;
        }
    }{};

    var cases = std.ArrayList(u8).init(testing.allocator);
    defer cases.deinit();

    var i: usize = 1;
    while (i < 1024) : (i += 1) {
        try cases.appendSlice("INFO ");
        var j: usize = 0;
        while (j < i) : (j += 1) {
            try cases.append('a');
        }
        try cases.appendSlice("\r\n");
    }
    while (i < 2048) : (i += 1) {
        try cases.appendSlice("INFO ");
        var j: usize = 0;
        while (j < i) : (j += 1) {
            try cases.append('a');
        }
        try cases.appendSlice("\n");
    }

    var stream = bufferStream(cases.items);
    // tinyOpReader has buffer of 16 bytes, overflow occures
    var br = tinyOpReader(testing.allocator, stream, &handler);
    defer br.deinit();
    try br.loop();
}

// test helper, parse one line and return parsed operation
fn test_parse_line(buf: []const u8) !Op {
    var handler = struct {
        last_op: Op = undefined,
        const Self = @This();
        fn on_op(self: *Self, op: Op) OpHandlerError!void {
            self.last_op = op;
        }
    }{};

    var opr = opReader(testing.allocator, bufferStream(buf), &handler);
    try opr.loop();
    return handler.last_op;
}

test "parse msg args" {
    var ma = try parse_msg_args(testing.allocator, "foo.bar 9 11");
    try expect(mem.eql(u8, ma.subject, "foo.bar"));
    try expect(ma.reply == null);
    try expectEqual(ma.sid, 9);
    try expectEqual(ma.size, 11);
    ma.deinit();

    ma = try parse_msg_args(testing.allocator, "bar.foo 10 INBOX.34 12");
    try expect(mem.eql(u8, ma.subject, "bar.foo"));
    try expect(mem.eql(u8, ma.reply.?, "INBOX.34"));
    try expectEqual(ma.sid, 10);
    try expectEqual(ma.size, 12);
    ma.deinit();

    ma = try parse_msg_args(testing.allocator, "bar.foo.bar    11\tINBOX.35           13 ");
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
        var err = parse_msg_args(testing.allocator, case);
        try expectError(OpParseError.UnexpectedMsgArgs, err);
    }

    const not_a_number_cases = [_][]const u8{
        "foo.bar 9 size_not_a_number",
        "bar.foo sid_not_a_number, 11",
    };
    for (not_a_number_cases) |case| {
        var err = parse_msg_args(testing.allocator, case);
        try expectError(OpParseError.InvalidCharacter, err);
    }
}

test "message operation reader" {
    var handler = struct {
        no: usize = 1,
        const Self = @This();

        fn on_op(self: *Self, op: Op) OpHandlerError!void {
            //print("subject: '{s}'\n", .{op.msg.args.subject});
            try expect(mem.eql(u8, "subject", op.msg.args.subject));
            try expectEqual(self.no, op.msg.args.size);
            try expectEqual(self.no, op.msg.payload.len);
            self.no += 1;
            op.msg.args.deinit();
        }
    }{};

    var cases = std.ArrayList(u8).init(testing.allocator);
    defer cases.deinit();

    var i: usize = 1;
    while (i < 1024) : (i += 1) {
        try cases.appendSlice("MSG subject 123 ");
        try std.fmt.format(cases.writer(), "{d}", .{i});
        try cases.appendSlice("\r\n");
        var j: usize = 0;
        while (j < i) : (j += 1) {
            try cases.append('a');
        }
    }
    //print("{s}", .{cases.items});

    var stream = bufferStream(cases.items);
    var br = tinyOpReader(testing.allocator, stream, &handler);
    defer br.deinit();
    try br.loop();
}
