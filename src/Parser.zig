const std = @import("std");
const print = std.debug.print;
const assert = std.debug.assert;
const net = std.net;
const fmt = std.fmt;
const mem = std.mem;
const event = std.event;
const Allocator = std.mem.Allocator;

// testing imports
const testing = std.testing;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

// TODO: you are repeating some of them in conn file and here
// constants
const max_args_len = 4096;
const empty_str = ([_]u8{})[0..]; // TODO check this empty string idea, trying to avoid undefined
const cr_lf = "\r\n";
const Self = @This();

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

pub const Msg = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    size: u64,
    payload: ?[]const u8 = null,
    alloc: Allocator,

    pub fn deinit(self: Msg) void {
        self.alloc.free(self.subject);
        if (self.reply != null) {
            self.alloc.free(self.reply.?);
        }
        if (self.payload != null) {
            self.alloc.free(self.payload.?);
        }
    }

    pub fn data(self: Msg) []const u8 {
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

const OpParseError = error{
    UnexpectedToken,
    MissingArguments,
    UnexpectedMsgArgs,
    ArgsTooLong,
    BufferNotConsumed,
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

// reads operations from the underlying reader in chunks
// calls handler for each parsed operation

var scratch: [max_args_len]u8 = undefined;

alloc: Allocator,
state: ParserState = .start,

args_buf: []u8 = scratch[0..],
args_buf_len: usize = 0,

payload_buf: []u8 = scratch[0..],
payload_buf_len: usize = 0,
payload_buf_owned: bool = false,

payload_size: usize = 0,
msg: ?Msg = null,

read_buffer: []const u8 = empty_str,
pos: usize = 0,
args_start: usize = 0,
drop: usize = 0,

pub fn init(alloc: Allocator) Self {
    return .{ .alloc = alloc };
}

// Push buffer to the parser.
// Parse buffer one operation at the time by calling next
// until next returns null.
pub fn push(self: *Self, buf: []const u8) !void {
    if (self.readBufferConsumed()) {
        self.read_buffer = buf;
        self.args_start = 0;
        self.drop = 0;
        self.pos = 0;
        return;
    }
    return OpParseError.BufferNotConsumed;
}

fn readBufferConsumed(self: *Self) bool {
    return ((self.read_buffer.len == 0) or
        (self.pos == self.read_buffer.len) or
        (self.pos == self.read_buffer.len - 1 and self.read_buffer[self.read_buffer.len - 1] == '\n') or
        (self.read_buffer.len > 1 and self.pos == self.read_buffer.len - 2 and self.read_buffer[self.read_buffer.len - 2] == '\r' and self.read_buffer[self.read_buffer.len - 1] == '\n'));
}

pub fn hasMore(self: *Self) bool {
    return !self.readBufferConsumed();
}

pub fn next(self: *Self) !?Op {
    const buf = self.read_buffer;

    var args_start: usize = self.args_start;
    var drop: usize = self.drop;
    var i: usize = self.pos;

    defer {
        self.args_start = args_start;
        self.drop = drop;
        self.pos = i;
    }

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
                        self.state = .start;
                        return Op.ping;
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
                        self.state = .start;
                        return Op.pong;
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
                        var op = try self.onInfo(buf[args_start .. i - drop]);
                        self.state = .start;
                        return op;
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
                        var op = try self.onErr(buf[args_start .. i - drop]);
                        self.state = .start;
                        return op;
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
                        self.state = .start;
                        return Op.ok;
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
                    var op = try self.onMsg(buf[i .. i + self.payload_size]);
                    i += self.payload_size;
                    self.payload_size = 0;
                    self.state = .start;
                    return op;
                } else {
                    // split buffer, save what we have so far
                    var pbuf = buf[i..];
                    try self.pushPayload(pbuf);
                    self.payload_size -= pbuf.len;
                    return null;
                }
            },
        }
    }

    // check for split buffer scenarios
    if (self.state == .info_args or self.state == .err_args or self.state == .msg_args) {
        try self.pushArgs(buf[args_start .. buf.len - drop]);
    }

    return null;
}

fn onMsg(self: *Self, buf: []const u8) !Op {
    var msg = self.msg.?;
    msg.payload = try self.getPayload(buf);
    self.msg = null;
    return Op{ .msg = msg };
}

fn onInfo(self: *Self, buf: []const u8) !Op {
    var args = try self.getArgs(buf);
    var info = try Info.jsonParse(self.alloc, args);
    return Op{ .info = info };
}

fn onErr(self: *Self, buf: []const u8) !Op {
    var desc = try self.alloc.dupe(u8, try self.getArgs(buf));
    var oe = OpErr{ .desc = desc, .alloc = self.alloc };
    return Op{ .err = oe };
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

fn setNext(self: *Self, buf: []const u8) !?Op {
    try self.push(buf);
    return self.next();
}

fn resetPayloadBuf(self: *Self) void {
    self.payload_buf = scratch[0..];
    self.payload_buf_len = 0;
    self.payload_buf_owned = false;
}

test "buffer not consumed" {
    var parser = init(testing.allocator);
    try parser.push(" ");
    const err = parser.push(" ");
    try expectError(OpParseError.BufferNotConsumed, err);
}

test "INFO operation" {
    const valid = [_][]const u8{
        "INFO {\"proto\":0}\r\n",
        "info {\"proto\":1}\n",
        "iNfO {\"proto\":2}\r\n",
        "InFo {\"proto\":3}\n",
    };

    var parser = init(testing.allocator);
    for (valid) |buf, i| {
        try parser.push(buf);
        const op = (try parser.next()).?;
        try expect(op.info.args.proto == i);
    }
}

test "INFO operation expect UnexpectedToken error" {
    const unexpected_token = [_][]const u8{ "INFOO something\r\n", "INFO_ something\r\n", "INFOsomething\r\n", "-err\n" };
    for (unexpected_token) |buf| {
        var parser = init(testing.allocator);
        try parser.push(buf);
        var err = parser.next();
        try expectError(OpParseError.UnexpectedToken, err);
    }
}

test "INFO operation expect MissingArguments error" {
    const missing_arguments = [_][]const u8{
        "INFO \r\n",
        "INFO \n",
        "-err \n",
        "-err\t   \r",
    };
    for (missing_arguments) |buf| {
        var parser = init(testing.allocator);
        try parser.push(buf);
        var err = parser.next();
        try expectError(OpParseError.MissingArguments, err);
    }
}

test "PING operation" {
    const pings = [_][]const u8{
        "PING\r\n",
        "ping\n",
        "ping     \n",
    };
    var parser = init(testing.allocator);
    for (pings) |buf| {
        try parser.push(buf);
        const op = (try parser.next()).?;
        try expect(op == .ping);
    }
}

test "PONG operation" {
    const pongs = [_][]const u8{
        "PONG\r\n",
        "pong\n",
        "pong     \n",
    };
    var parser = init(testing.allocator);
    for (pongs) |buf| {
        try parser.push(buf);
        const op = (try parser.next()).?;
        try expect(op == .pong);
    }
}

test "ERR operation" {
    const errs = [_][]const u8{
        "-ERR 'Stale Connection'\r\n",
        "-err 'Unknown Protocol Operation'\n",
        "-eRr\t'Permissions Violation for Subscription to foo.bar'\n",
    };
    var parser = init(testing.allocator);
    for (errs) |buf, i| {
        try parser.push(buf);
        const op = (try parser.next()).?;
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

test "max args line" {
    var parser = init(testing.allocator);

    var max_line = std.ArrayList(u8).init(testing.allocator);
    defer max_line.deinit();
    try max_line.appendSlice("-ERR ");
    try max_line.appendNTimes('a', max_args_len);

    try parser.push(max_line.items);
    const noop = try parser.next();
    try expect(noop == null);

    try parser.push(cr_lf);
    const op = (try parser.next()).?;
    try expect(op == .err);
    op.deinit();
}

test "args line too long" {
    var parser = init(testing.allocator);

    var too_long = std.ArrayList(u8).init(testing.allocator);
    defer too_long.deinit();
    try too_long.appendSlice("-ERR ");
    try too_long.appendNTimes('a', max_args_len + 1); // one more then above

    try parser.push(too_long.items);
    const err = parser.next();
    try expectError(OpParseError.ArgsTooLong, err);
}

test "split buffer scenarios" {
    var parser = init(testing.allocator);

    // sizes for the chunks we will push to the parser
    var sizes = [_]usize{ 2, 3, 5, 8, 13, 21, 34, 55, 89, 144 };
    for (sizes) |size| {
        try parser.push("INFO "); // start info message
        const noop = try parser.next();
        try expect(noop == null);

        // split long info args line into chunks of size
        var x: usize = 0;
        var y: usize = size;
        var buf = test_info_op;
        while (x < buf.len) {
            if (y > buf.len) {
                y = buf.len;
            }
            try parser.push(buf[x..y]); // push chunk
            const noop2 = try parser.next();
            try expect(noop2 == null);
            x = y;
            y += size;
        }

        try parser.push(cr_lf); // finish
        const op = (try parser.next()).?;
        try assertInfoArgs(op.info.args);
        op.deinit();
    }
}

const test_info_op =
    \\{"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
;

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

test "MSG with different payload sizes" {
    var parser = init(testing.allocator);

    var buf: [1024]u8 = undefined;
    var i: usize = 1;
    while (i < 1024) : (i += 1) {
        try expect((try parser.setNext("MSG subject 123 ")) == null);

        // create message with i payload size
        var offset = fmt.formatIntBuf(buf[0..], i, 10, .lower, .{});
        try expect((try parser.setNext(buf[0..offset])) == null);
        try expect((try parser.setNext(cr_lf)) == null);
        const op = (try parser.setNext(buf[0..i])).?;

        // assert operation
        try expect(mem.eql(u8, "subject", op.msg.subject));
        try expectEqual(i, op.msg.size);
        try expectEqual(i, op.msg.payload.?.len);
        op.deinit();
    }
}

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
