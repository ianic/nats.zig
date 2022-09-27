const std = @import("std");

const Self = @This();

pub const Error = error{
    SplitBuffer,
    UnexpectedToken,
    BadHeaderSize,
};

// Protocol operation tags
// ref: https://docs.nats.io/reference/reference-protocols/nats-protocol#protocol-messages
const OperationTag = enum {
    info,
    msg,
    hmsg,
    err,
    ok,
    ping,
    pong,
};

// Protocol operations
pub const Operation = union(OperationTag) {
    /// `INFO {["option_name":option_value],...}`
    info: Info,
    /// `MSG <subject> <#sid> [reply-to] <#bytes>\r\n[payload]\r\n`
    msg: Msg,
    /// `HMSG <subject> <#sid> [reply-to] <#header-bytes> <#total-bytes>\r\n<version-line>\r\n[headers]\r\n\r\n[payload]\r\n`
    hmsg: Msg,
    /// `-ERR <error message>`
    err: Err,
    /// `OK`
    ok,
    /// `PING`
    ping,
    /// `PONG`
    pong,
};

pub const Info = struct {
    args: []const u8,
};

pub const Err = struct {
    args: []const u8,
};

pub const Msg = struct {
    subject: []const u8,
    sid: u64, // subscription id
    reply_to: ?[]const u8,
    size: u64,
    payload: ?[]const u8 = null,
    header: ?[]const u8 = null,
};

source: []const u8, // buffer which we are parsing
index: usize = 0, // current parser position in source

parsed_index: usize = 0, // part of the source buffer which is parsed into opertations
operations: usize = 0,

pub fn init(source: []const u8) Self {
    return Self{
        .source = source,
    };
}

pub fn next(self: *Self) !Operation {
    const tag = try self.readOperation();
    self.index += 1; // we are now on the whitespace, move forward

    const op: Operation = switch (tag) {
        .ping => Operation.ping,
        .pong => Operation.pong,
        .ok => Operation.ok,
        .err => Operation{ .err = Err{ .args = try self.readArgs() } },
        .info => Operation{ .info = Info{ .args = try self.readArgs() } },
        .msg => try self.readMsg(),
        .hmsg => try self.readHmsg(),
    };
    self.eatNewLine();
    self.parsed_index = self.index;
    self.operations += 1;
    return op;
}

pub fn unparsed(self: *Self) []const u8 {
    return self.source[self.parsed_index..];
}

fn readMsg(self: *Self) !Operation {
    var msg = try self.msgArgs();
    if (msg.size > 0) {
        const payload_end = self.index + msg.size;
        if (self.source.len < payload_end) {
            return Error.SplitBuffer;
        }
        msg.payload = self.source[self.index..payload_end];
        self.index = payload_end;
    }
    return Operation{ .msg = msg };
}

fn readHmsg(self: *Self) !Operation {
    var hmsg = try self.hmsgArgs();
    var msg = hmsg.msg;
    if (msg.size > 0) {
        const payload_end = self.index + msg.size;
        if (self.source.len < payload_end) {
            return Error.SplitBuffer;
        }
        msg.header = self.source[self.index .. self.index + hmsg.header_size];
        msg.payload = self.source[self.index + hmsg.header_size .. payload_end];
        self.index = payload_end;
    }
    return Operation{ .hmsg = msg };
}

fn readOperation(self: *Self) !OperationTag {
    // parser states
    const State = enum {
        start,
        i,
        in,
        inf,
        info,
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
        m,
        ms,
        msg,
        plus,
        o,
        ok,
        h,
    };
    var state: State = .start;
    var hmsg = false;

    while (self.index < self.source.len) : (self.index += 1) {
        const c = self.source[self.index];
        state = switch (state) {
            .start => switch (c) {
                '\r', '\n' => .start,
                'I', 'i' => .i,
                'P', 'p' => .p,
                'M', 'm' => .m,
                'H', 'h' => blk: {
                    hmsg = true;
                    break :blk .h;
                },
                '-' => .minus,
                '+' => .plus,
                else => return Error.UnexpectedToken,
            },
            .p => switch (c) {
                'I', 'i' => .pi,
                'O', 'o' => .po,
                else => return Error.UnexpectedToken,
            },
            .pi => switch (c) {
                'N', 'n' => .pin,
                else => return Error.UnexpectedToken,
            },
            .pin => switch (c) {
                'G', 'g' => .ping,
                else => return Error.UnexpectedToken,
            },
            .ping => switch (c) {
                '\n' => return OperationTag.ping, // found ping operation
                else => .ping,
            },
            .po => switch (c) {
                'N', 'n' => .pon,
                else => return Error.UnexpectedToken,
            },
            .pon => switch (c) {
                'G', 'g' => .pong,
                else => return Error.UnexpectedToken,
            },
            .pong => switch (c) {
                '\n' => return OperationTag.pong, // found pong operation
                else => .pong,
            },
            .i => switch (c) {
                'N', 'n' => .in,
                else => return Error.UnexpectedToken,
            },
            .in => switch (c) {
                'F', 'f' => .inf,
                else => return Error.UnexpectedToken,
            },
            .inf => switch (c) {
                'O', 'o' => .info,
                else => return Error.UnexpectedToken,
            },
            .info => switch (c) {
                ' ', '\t' => return OperationTag.info, // found info operation
                else => return Error.UnexpectedToken,
            },
            .minus => switch (c) {
                'E', 'e' => .e,
                else => return Error.UnexpectedToken,
            },
            .e => switch (c) {
                'R', 'r' => .er,
                else => return Error.UnexpectedToken,
            },
            .er => switch (c) {
                'R', 'r' => .err,
                else => return Error.UnexpectedToken,
            },
            .err => switch (c) {
                ' ', '\t' => return OperationTag.err, // found err operation
                else => return Error.UnexpectedToken,
            },
            .plus => switch (c) {
                'O', 'o' => .o,
                else => return Error.UnexpectedToken,
            },
            .o => switch (c) {
                'K', 'k' => .ok,
                else => return Error.UnexpectedToken,
            },
            .ok => switch (c) {
                '\n' => return OperationTag.ok,
                else => .ok,
            },
            .h => switch (c) {
                'M', 'm' => .m,
                else => return Error.UnexpectedToken,
            },
            .m => switch (c) {
                'S', 's' => .ms,
                else => return Error.UnexpectedToken,
            },
            .ms => switch (c) {
                'G', 'g' => .msg,
                else => return Error.UnexpectedToken,
            },
            .msg => switch (c) {
                ' ', '\t' => return if (hmsg) OperationTag.hmsg else OperationTag.msg, // found msg/hmsg operation
                else => return Error.UnexpectedToken,
            },
        };
    }
    return Error.SplitBuffer;
}

const Loc = struct {
    start: usize = 0,
    end: usize = 0,

    fn empty(self: Loc) bool {
        return self.start == self.end;
    }
};

// from the current position to the end of the line
fn findArgsLine(self: *Self) !Loc {
    var loc = Loc{ .start = self.index };
    var drop: usize = 0;

    while (self.index < self.source.len) : (self.index += 1) {
        switch (self.source[self.index]) {
            ' ', '\t' => {
                // drop whitespace at start
                if (loc.start == self.index or loc.start == self.index - 1) {
                    loc.start = self.index + 1;
                    continue;
                }
            },
            '\r' => {
                drop = 1;
            },
            '\n' => {
                loc.end = self.index - drop;
                return loc;
            },
            else => {},
        }
    }
    return Error.SplitBuffer;
}

fn readArgs(self: *Self) ![]const u8 {
    const l = try self.findArgsLine();
    return self.source[l.start..l.end];
}

// from current location to the next whitespace (or end of line)
fn findArg(self: *Self) !Loc {
    var loc = Loc{ .start = self.index };
    var empty = true;
    while (self.index < self.source.len) : (self.index += 1) {
        switch (self.source[self.index]) {
            ' ', '\t' => {
                if (empty) {
                    loc.start = self.index + 1;
                    continue;
                }
                loc.end = self.index;
                return loc;
            },
            '\r', '\n' => {
                loc.end = self.index;
                return loc;
            },
            else => {
                empty = false;
            },
        }
    }
    return Error.SplitBuffer;
}

fn eatNewLine(self: *Self) void {
    while (self.index < self.source.len) : (self.index += 1) {
        switch (self.source[self.index]) {
            '\r', '\n' => {},
            else => return,
        }
    }
}

fn msgArgs(self: *Self) !Msg {
    const a1 = try self.findArg();
    const a2 = try self.findArg();
    const a3 = try self.findArg();
    const a4 = try self.findArg();
    var msg = Msg{
        .subject = self.source[a1.start..a1.end],
        .sid = try std.fmt.parseUnsigned(u64, self.source[a2.start..a2.end], 10),
        .size = 0,
        .reply_to = null,
        .payload = null,
    };
    if (a4.empty()) {
        msg.size = try std.fmt.parseUnsigned(u64, self.source[a3.start..a3.end], 10);
    } else {
        msg.reply_to = self.source[a3.start..a3.end];
        msg.size = try std.fmt.parseUnsigned(u64, self.source[a4.start..a4.end], 10);
    }
    self.eatNewLine();
    return msg;
}

const Hmsg = struct {
    msg: Msg,
    header_size: usize,
};

fn hmsgArgs(self: *Self) !Hmsg {
    const a1 = try self.findArg();
    const a2 = try self.findArg();
    const a3 = try self.findArg();
    const a4 = try self.findArg();
    const a5 = try self.findArg();
    var msg = Msg{
        .subject = self.source[a1.start..a1.end],
        .sid = try std.fmt.parseUnsigned(u64, self.source[a2.start..a2.end], 10),
        .size = 0,
        .reply_to = null,
        .payload = null,
    };
    var header_size: usize = 0;
    if (a5.empty()) {
        header_size = try std.fmt.parseUnsigned(u64, self.source[a3.start..a3.end], 10);
        msg.size = try std.fmt.parseUnsigned(u64, self.source[a4.start..a4.end], 10);
    } else {
        msg.reply_to = self.source[a3.start..a3.end];
        header_size = try std.fmt.parseUnsigned(u64, self.source[a4.start..a4.end], 10);
        msg.size = try std.fmt.parseUnsigned(u64, self.source[a5.start..a5.end], 10);
    }
    if (msg.size < header_size) {
        return Error.BadHeaderSize;
    }
    self.eatNewLine();
    return Hmsg{
        .msg = msg,
        .header_size = header_size,
    };
}

// testing imports
const testing = std.testing;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;
const mem = std.mem;

test "PING operation" {
    const pings = [_][]const u8{
        "PING\r\n",
        "ping\n",
        "ping     \n",
    };
    for (pings) |buf| {
        var parser = init(buf);
        const op = try parser.next();
        try expect(op == .ping);
        try expect(op == OperationTag.ping);
        try expect(@as(OperationTag, op) == OperationTag.ping);
        try expectEqual(parser.parsed_index, buf.len);
        try expect(parser.unparsed().len == 0);
    }
}

test "PONG operation" {
    const pongs = [_][]const u8{
        "PONG\r\n",
        "pong\n",
        "pong     \n",
    };
    for (pongs) |buf| {
        var parser = init(buf);
        const op = try parser.next();
        try expect(op == .pong);
        try expect(parser.unparsed().len == 0);
    }
}

test "ERR operation" {
    const errs = [_][]const u8{
        "-ERR 'Stale Connection'\r\n",
        "-err 'Unknown Protocol Operation'\n",
        "-eRr\t'Permissions Violation for Subscription to foo.bar'\t\n",
    };
    for (errs) |buf, i| {
        var parser = init(buf);
        const op = try parser.next();
        try expect(op == .err);
        switch (i) {
            0 => try expect(mem.eql(u8, "'Stale Connection'", op.err.args)),
            1 => try expect(mem.eql(u8, "'Unknown Protocol Operation'", op.err.args)),
            2 => try expect(mem.eql(u8, "'Permissions Violation for Subscription to foo.bar'\t", op.err.args)),
            else => {},
        }
        try expect(parser.unparsed().len == 0);
    }
}

test "INFO operation" {
    const valid = [_][]const u8{
        "INFO {\"proto\":0}\r\n",
        "info {\"proto\":1}\n",
        "iNfO {\"proto\":2}\r\n",
        "InFo {\"proto\":3}\n",
    };
    var printBuf: [16]u8 = undefined;
    for (valid) |buf, i| {
        var parser = init(buf);
        const op = try parser.next();
        try expect(op == .info);
        try expect(parser.unparsed().len == 0);
        try expectEqualStrings(
            try std.fmt.bufPrint(printBuf[0..], "{{\"proto\":{d}}}", .{i}),
            op.info.args,
        );
    }
}

test "UnexpectedToken error" {
    const unexpected_token = [_][]const u8{
        "INFOO something\r\n",
        "INFO_ something\r\n",
        "INFOsomething\r\n",
        "-err\n",
        "-ok\n",
    };
    for (unexpected_token) |buf| {
        var parser = init(buf);
        var err = parser.next();
        try expectError(Error.UnexpectedToken, err);
    }
}

test "MSG operation" {
    const valid = [_][]const u8{
        "MSG subject 123 3\n123\n",
        "MSG \t subject  123\t3\r\n123\r\n",
    };
    for (valid) |buf| {
        var parser = init(buf);
        const op = try parser.next();
        try expect(op == .msg);
        try expectEqualStrings("subject", op.msg.subject);
        try expectEqual(op.msg.sid, 123);
        try expectEqual(op.msg.size, 3);
        try expectEqual(op.msg.payload.?.len, 3);
        try expectEqualStrings("123", op.msg.payload.?);
        try expectEqual(parser.parsed_index, buf.len);
        try expect(parser.unparsed().len == 0);
    }
}

test "MSG without payload" {
    const buf = "MSG subject 123 0\nPING\n";
    var parser = init(buf);
    const op = try parser.next();
    try expect(op == .msg);
    try expectEqualStrings("subject", op.msg.subject);
    try expectEqual(op.msg.sid, 123);
    try expectEqual(op.msg.size, 0);
    try expectEqual(op.msg.payload, null);
    const op2 = try parser.next();
    try expect(op2 == .ping);
    try expect(parser.unparsed().len == 0);
}

test "MSG with reply" {
    const buf = "msg bar.foo 10 INBOX.34 12\n012345678901\n";
    var parser = init(buf);
    const op = try parser.next();
    try expect(op == .msg);
    try expectEqualStrings("bar.foo", op.msg.subject);
    try expectEqual(op.msg.sid, 10);
    try expectEqual(op.msg.size, 12);
    try expectEqual(op.msg.payload.?.len, 12);
    try expectEqualStrings("INBOX.34", op.msg.reply_to.?);
    try expectEqualStrings("012345678901", op.msg.payload.?);
    try expectEqual(parser.parsed_index, buf.len);
    try expectEqual(buf[parser.parsed_index..].len, 0);
    try expect(parser.unparsed().len == 0);
}

test "split buffer" {
    const cases = [_]struct {
        buf: []const u8,
        ops: usize,
        parsed_index: usize,
        unparsed: []const u8,
    }{
        .{ .buf = "PING\nPONG\nMSG subject ", .ops = 2, .parsed_index = 10, .unparsed = "MSG subject " },
        .{ .buf = "PING\nPONG\n-err \nMSG subject 123 ", .ops = 3, .parsed_index = 16, .unparsed = "MSG subject 123 " },
    };

    for (cases) |c| {
        var parser = init(c.buf);
        var i: usize = 0;
        while (i < c.ops) : (i += 1) {
            _ = try parser.next();
        }
        const err = parser.next();
        try expectError(Error.SplitBuffer, err);
        try expectEqual(c.parsed_index, parser.parsed_index);
        try expectEqualStrings("MSG", c.buf[parser.parsed_index .. parser.parsed_index + 3]);
        try expectEqualStrings(c.unparsed, parser.unparsed());
        try expectEqual(c.ops, parser.operations);
    }
}

test "HMSG operation" {
    var parser = init("HMSG foo 123 3 8\r\nXXXhello\r");
    var op = try parser.next();
    try expect(op == .hmsg);
    try expectEqualStrings("foo", op.hmsg.subject);
    try expectEqual(op.hmsg.sid, 123);
    try expect(op.hmsg.payload.?.len == 5);
    try expect(op.hmsg.header.?.len == 3);
    try expectEqualStrings("hello", op.hmsg.payload.?);
    try expectEqualStrings("XXX", op.hmsg.header.?);

    parser = init("HMSG foo.bar 123 INBOX.22 3 14\r\nOK:hello world\r");
    op = try parser.next();
    try expect(op == .hmsg);
    try expect(op.hmsg.payload.?.len == 11);
    try expect(op.hmsg.header.?.len == 3);
    try expectEqualStrings("hello world", op.hmsg.payload.?);
    try expectEqualStrings("OK:", op.hmsg.header.?);
    try expectEqualStrings("foo.bar", op.hmsg.subject);
    try expectEqual(op.hmsg.sid, 123);
}
