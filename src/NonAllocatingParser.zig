const std = @import("std");

const Self = @This();

pub const Error = error{
    SplitBuffer,
    UnexpectedToken,

    // TODO those below are still unused
    //UnsupportedOperation,
    MissingArguments,
    UnexpectedMsgArgs,
    ArgsTooLong,
    BufferNotConsumed,
    OpNotFound,
};

// protocol operation names
// ref: https://docs.nats.io/reference/reference-protocols/nats-protocol#protocol-messages
const OperationToken = enum {
    // sent by server
    info,
    msg,
    ok,
    err,
    // sent by client
    connect,
    @"pub",
    sub,
    unsub,
    // sent by both client and server
    ping,
    pong,
    // currently unsupported
    hmsg,
};

// protocol operations
pub const Operation = union(OperationToken) {
    info: Info,
    msg: Msg,
    ok,
    err: Err,

    connect,
    @"pub",
    sub,
    unsub,

    ping,
    pong,

    hmsg,
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
    reply: ?[]const u8,
    size: u64,
    payload: ?[]const u8 = null,
};

source: []const u8, // buffer which we are parsing
parsed_index: usize = 0, // part of the source buffer which is pared into opertations
index: usize = 0, // current parser position in source

pub fn init(source: []const u8) Self {
    return Self{
        .source = source,
    };
}

pub fn next(self: *Self) !Operation {
    const tag = try self.operation();
    const op: Operation = switch (tag) {
        .ping => Operation.ping,
        .pong => Operation.pong,
        .ok => Operation.ok,
        .err => Operation{ .err = Err{ .args = try self.argsLine() } },
        .info => Operation{ .info = Info{ .args = try self.argsLine() } },
        .msg => blk: {
            var msg = try self.msgArgs();
            if (msg.size > 0) {
                const payload_end = self.index + msg.size;
                if (self.source.len < payload_end) {
                    return Error.SplitBuffer;
                }
                msg.payload = self.source[self.index..payload_end];
                self.index = payload_end;
            }
            break :blk Operation{ .msg = msg };
        },
        else => unreachable,
    };
    self.eatNewLine();
    self.parsed_index = self.index;
    return op;
}

pub fn operation(self: *Self) !OperationToken {
    // parser states
    const State = enum {
        start,
        i,
        in,
        inf,
        info,
        //info_, // info operation and space 'INFO '
        //info_args, // info operation arguments
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
        //err_,
        //err_args,
        m,
        ms,
        msg,
        //msg_,
        //msg_args,
        //msg_payload,
        plus,
        o,
        ok,
    };
    var state: State = .start;

    while (self.index < self.source.len) : (self.index += 1) {
        const b = self.source[self.index];
        switch (state) {
            .start => {
                switch (b) {
                    '\r', '\n' => {},
                    'I', 'i' => state = .i,
                    'P', 'p' => state = .p,
                    'M', 'm' => state = .m,
                    '-' => state = .minus,
                    '+' => state = .plus,
                    else => return Error.UnexpectedToken,
                }
            },
            .p => {
                switch (b) {
                    'I', 'i' => state = .pi,
                    'O', 'o' => state = .po,
                    else => return Error.UnexpectedToken,
                }
            },
            .pi => {
                switch (b) {
                    'N', 'n' => state = .pin,
                    else => return Error.UnexpectedToken,
                }
            },
            .pin => {
                switch (b) {
                    'G', 'g' => state = .ping,
                    else => return Error.UnexpectedToken,
                }
            },
            .ping => {
                switch (b) {
                    '\n' => return OperationToken.ping, // found ping operation
                    else => {},
                }
            },
            .po => {
                switch (b) {
                    'N', 'n' => state = .pon,
                    else => return Error.UnexpectedToken,
                }
            },
            .pon => {
                switch (b) {
                    'G', 'g' => state = .pong,
                    else => return Error.UnexpectedToken,
                }
            },
            .pong => {
                switch (b) {
                    '\n' => return OperationToken.pong, // found pong operation
                    else => {},
                }
            },
            .i => {
                switch (b) {
                    'N', 'n' => state = .in,
                    else => return Error.UnexpectedToken,
                }
            },
            .in => {
                switch (b) {
                    'F', 'f' => state = .inf,
                    else => return Error.UnexpectedToken,
                }
            },
            .inf => {
                switch (b) {
                    'O', 'o' => state = .info,
                    else => return Error.UnexpectedToken,
                }
            },
            .info => {
                switch (b) {
                    ' ', '\t' => return OperationToken.info, // found info operation
                    else => return Error.UnexpectedToken,
                }
            },
            .minus => {
                switch (b) {
                    'E', 'e' => state = .e,
                    else => return Error.UnexpectedToken,
                }
            },
            .e => {
                switch (b) {
                    'R', 'r' => state = .er,
                    else => return Error.UnexpectedToken,
                }
            },
            .er => {
                switch (b) {
                    'R', 'r' => state = .err,
                    else => return Error.UnexpectedToken,
                }
            },
            .err => {
                switch (b) {
                    ' ', '\t' => return OperationToken.err, // found err operation
                    else => return Error.UnexpectedToken,
                }
            },
            .plus => {
                switch (b) {
                    'O', 'o' => state = .o,
                    else => return Error.UnexpectedToken,
                }
            },
            .o => {
                switch (b) {
                    'K', 'k' => state = .ok,
                    else => return Error.UnexpectedToken,
                }
            },
            .ok => {
                switch (b) {
                    '\n' => return OperationToken.ok,
                    else => {},
                }
            },
            .m => {
                switch (b) {
                    'S', 's' => state = .ms,
                    else => return Error.UnexpectedToken,
                }
            },
            .ms => {
                switch (b) {
                    'G', 'g' => state = .msg,
                    else => return Error.UnexpectedToken,
                }
            },
            .msg => {
                switch (b) {
                    ' ', '\t' => return OperationToken.msg, // found msg operation
                    else => return Error.UnexpectedToken,
                }
            },
        }
    }
    return Error.SplitBuffer;
}

pub fn skipWhitespace(self: *Self) void {
    while (self.index < self.source.len) : (self.index += 1) {
        const b = self.source[self.index];
        switch (b) {
            ' ', '\t' => {},
            else => return,
        }
    }
}

pub const Loc = struct {
    start: usize = 0,
    end: usize = 0,

    pub fn empty(self: Loc) bool {
        return self.start == self.end;
    }
};

// from the current position to the end of the line
fn findArgsLine(self: *Self) !Loc {
    var loc = Loc{ .start = self.index };
    var drop: usize = 0;

    while (self.index < self.source.len) : (self.index += 1) {
        const b = self.source[self.index];
        switch (b) {
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

fn argsLine(self: *Self) ![]const u8 {
    const a = try self.findArgsLine();
    return self.source[a.start..a.end];
}

// from current location to the next whitespace (or end of line)
fn findArg(self: *Self) !Loc {
    var loc = Loc{ .start = self.index };
    while (self.index < self.source.len) : (self.index += 1) {
        const b = self.source[self.index];
        switch (b) {
            ' ', '\t' => {
                if (loc.start == self.index or loc.start == self.index - 1) {
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
            else => {},
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
        .reply = null,
        .payload = null,
    };
    if (a4.empty()) {
        msg.size = try std.fmt.parseUnsigned(u64, self.source[a3.start..a3.end], 10);
    } else {
        msg.reply = self.source[a3.start..a3.end];
        msg.size = try std.fmt.parseUnsigned(u64, self.source[a4.start..a4.end], 10);
    }
    self.eatNewLine();
    return msg;
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
        try expect(op == OperationToken.ping);
        try expect(@as(OperationToken, op) == OperationToken.ping);
        try expectEqual(parser.parsed_index, buf.len);
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
    try expectEqualStrings("INBOX.34", op.msg.reply.?);
    try expectEqualStrings("012345678901", op.msg.payload.?);
    try expectEqual(parser.parsed_index, buf.len);
    try expectEqual(buf[parser.parsed_index..].len, 0);
}

test "split buffer" {
    const cases = [_]struct {
        buf: []const u8,
        ops: usize,
        parsed_index: usize,
    }{
        .{ .buf = "PING\nPONG\nMSG subject ", .ops = 2, .parsed_index = 10 },
        .{ .buf = "PING\nPONG\n-err \nMSG subject 123 ", .ops = 3, .parsed_index = 16 },
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
    }
}

// what's the biggest message size
// unparsed part of the buffer function to return that
