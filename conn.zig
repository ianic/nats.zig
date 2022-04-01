const std = @import("std");
const print = std.debug.print;
const net = std.net;

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
}

const Parser = struct {
    state: ParserState,
    command: Command,
    msg: []const u8,

    fn init() Parser {
        return Parser{
            .state = .start,
            .command = .unknown,
            .msg = undefined,
        };
    }

    fn parse(self: *Parser, buf: []const u8) ParserError!void {
        for (buf) |b, i| {
            switch (self.state) {
                .start => {
                    switch (b) {
                        'I', 'i' => self.state = .i,
                        else => return ParserError.UnexpectedToken,
                    }
                },
                .i => {
                    switch (b) {
                        'N', 'n' => self.state = .in,
                        else => return ParserError.UnexpectedToken,
                    }
                },
                .in => {
                    switch (b) {
                        'F', 'f' => self.state = .inf,
                        else => return ParserError.UnexpectedToken,
                    }
                },
                .inf => {
                    switch (b) {
                        'O', 'o' => self.state = .info,
                        else => return ParserError.UnexpectedToken,
                    }
                },
                .info => {
                    switch (b) {
                        ' ', '\t' => self.state = .info_,
                        else => return ParserError.UnexpectedToken,
                    }
                },
                .info_ => {
                    self.command = .info;
                    self.msg = buf[i..];
                    return;
                },
            }
        }
    }
};

const Command = enum {
    unknown,
    info,
    ping,
    pong,
    msg,
    hmsg,
};

const ParserError = error{
    UnexpectedToken,
};

const ParserState = enum {
    start,
    i,
    in,
    inf,
    info,
    info_,
};

const Tokens = enum(u8) {
    start,
    i = 'I',
    _,
};

const expect = std.testing.expect;
test "Parser info message" {
    const buf =
        \\INFO {"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1"}
    ;

    // print("len: {d}\n", .{buf.len});
    // try expect(buf.len == 208);
    // var p = Parser.init();
    // try p.parse(buf[0..]);
    // try expect(p.command == .info);
    // try expect(p.msg.len == 203);

    const Case = struct {
        buf: []const u8,
        command: Command,
        args_buf_len: u32,
    };
    const valid = [_]Case{
        Case{
            .buf = "INFO 0123456789",
            .command = .info,
            .args_buf_len = 10,
        },
        Case{
            .buf = "info 0123456789",
            .command = .info,
            .args_buf_len = 10,
        },
        Case{
            .buf = "iNfO 0123456789",
            .command = .info,
            .args_buf_len = 10,
        },
        Case{
            .buf = buf,
            .command = .info,
            .args_buf_len = 203,
        },
    };

    for (valid) |r| {
        var p = Parser.init();
        try p.parse(r.buf);
        try expect(p.command == r.command);
        try expect(p.msg.len == r.args_buf_len);
    }

    const invalid = [_][]const u8{
        "INFOO something",
        "INFO_ something",
        "INFOsomething",
    };
    for (invalid) |ib| {
        var p = Parser.init();

        if (p.parse(ib)) {
            unreachable;
        } else |err| switch (err) {
            _ => unreachable,
            ParserError.UnexpectedToken => {},
        }
    }
}
