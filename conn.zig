const std = @import("std");
const print = std.debug.print;
const net = std.net;
const Allocator = std.mem.Allocator;

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

const Parser = struct {
    state: ParserState,
    op: OpName,
    msg: []const u8,

    fn init() Parser {
        return Parser{
            .state = .start,
            .op = undefined,
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
                    self.op = .info;
                    self.msg = buf[i..];
                    return;
                },
                .info_args => {},
            }
        }
    }
};

pub fn buildParser(comptime callback: fn (op: Op) void) BuildParser(callback) {
    return .{ .state = .start };
}

pub fn BuildParser(
    //comptime readFn: fn (buffer: []u8) std.io.FixedBufferStream.ReadError!usize,
    comptime callback: fn (op: Op) void,
) type {
    return struct {
        const Self = @This();
        state: ParserState,

        fn init() Self {
            return Self{
                .state = .start,
            };
        }

        // fn loop(self: *Self) void {
        //     var stratch: [16]u8 = undefined;
        //     while (true) {
        //         //var tmp = stratch[0..];
        //         var bytes_read = try readFn(stratch[0..]);
        //         if (bytes_read == 0) {
        //             break;
        //         }
        //         var bytes = stratch[0..bytes_read];
        //         try self.parse(bytes);
        //         print("bytes read: {d} {d}\n", .{ bytes_read, bytes.len });
        //     }
        // }

        fn parse(self: *Self, buf: []const u8) ParserError!void {
            var args_start: usize = 0;
            var drop: usize = 0;
            for (buf) |b, i| {
                switch (self.state) {
                    .start => {
                        args_start = 0;
                        drop = 0;

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
                            ' ', '\t' => {},
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
                                var op = Op{
                                    .name = .info,
                                    .args = buf[args_start .. i - drop],
                                };
                                callback(op);
                                self.state = .start;
                            },
                            else => {},
                        }
                    },
                    .info_ => {}, // TODO remove
                }
            }
        }
    };
}

const Op = struct {
    name: OpName,
    args: []const u8,
    payload: []u8 = undefined,
    // alloc: Allocator = undefined,

    // const Self = @This();

    // fn copy(self: *Self, alloc: Allocator) !Op {
    //     var al = std.ArrayList(u8).init(alloc);
    //     try al.appendSlice(self.args);
    //     self.alloc = alloc;
    //     return Op {
    //         .name = self.name,
    //         .args = al.toOwnedSlice(),
    //     };
    // }

    // fn deinit(self: *Self) void {
    //     self.alloc.free(self.args);
    // }
};

const OpName = enum {
    info,
    connect,
    pub_op, // pub is reserved word
    sub,
    unsub,
    msg,
    ping,
    pong,
    hmsg,
    ok,
    err,
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
    info_args,
};

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
const mem = std.mem;

test "parser info message" {
    const buf =
        \\INFO {"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1"}
    ;

    const Case = struct {
        buf: []const u8,
        op: OpName,
        args_buf_len: u32,
    };
    const valid = [_]Case{
        Case{
            .buf = "INFO 0123456789",
            .op = .info,
            .args_buf_len = 10,
        },
        Case{
            .buf = "info 0123456789",
            .op = .info,
            .args_buf_len = 10,
        },
        Case{
            .buf = "iNfO 0123456789",
            .op = .info,
            .args_buf_len = 10,
        },
        Case{
            .buf = buf,
            .op = .info,
            .args_buf_len = 203,
        },
    };

    for (valid) |r| {
        var p = Parser.init();
        try p.parse(r.buf);
        try expect(p.op == r.op);
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

test "decode server info message body" {
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

fn printOp(op: Op) void {
    print("op: {}\nargs: {s}\n", .{ op.name, op.args });
}

test "build parser" {
    const buf =
        \\INFO {"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
        \\INFO foo
        \\INFO foo bar
        \\
    ;

    const c = struct {
        var ops = std.ArrayList(Op).init(std.testing.allocator);
        const Self = @This();

        fn printOp(op: Op) void {
            ops.append(op) catch {};
            //print("op: {}\nargs: {s}\n", .{ op.name, op.args });
        }
    };
    defer c.ops.deinit();

    var p = buildParser(c.printOp);
    //var p = BuildParser(c.printOp).init();
    try p.parse(buf);

    try expectEqual(c.ops.items.len, 3);
    try expectEqual(c.ops.items[0].name, .info);
    try expectEqual(c.ops.items[0].args[0], '{');
    try expectEqual(c.ops.items[0].args[278], '}');
    try expectEqual(c.ops.items[0].args.len, 279);

    try expectEqual(c.ops.items[1].name, .info);
    try expect(mem.eql(u8, c.ops.items[1].args, "foo"));

    try expectEqual(c.ops.items[2].name, .info);
    try expect(mem.eql(u8, c.ops.items[2].args, "foo bar"));
}

// test "fixed buffer" {
//     const buf =
//         \\INFO {"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
//         \\INFO foo
//         \\INFO foo bar
//         \\
//     ;

//     var stream = std.io.fixedBufferStream(buf);
//     var stratch: [16]u8 = undefined;
//     while (true) {
//         //var tmp = stratch[0..];
//         var bytes_read = try stream.read(stratch[0..]);
//         if (bytes_read == 0) {
//             break;
//         }
//         var bytes = stratch[0..bytes_read];
//         print("bytes read: {d} {d}\n", .{ bytes_read, bytes.len });
//     }
// }

test "operation reader" {
    var clt = OperationsCollector.init(std.testing.allocator);
    defer clt.deinit();

    var stream = std.io.fixedBufferStream(test_info_operations());
    var br = opReader(stream, &clt);
    try br.loop();

    try assert_info_operations_parsed(&clt);
}

test "operation reader with buffer overflow" {
    var clt = OperationsCollector.init(std.testing.allocator);
    defer clt.deinit();

    var stream = std.io.fixedBufferStream(test_info_operations());
    var br = tinyOpReader(stream, &clt);
    defer br.deinit();
    try br.loop();

    //try expectEqual(clt.ops.items.len, 3);
    //print("args[0]: {s}\n", .{clt.ops.items[0].args});
    try assert_info_operations_parsed(&clt);
}

fn test_info_operations() []const u8 {
    const buf =
        \\INFO {"server_id":"id","server_name":"name","version":"2.8.0","proto":1,"go":"go1.18","host":"0.0.0.0","port":4222,"headers":true,"max_payload":123456,"jetstream":true,"client_id":53,"client_ip":"127.0.0.1","connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}
        \\INFO foo
        \\INFO foo bar
        \\
    ;
    return buf;
}

fn assert_info_operations_parsed(clt: *OperationsCollector) !void {
    try expectEqual(clt.ops.items.len, 3);
    try expectEqual(clt.ops.items[0].name, .info);
    try expectEqual(clt.ops.items[0].args[0], '{');
    try expectEqual(clt.ops.items[0].args[278], '}');
    try expectEqual(clt.ops.items[0].args.len, 279);

    try expectEqual(clt.ops.items[1].name, .info);
    try expect(mem.eql(u8, clt.ops.items[1].args, "foo"));

    try expectEqual(clt.ops.items[2].name, .info);
    try expect(mem.eql(u8, clt.ops.items[2].args, "foo bar"));
}

const OperationsCollector = struct {
    ops: std.ArrayList(Op),
    const Self = @This();

    fn init(alloc: Allocator) Self {
        return Self{
            .ops = std.ArrayList(Op).init(alloc),
        };
    }

    fn on_op(self: *Self, op: Op) void {
        var al = std.ArrayList(u8).init(std.testing.allocator);
        al.appendSlice(op.args) catch {};
        var op2 = Op{
            .name = op.name,
            .args = al.toOwnedSlice(),
        };
        //var op2 = try &op.copy(std.testing.allocator) catch {};
        self.ops.append(op2) catch {};
        //print("op: {}\nargs: {s}\n", .{ op.name, op.args });
    }

    fn deinit(self: *Self) void {
        for (self.ops.items) |op| {
            std.testing.allocator.free(op.args);
            //op.deinit();
        }
        self.ops.deinit();
    }
};

// reads operations from the underlying reader
// calls op handler for each parsed operation
pub fn OpReader(
    comptime buffer_size: usize,
    comptime ReaderType: type,
    comptime OpHandlerType: type,
) type {
    return struct {
        parent: ReaderType,
        handler: OpHandlerType,
        state: ParserState = .start,
        args_buf: ?std.ArrayList(u8) = null,

        //const Error = ReaderType.ReadError || ParserError ;
        const Self = @This();

        pub fn loop(self: *Self) !void {
            var scratch: [buffer_size]u8 = undefined;

            while (true) {
                var bytes_read = try self.parent.read(scratch[0..]);
                if (bytes_read == 0) {
                    return;
                }
                try self.parse(scratch[0..bytes_read]);
            }
        }

        fn parse(self: *Self, buf: []const u8) !void {
            var args_start: usize = 0;
            var drop: usize = 0;
            for (buf) |b, i| {
                switch (self.state) {
                    .start => {
                        args_start = 0;
                        drop = 0;
                        self.deinit();

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
                            ' ', '\t' => {},
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
                                try self.on_info(buf[args_start .. i - drop]);
                                self.state = .start;
                            },
                            else => {},
                        }
                    },
                    .info_ => {}, // TODO remove
                }
            }

            // we are in the middle if collecting args
            if (self.state == .info_args) {
                try self.push_args(buf[args_start..buf.len-drop]);
            }
        }

        fn on_info(self: *Self, buf: []const u8) !void {
            if (self.args_buf == null) {
                var op = Op{
                    .name = .info,
                    .args = buf,
                };
                self.handler.on_op(op);
                return;
            }
            try self.push_args(buf);
            var op = Op{
                .name = .info,
                .args = self.args_buf.?.items,
            };
            self.handler.on_op(op);
        }

        fn push_args(self: *Self, buf: []const u8) !void {
            if (self.args_buf == null) {
                self.args_buf = std.ArrayList(u8).init(std.testing.allocator); // TODO allocator
            }
            try self.args_buf.?.appendSlice(buf);
        }

        fn on_op(self: *Self, op: Op) void {
            self.handler.on_op(op);
        }

        fn deinit(self: *Self) void {
            if (self.args_buf) |*ab| {
                ab.deinit();
                self.args_buf = null;
            }
        }
    };
}

pub fn opReader(
    parent_stream: anytype,
    op_handler: anytype,
) OpReader(4096, @TypeOf(parent_stream), @TypeOf(op_handler)) {
    return .{
        .parent = parent_stream,
        .handler = op_handler,
    };
}

pub fn tinyOpReader(
    parent_stream: anytype,
    op_handler: anytype,
) OpReader(16, @TypeOf(parent_stream), @TypeOf(op_handler)) {
    return .{
        .parent = parent_stream,
        .handler = op_handler,
    };
}
