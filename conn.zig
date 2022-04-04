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

// const Parser = struct {
//     state: ParserState,
//     op: OpName,
//     msg: []const u8,

//     fn init() Parser {
//         return Parser{
//             .state = .start,
//             .op = undefined,
//             .msg = undefined,
//         };
//     }

//     fn parse(self: *Parser, buf: []const u8) ParserError!void {
//         for (buf) |b, i| {
//             switch (self.state) {
//                 .start => {
//                     switch (b) {
//                         'I', 'i' => self.state = .i,
//                         else => return ParserError.UnexpectedToken,
//                     }
//                 },
//                 .i => {
//                     switch (b) {
//                         'N', 'n' => self.state = .in,
//                         else => return ParserError.UnexpectedToken,
//                     }
//                 },
//                 .in => {
//                     switch (b) {
//                         'F', 'f' => self.state = .inf,
//                         else => return ParserError.UnexpectedToken,
//                     }
//                 },
//                 .inf => {
//                     switch (b) {
//                         'O', 'o' => self.state = .info,
//                         else => return ParserError.UnexpectedToken,
//                     }
//                 },
//                 .info => {
//                     switch (b) {
//                         ' ', '\t' => self.state = .info_,
//                         else => return ParserError.UnexpectedToken,
//                     }
//                 },
//                 .info_ => {
//                     self.op = .info;
//                     self.msg = buf[i..];
//                     return;
//                 },
//                 .info_args => {},
//             }
//         }
//     }
// };

// pub fn buildParser(comptime callback: fn (op: Op) void) BuildParser(callback) {
//     return .{ .state = .start };
// }

// pub fn BuildParser(
//     //comptime readFn: fn (buffer: []u8) std.io.FixedBufferStream.ReadError!usize,
//     comptime callback: fn (op: Op) void,
// ) type {
//     return struct {
//         const Self = @This();
//         state: ParserState,

//         fn init() Self {
//             return Self{
//                 .state = .start,
//             };
//         }

//         // fn loop(self: *Self) void {
//         //     var stratch: [16]u8 = undefined;
//         //     while (true) {
//         //         //var tmp = stratch[0..];
//         //         var bytes_read = try readFn(stratch[0..]);
//         //         if (bytes_read == 0) {
//         //             break;
//         //         }
//         //         var bytes = stratch[0..bytes_read];
//         //         try self.parse(bytes);
//         //         print("bytes read: {d} {d}\n", .{ bytes_read, bytes.len });
//         //     }
//         // }

//         fn parse(self: *Self, buf: []const u8) ParserError!void {
//             var args_start: usize = 0;
//             var drop: usize = 0;
//             for (buf) |b, i| {
//                 switch (self.state) {
//                     .start => {
//                         args_start = 0;
//                         drop = 0;

//                         switch (b) {
//                             'I', 'i' => self.state = .i,
//                             else => return ParserError.UnexpectedToken,
//                         }
//                     },
//                     .i => {
//                         switch (b) {
//                             'N', 'n' => self.state = .in,
//                             else => return ParserError.UnexpectedToken,
//                         }
//                     },
//                     .in => {
//                         switch (b) {
//                             'F', 'f' => self.state = .inf,
//                             else => return ParserError.UnexpectedToken,
//                         }
//                     },
//                     .inf => {
//                         switch (b) {
//                             'O', 'o' => self.state = .info,
//                             else => return ParserError.UnexpectedToken,
//                         }
//                     },
//                     .info => {
//                         switch (b) {
//                             ' ', '\t' => {},
//                             else => {
//                                 self.state = .info_args;
//                                 args_start = i;
//                             },
//                         }
//                     },
//                     .info_args => {
//                         switch (b) {
//                             '\r' => {
//                                 drop = 1;
//                             },
//                             '\n' => {
//                                 var op = Op{
//                                     .name = .info,
//                                     .args = buf[args_start .. i - drop],
//                                 };
//                                 callback(op);
//                                 self.state = .start;
//                             },
//                             else => {},
//                         }
//                     },
//                     .info_ => {}, // TODO remove
//                 }
//             }
//         }
//     };
// }

// const Op = struct {
//     name: OpName,
//     args: []const u8,
//     payload: []u8 = undefined,
//     // alloc: Allocator = undefined,

//     // const Self = @This();

//     // fn copy(self: *Self, alloc: Allocator) !Op {
//     //     var al = std.ArrayList(u8).init(alloc);
//     //     try al.appendSlice(self.args);
//     //     self.alloc = alloc;
//     //     return Op {
//     //         .name = self.name,
//     //         .args = al.toOwnedSlice(),
//     //     };
//     // }

//     // fn deinit(self: *Self) void {
//     //     self.alloc.free(self.args);
//     // }
// };

// const OpName = enum {
//     info,
//     connect,
//     pub_op, // pub is reserved word
//     sub,
//     unsub,
//     msg,
//     ping,
//     pong,
//     hmsg,
//     ok,
//     err,
// };

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

    const invalid = [_][]const u8{
        "INFOO something\r\n",
        "INFO_ something\r\n",
        "INFOsomething\r\n",
    };
    for (invalid) |buf| {
        if (test_parse_line(buf)) {
            unreachable;
        } else |err| switch (err) {
            ParserError.UnexpectedToken => {},
            else => unreachable,
        }
    }

    const missing_arguments = [_][]const u8{
        "INFO \r\n",
        "INFO \n",
    };
    for (missing_arguments) |buf| {
        if (test_parse_line(buf)) {
            unreachable;
        } else |err| switch (err) {
            ParserError.MissingArguments => {},
            else => unreachable,
        }
    }
}

fn test_parse_line(buf: []const u8) !Op {
    var noopHandler = struct {
        last_op: Op = undefined,
        const Self = @This();
        fn on_op(self: *Self, op: Op) void { self.last_op = op; }
    }{};

    var opr = opReader(testing.allocator, std.io.fixedBufferStream(buf), &noopHandler);
    try opr.loop();
    return noopHandler.last_op;
}

test "decode server info message body JSON into ServerInfo struct" {
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

// fn printOp(op: Op) void {
//     print("op: {}\nargs: {s}\n", .{ op.name, op.args });
// }

test "operation reader" {
    var clt = OperationsCollector.init(std.testing.allocator);
    defer clt.deinit();

    var stream = std.io.fixedBufferStream(test_info_operations());
    // opReader has buffer of 4096 bytes, big enough for whole stream of test data
    var br = opReader(testing.allocator, stream, &clt);
    try br.loop();

    try assert_info_operations_parsed(&clt);
}

test "operation reader with buffer overflow" {
    var clt = OperationsCollector.init(std.testing.allocator);
    defer clt.deinit();

    var stream = std.io.fixedBufferStream(test_info_operations());
    // tinyOpReader has buffer of 16 bytes, overflow occures
    var br = tinyOpReader(testing.allocator, stream, &clt);
    defer br.deinit();
    try br.loop();

    try assert_info_operations_parsed(&clt);
}

test "operation reader with buffer overflow 2" {
    var clt = struct {
        no: usize = 1,
        const Self = @This();

        fn on_op(self: *Self, op: Op) void {
            //print("\nargs: {d} {d} '{s}'", .{ self.no, op.info.args.len, op.info.args });
            expectEqual(self.no, op.info.args.len) catch {
                unreachable;
            };
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

    var stream = std.io.fixedBufferStream(cases.items);
    // tinyOpReader has buffer of 16 bytes, overflow occures
    var br = tinyOpReader(testing.allocator, stream, &clt);
    defer br.deinit();
    try br.loop();

    //try assert_info_operations_parsed(&clt);
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
    var ops = clt.ops.items;
    try expectEqual(ops.len, 3);

    var op = ops[0];
    try expectEqual(op.info.args[0], '{');
    try expectEqual(op.info.args[278], '}');
    try expectEqual(op.info.args.len, 279);

    op = ops[1];
    try expect(mem.eql(u8, op.info.args, "foo"));

    op = ops[2];
    try expect(mem.eql(u8, op.info.args, "foo bar"));
}

const OperationsCollector = struct {
    ops: std.ArrayList(Op),
    alloc: Allocator,

    const Self = @This();

    fn init(alloc: Allocator) Self {
        return Self{
            .alloc = alloc,
            .ops = std.ArrayList(Op).init(alloc),
        };
    }

    fn on_op(self: *Self, op: Op) void {
        switch (op) {
            Op.info => |i| {
                // make copy of args buffer
                var al = std.ArrayList(u8).init(self.alloc);
                al.appendSlice(i.args) catch {};
                self.ops.append(Op{ .info = .{ .args = al.toOwnedSlice() } }) catch {};
            },
            else => {
                self.ops.append(op) catch {};
            },
        }
        // var al = std.ArrayList(u8).init(std.testing.allocator);
        // al.appendSlice(op.args) catch {};
        // var op2 = Op{
        //     .name = op.name,
        //     .args = al.toOwnedSlice(),
        // };
        // //var op2 = try &op.copy(std.testing.allocator) catch {};
        // self.ops.append(op2) catch {};
        // //print("op: {}\nargs: {s}\n", .{ op.name, op.args });
    }

    fn deinit(self: *Self) void {
        for (self.ops.items) |op| {
            switch (op) {
                Op.info => |i| {
                    self.alloc.free(i.args);
                },
                else => {},
            }
        }
        self.ops.deinit();
    }
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

const Op = union(OpName) {
    info: struct { args: []const u8 },
    connect,
    pub_op,
    sub,
    unsub,
    msg: struct { args: []const u8, payload: []const u8 },
    ping,
    pong,
    hmsg,
    ok,
    err,
};

const ParserError = error{
    UnexpectedToken,
    MissingArguments,
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
                            ' ', '\t' => {
                                self.state = .info_;
                            },
                            else => return ParserError.UnexpectedToken,
                        }
                    },
                    .info_ => {
                        switch (b) {
                            ' ', '\t' => {},
                            '\r', '\n' => return ParserError.MissingArguments,
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
                }
            }

            // check for split buffer scenarios
            if (self.state == .info_args) {
                try self.push_args(buf[args_start .. buf.len - drop]);
            }
        }

        fn on_info(self: *Self, buf: []const u8) !void {
            if (self.args_buf) |*ab| {
                try ab.appendSlice(buf);
                self.handler.on_op(Op{ .info = .{ .args = ab.items } });
                return;
            }
            self.handler.on_op(Op{ .info = .{ .args = buf } });
        }

        fn push_args(self: *Self, buf: []const u8) !void {
            if (self.args_buf) |*ab| {
                try ab.appendSlice(buf);
                return;
            }
            var ab = std.ArrayList(u8).init(self.alloc);
            try ab.appendSlice(buf);
            self.args_buf = ab;
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

// test "experiment" {
//     var i = Op2{ .info = "a" };

//     switch (i) {
//         Op2.info => |value| print("\nvalue: {s}\n", .{value}),
//         else => unreachable,
//     }

//     var j = Op2{ .msg = .{ .args = "argssss", .payload = "payloadddd" } };

//     switch (j) {
//         Op2.info => |value| print("\nvalue: {s}\n", .{value}),
//         Op2.msg => |m| print("\n msg: {s} {s}\n", .{ m.args, m.payload }),
//         else => unreachable,
//     }
// }
