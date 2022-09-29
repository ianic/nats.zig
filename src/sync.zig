const std = @import("std");
const net = std.x.net;
const tcp = net.tcp;
const log = std.log.scoped(.nats);
const Parser = @import("../src/NonAllocatingParser.zig");
// TODO move this to parser (small letter)
const Operation = Parser.Operation;
const Msg = Parser.Msg;
const Info = Parser.Info;
const Err = Parser.Err;
const Allocator = std.mem.Allocator;

const default = struct {
    const ip = "127.0.0.1";
    const port = 4222;

    const read_buffer_size = 16;
    const max_control_line_size = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
    const read_timeout = 1000; // milliseconds

    const connect = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
    const pong = "PONG\r\n";
    const ping = "PING\r\n";
};

const Error = error{
    HandshakeFailed,
    ServerError,
    EOF, // when no bytes are received on socket
    NotOpenForReading, // when socket is closed
    RequestTimeout,
    MaxPayloadExceeded,
    ConnectionResetByPeer,
    ClientNotConnected,
    WriteBufferExceeded,
    Disconnected,
};

pub fn connect(allocator: Allocator) !Conn {
    return try Conn.connect(allocator);
}

const Conn = struct {
    const Self = @This();

    allocator: Allocator,
    net_cli: tcp.Client,
    scratch_buf: [default.max_control_line_size]u8 = undefined,
    read_buf: []u8,
    sid: u64 = 0,
    parser: Parser,

    pub fn connect(allocator: Allocator) !Self {
        const read_buf = try allocator.alloc(u8, default.read_buffer_size);
        errdefer allocator.free(read_buf);

        var conn = Self{
            .allocator = allocator,
            .net_cli = try tcpConnect(),
            .read_buf = read_buf,
            .parser = Parser.init(read_buf[0..0]),
        };
        try conn.connectHandshake();
        return conn;
    }

    pub fn close(self: *Self) void {
        self.net_cli.shutdown(.both) catch {};
        self.allocator.free(self.read_buf);
    }

    pub fn read(self: *Self) !?Msg {
        while (true) {
            if (try self.parseReadBuffer()) |msg| {
                return msg;
            }
            const unparsed = self.parser.unparsedBytes();
            try self.net_cli.setReadTimeout(default.read_timeout);
            const offset = self.net_cli.read(self.read_buf[unparsed..], 0) catch |err| {
                if (err == error.WouldBlock) {
                    return null;
                }
                return err;
            };
            if (offset == 0) {
                return null;
            }
            const buf = self.read_buf[0 .. offset + unparsed];
            debugConnIn(buf);
            self.parser.reInit(buf);
        }
    }

    fn parseReadBuffer(self: *Self) !?Msg {
        while (true) {
            const opt_op = self.parser.next() catch |err| {
                switch (err) {
                    Parser.Error.SplitBuffer => {
                        try self.onParserSplitBuffer();
                        // const unparsed = self.parser.unparsed();
                        // std.mem.copy(u8, self.read_buf, unparsed);
                        // log.info("split buffer, copying {d} unparsed bytes", .{unparsed.len});
                        return null;
                    },
                    // Parser.Error.BufferOverflow => {
                    //     const read_buf = try self.allocator.alloc(u8, self.read_buf.len * 2);
                    //     const unparsed = self.parser.unparsed();
                    //     std.mem.copy(u8, read_buf, unparsed);
                    //     self.allocator.free(self.read_buf);
                    //     self.read_buf = read_buf;
                    //     log.info(
                    //         "read buffer overflow, extending buffer to {d}, copying {d} unparsed bytes",
                    //         .{ self.read_buf.len, unparsed.len },
                    //     );
                    //     return null;
                    // },
                    else => return err,
                }
            };
            if (opt_op) |op| {
                switch (op) {
                    .msg => return op.msg,
                    .hmsg => return op.hmsg,
                    .ping => try self.sendPong(),
                    .info => self.onInfo(op.info),
                    .pong => {},
                    .ok => {},
                    .err => {
                        log.err("error: {s}", .{op.err.args});
                        return Error.ServerError;
                    },
                }
                continue;
            }
            return null;
        }
    }

    fn onParserSplitBuffer(self: *Self) !void {
        const extend = self.parser.parsed_index < self.parser.source.len / 2 or
            self.parser.stat.batch_operations < 4;

        const unparsed = self.parser.unparsed();
        if (extend) {
            const old_read_buf = self.read_buf;
            self.read_buf = try self.allocator.alloc(u8, self.read_buf.len * 2);
            std.mem.copy(u8, self.read_buf, unparsed);
            self.allocator.free(old_read_buf);
            log.debug(
                "read buffer overflow, extending buffer to {d}, copying {d} unparsed bytes",
                .{ self.read_buf.len, unparsed.len },
            );
        } else {
            std.mem.copy(u8, self.read_buf, unparsed);
            log.debug("split buffer, copying {d} unparsed bytes", .{unparsed.len});
        }
    }

    fn sendPong(self: *Self) !void {
        _ = try self.netWrite(default.pong);
    }

    fn tcpConnect() !tcp.Client {
        const addr = net.ip.Address.initIPv4(try std.x.os.IPv4.parse(default.ip), default.port);
        const client = try tcp.Client.init(.ip, .{ .close_on_exec = true });
        try client.connect(addr);
        errdefer client.deinit();
        return client;
    }

    fn connectHandshake(self: *Self) !void {
        // expect INFO at start
        const op = try self.readOp();
        if (op != .info) {
            return Error.HandshakeFailed;
        }
        self.onInfo(op.info);

        // send CONNECT, PING
        _ = try self.netWrite(default.connect);
        _ = try self.netWrite(default.ping);

        // expect PONG
        if (try self.readOp() != .pong) {
            return Error.HandshakeFailed;
        }
    }

    fn onInfo(self: *Self, info: Info) void {
        // TODO
        _ = info;
        _ = self;
    }

    fn readOp(self: *Self) !Operation {
        const offset = try self.net_cli.read(&self.scratch_buf, 0);
        debugConnIn(self.scratch_buf[0..offset]);
        var parser = Parser.init(self.scratch_buf[0..offset]);

        // TODO
        const op = try parser.next();
        return op.?;
    }

    fn netRead(self: *Self, buf: []u8) !usize {
        if (self.net_cli) |c| {
            return try c.read(buf, 0);
        }
        return Error.ClientNotConnected;
    }

    fn netWrite(self: *Self, buf: []const u8) !void {
        _ = try self.net_cli.write(buf, 0);
        debugConnOut(buf);
    }

    fn netClose(self: *Self) void {
        _ = self;
    }

    pub fn subscribe(self: *Self, subject: []const u8) !u64 {
        self.sid += 1;
        try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "SUB {s} {d}\r\n", .{ subject, self.sid }));
        return self.sid;
    }

    pub fn publish(self: *Self, subject: []const u8, payload: []const u8) !void {
        try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "PUB {s} {d}\r\n", .{ subject, payload.len }));
        try self.netWrite(payload);
        try self.netWrite("\r\n");
    }
};

fn debugConnIn(buf: []const u8) void {
    logProtocolOp(">>", buf);
}

fn debugConnOut(buf: []const u8) void {
    logProtocolOp("<<", buf);
}

fn logProtocolOp(prefix: []const u8, buf: []const u8) void {
    if (buf.len == 0) {
        return;
    }
    var b = buf[0..];
    if (buf[buf.len - 1] == '\n') {
        b = buf[0 .. buf.len - 1];
    }
    if (b.len > 0 and b[b.len - 1] == '\r') {
        b = b[0 .. b.len - 1];
    }
    if (b.len > 0 and b[0] == '\r') {
        b = b[1..];
    }
    if (b.len > 128) {
        log.debug("{s} {s}...+{d} bytes", .{ prefix, b[0..128], buf.len - 128 });
    } else {
        log.debug("{s} {s}", .{ prefix, b });
    }
}
