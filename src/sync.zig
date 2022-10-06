const std = @import("std");
const net = std.x.net;
const tcp = net.tcp;
const log = std.log.scoped(.nats);
const Parser = @import("../src/NonAllocatingParser.zig");
const Operation = Parser.Operation;
const Msg = Parser.Msg;
const Info = Parser.Info;
const Err = Parser.Err;
const Allocator = std.mem.Allocator;

const operation = struct {
    const connect = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
    const pong = "PONG\r\n";
    const ping = "PING\r\n";
};

const default = struct {
    const max_control_line_size = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
};

const Options = struct {
    ip: []const u8 = "127.0.0.1",
    port: u16 = 4222,

    read_buffer_size: u32 = 4096,
    read_timeout: u32 = 1000, // milliseconds

    max_reconnect: u8 = 60,
    reconnect_wait: usize = std.time.ns_per_s * 2,
};

const Error = error{
    HandshakeFailed,
    ServerError,
};

pub fn connect(allocator: Allocator, options: Options) !Conn {
    return try Conn.connect(allocator, options);
}

const Conn = struct {
    const Self = @This();

    allocator: Allocator,
    net_cli: tcp.Client,
    scratch_buf: [default.max_control_line_size]u8 = undefined,
    read_buf: []u8,
    sid: u64 = 0,
    parser: Parser,
    options: Options,

    pub fn connect(allocator: Allocator, options: Options) !Self {
        const read_buf = try allocator.alloc(u8, options.read_buffer_size);
        errdefer allocator.free(read_buf);

        var conn = Self{
            .allocator = allocator,
            .net_cli = try tcpConnect(options.ip, options.port),
            .read_buf = read_buf,
            .parser = Parser.init(read_buf[0..0]),
            .options = options,
        };
        try conn.initNetCli();
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
            if (try self.netRead()) |buf| {
                self.parser.reInit(buf);
                continue;
            }
            return null;
        }
    }

    fn netRead(self: *Self) !?[]const u8 {
        const unparsed = self.parser.unparsedBytes();
        const offset = self.net_cli.read(self.read_buf[unparsed..], 0) catch |err| {
            if (err == error.WouldBlock) {
                return null; // read timeout expired
            }
            return err;
        };
        if (offset == 0) {
            try self.reConnect();
        }
        const buf = self.read_buf[0 .. offset + unparsed];
        debugConnIn(buf);
        return buf;
    }

    fn parseReadBuffer(self: *Self) !?Msg {
        while (true) {
            if (try self.parseOperation()) |op| {
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

    fn parseOperation(self: *Self) !?Operation {
        return self.parser.next() catch |err| {
            switch (err) {
                Parser.Error.SplitBuffer => {
                    try self.onParserSplitBuffer();
                    return null;
                },
                else => return err,
            }
        };
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
        _ = try self.netWrite(operation.pong);
    }

    fn reConnect(self: *Self) !void {
        self.net_cli.shutdown(.both) catch {};
        self.net_cli.deinit();
        var cnt: usize = 0;
        while (true) {
            log.debug("reconnecting {d} of {d}", .{ cnt, self.options.max_reconnect });
            var client = tcpConnect(self.options.ip, self.options.port) catch |err| {
                if (cnt < self.options.max_reconnect) {
                    std.time.sleep(self.options.reconnect_wait);
                    cnt += 1;
                    continue;
                }
                return err;
            };
            self.net_cli = client;
            try self.initNetCli();
            break;
        }
    }

    fn initNetCli(self: *Self) !void {
        try self.net_cli.setReadTimeout(self.options.read_timeout);
    }

    fn tcpConnect(ip: []const u8, port: u16) !tcp.Client {
        const addr = net.ip.Address.initIPv4(try std.x.os.IPv4.parse(ip), port);
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
        _ = try self.netWrite(operation.connect);
        _ = try self.netWrite(operation.ping);

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

    fn netWrite(self: *Self, buf: []const u8) !void {
        _ = try self.net_cli.write(buf, 0);
        debugConnOut(buf);
    }

    pub fn subscribe(self: *Self, subject: []const u8) !u64 {
        self.sid += 1;
        try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "SUB {s} {d}\r\n", .{ subject, self.sid }));
        return self.sid;
    }

    pub fn unsubscribe(self: *Self, sid: u64) !void {
        try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "UNSUB {d}\r\n", .{sid}));
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
