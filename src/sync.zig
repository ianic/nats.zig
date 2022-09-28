const std = @import("std");
const net = std.x.net;
const tcp = net.tcp;
const log = std.log.scoped(.nats);
const Parser = @import("../src/NonAllocatingParser.zig");
// TODO move this to parser (small letter)
const Operation = Parser.Operation;
const Msg = Parser.Msg;

// constants
const max_args_len = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
const connect_op = "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"headers\":false,\"name\":\"\",\"lang\":\"zig\",\"version\":\"0.1.0\",\"protocol\":1}\r\n";
const pong_op = "PONG\r\n";
const ping_op = "PING\r\n";

pub fn connect() !Conn {
    return try Conn.connect();
}

const Conn = struct {
    const Self = @This();

    net_cli: tcp.Client,
    scratch_buf: [max_args_len]u8 = undefined,

    pub fn connect() !Self {
        var conn = Self{
            .net_cli = try tcpConnect(),
        };
        try conn.connectHandshake();
        return conn;
    }

    pub fn close(self: *Self) void {
        self.net_cli.shutdown(.both) catch {};
    }

    pub fn read(self: *Self) !?Msg {
        while (true) {
            try self.net_cli.setReadTimeout(3000);
            var op = self.readOp() catch |err| {
                if (err == error.WouldBlock) {
                    return null;
                }
                return err;
            };
            switch (op) {
                .msg => return op.msg,
                .hmsg => return op.hmsg,
                .ping => {
                    _ = try self.netWrite(pong_op);
                },
                .info => {
                    // TODO use info message
                },
                .pong => {},
                .ok => {},
                .err => {
                    return Error.ServerError;
                },
            }
        }
    }

    fn tcpConnect() !tcp.Client {
        const addr = net.ip.Address.initIPv4(try std.x.os.IPv4.parse("127.0.0.1"), 4222);
        const client = try tcp.Client.init(.ip, .{ .close_on_exec = true });
        try client.connect(addr);
        errdefer client.deinit();
        return client;
    }

    fn connectHandshake(self: *Self) !void {
        // expect INFO at start
        if (try self.readOp() != .info) {
            return Error.HandshakeFailed;
        }
        //  TODO use info message
        //self.onInfo(op.info);

        // send CONNECT, PING
        _ = try self.netWrite(connect_op);
        _ = try self.netWrite(ping_op);

        // expect PONG
        if (try self.readOp() != .pong) {
            return Error.HandshakeFailed;
        }
    }

    fn readOp(self: *Self) !Operation {
        const offset = try self.net_cli.read(&self.scratch_buf, 0);
        debugConnIn(self.scratch_buf[0..offset]);
        var parser = Parser.init(self.scratch_buf[0..offset]);
        return parser.next();
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
};

fn debugConnIn(buf: []const u8) void {
    logProtocolOp(">", buf);
}

fn debugConnOut(buf: []const u8) void {
    logProtocolOp("<", buf);
}

fn logProtocolOp(prefix: []const u8, buf: []const u8) void {
    if (buf.len == 0) {
        return;
    }
    var b = buf[0..];
    if (buf[buf.len - 1] == '\n') {
        b = buf[0 .. buf.len - 1];
    }
    log.debug("{s} {s}", .{ prefix, b });
}

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
