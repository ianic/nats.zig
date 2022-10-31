const std = @import("std");
const Allocator = std.mem.Allocator;

const Nkeys = @import("Nkeys.zig");
const Parser = @import("../src/NonAllocatingParser.zig");
const Operation = Parser.Operation;
const Msg = Parser.Msg;
const Err = Parser.Err;
const Info = @import("Parser.zig").Info;

const log = std.log.scoped(.nats);

const operation = struct {
    const pong = "PONG\r\n";
    const ping = "PING\r\n";
};

const default = struct {
    const max_control_line_size = 4096; // ref: https://github.com/nats-io/nats.go/blob/c75dfd54b52c9f37139ab592da3d4fcbec34eda2/parser.go#L28
};

pub const Options = struct {
    host: []const u8 = "localhost",
    port: u16 = 4222,

    creds_file_path: ?[]const u8 = null,

    read_buffer_size: u32 = 4096,
    read_timeout: u32 = 1000, // milliseconds

    with_reconnect: bool = true,
    max_reconnect: u8 = 60,
    reconnect_wait: usize = std.time.ns_per_s * 2,
};

const Error = error{
    HandshakeFailed,
    ServerError,
    MissingCredsFile,
    BrokenPipe,
    NotSubscribed,
};

pub fn connect(allocator: Allocator, options: Options) !Conn {
    try ignoreSigPipe();
    return try Conn.connect(allocator, options);
}

pub fn ignoreSigPipe() !void {
    // write to the closed socket raises signal (which closes app by default)
    // instead of returning error
    // by ignoring we got error on socket write
    var act = std.os.Sigaction{
        // TODO: change to this when https://github.com/ziglang/zig/issues/13216 is merged
        //.handler = .{ .handler = std.os.SIG.IGN },
        .handler = .{ .handler = signalHandler },
        .mask = std.os.empty_sigset,
        .flags = 0,
    };
    try std.os.sigaction(std.os.SIG.PIPE, &act, null);
}

fn signalHandler(_: c_int) callconv(.C) void {}

pub const Conn = struct {
    const Self = @This();
    const Subscriptions = std.AutoHashMap(u16, Subscription);

    allocator: Allocator,
    net: Net,
    scratch_buf: [default.max_control_line_size]u8 = undefined,
    read_buf: []u8,
    sid: u16 = 0,
    parser: Parser,
    options: Options,
    subscriptions: Subscriptions,
    info: ?Info = null,

    pub fn connect(allocator: Allocator, options: Options) !Self {
        var conn = try init(allocator, options);
        errdefer conn.deinit();

        try conn.connectHandshake();
        return conn;
    }

    fn init(allocator: Allocator, options: Options) !Self {
        const read_buf = try allocator.alloc(u8, options.read_buffer_size);
        errdefer allocator.free(read_buf);

        const net = try Net.init(allocator, .{
            .host = options.host,
            .port = options.port,
            .read_timeout = options.read_timeout, // milliseconds
            .max_reconnect = options.max_reconnect,
            .reconnect_wait = options.reconnect_wait,
        });
        errdefer net.close();

        return .{
            .allocator = allocator,
            .net = net,
            .read_buf = read_buf,
            .parser = Parser.init(read_buf[0..0]),
            .options = options,
            .subscriptions = Subscriptions.init(allocator),
        };
    }

    pub fn close(self: *Self) void {
        self.deinit();
    }

    pub fn deinit(self: *Self) void {
        self.subscriptions.deinit();
        self.net.close();
        self.allocator.free(self.read_buf);
        if (self.info) |i| {
            i.deinit(self.allocator);
        }
    }

    // Reads next message from connection.
    // Returns null if options.read_timeout expires while waiting for message.
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
        const offset = self.net.read(self.read_buf[unparsed..]) catch |err| {
            if (err == error.WouldBlock) {
                return null; // read timeout expired
            }
            return err;
        };
        if (offset == 0) { // connection closed
            if (self.options.with_reconnect) {
                try self.reconnect();
            } else {
                return Error.BrokenPipe;
            }
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
                    .info => try self.onInfo(op.info),
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

    fn sendPong(self: *Self) !void {
        try self.netWrite(operation.pong);
    }

    fn onInfo(self: *Self, info: Parser.Info) !void {
        if (self.info) |i| {
            i.deinit(self.allocator);
        }
        self.info = try Info.jsonParse(self.allocator, info.args);
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
        const extend = (self.parser.source.len > self.read_buf.len / 2) and (self.parser.parsed_index < self.parser.source.len / 2 or
            self.parser.stat.batch_operations < 4);

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

    pub fn reconnect(self: *Self) !void {
        try self.net.reconnect();
        try self.connectHandshake();
        try self.reSubscribe();
    }

    fn connectHandshake(self: *Self) !void {
        // expect INFO at start
        const op = try self.readOp();
        if (op != .info) {
            return Error.HandshakeFailed;
        }
        try self.onInfo(op.info);
        const info = self.info.?;

        if (info.tls_required) {
            try self.net.wrapTLS();
        }

        // send CONNECT, PING
        try self.netWrite(try self.connectOperation(info));
        try self.netWrite(operation.ping);

        // expect PONG TODO is this necessary, what if we get INFO before POGN
        if (try self.readOp() != .pong) {
            return Error.HandshakeFailed;
        }
    }

    // create connect operation
    // use creds_file to make nonce signature if authentication is required
    fn connectOperation(self: *Self, info: Info) ![]const u8 {
        var cnt = Connect{};
        var nk: ?Nkeys = null;
        defer if (nk != null) {
            nk.?.deinit();
        };

        if (info.auth_required) {
            if (info.nonce) |nonce| {
                if (self.options.creds_file_path) |fp| {
                    nk = try Nkeys.init(self.allocator, fp);
                    cnt.sig = try nk.?.sign(nonce);
                    cnt.jwt = nk.?.jwt;
                } else {
                    return Error.MissingCredsFile;
                }
            }
        }
        return try cnt.operation(&self.scratch_buf);
    }

    // read and parse one operation, used in connection handshake
    fn readOp(self: *Self) !Operation {
        const offset = try self.net.read(&self.scratch_buf);
        if (offset == 0) {
            return Error.HandshakeFailed;
        }
        debugConnIn(self.scratch_buf[0..offset]);
        var parser = Parser.init(self.scratch_buf[0..offset]);
        if (try parser.next()) |op| {
            return op;
        }
        return Error.HandshakeFailed;
    }

    fn netWrite(self: *Self, buf: []const u8) !void {
        self.net.write(buf) catch |err| {
            if (err != error.BrokenPipe) {
                log.debug("netWrite error: {s}", .{err});
            }
            return Error.BrokenPipe;
        };
        debugConnOut(buf);
    }

    pub fn subscribe(self: *Self, subject: []const u8) !u16 {
        self.sid += 1;
        const sid = self.sid;
        try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "SUB {s} {d}\r\n", .{ subject, sid }));
        try self.subscriptions.put(sid, Subscription{
            .sid = sid,
            .subject = subject,
        });
        return sid;
    }

    fn reSubscribe(self: *Self) !void {
        var iter = self.subscriptions.iterator();
        while (iter.next()) |e| {
            const s = e.value_ptr;
            try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "SUB {s} {d}\r\n", .{ s.subject, s.sid }));
        }
    }

    pub fn unsubscribeBySid(self: *Self, sid: u16) !void {
        try self.netWrite(try std.fmt.bufPrint(&self.scratch_buf, "UNSUB {d}\r\n", .{sid}));
        _ = self.subscriptions.remove(sid);
    }

    pub fn unsubscribe(self: *Self, subject: []const u8) !u16 {
        var iter = self.subscriptions.iterator();
        while (iter.next()) |e| {
            const s = e.value_ptr;
            if (std.mem.eql(u8, subject, s.subject)) {
                try self.unsubscribeBySid(s.sid);
                return s.sid;
            }
        }
        return Error.NotSubscribed;
    }

    pub fn publish(self: *Self, subject: []const u8, payload: []const u8) !void {
        if (self.options.with_reconnect) {
            return self.publishWithReconnect(subject, payload);
        }
        return self.publishRaw(subject, payload);
    }

    fn publishWithReconnect(self: *Self, subject: []const u8, payload: []const u8) !void {
        self.publishRaw(subject, payload) catch |err| {
            if (err != error.BrokenPipe) {
                return err;
            }
            try self.reconnect();
            try self.publishRaw(subject, payload);
        };
    }

    fn publishRaw(self: *Self, subject: []const u8, payload: []const u8) !void {
        const header = try std.fmt.bufPrint(&self.scratch_buf, "PUB {s} {d}\r\n", .{ subject, payload.len });
        try self.netWrite(header);
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

const Subscription = struct {
    sid: u16,
    subject: []const u8,
};

pub const Connect = struct {
    verbose: bool = false,
    pedantic: bool = false,
    tls_required: bool = false,
    headers: bool = false,
    name: ?[]const u8 = null,
    lang: ?[]const u8 = "zig",
    version: ?[]const u8 = null,
    protocol: u8 = 1,
    jwt: ?[]const u8 = null,
    sig: ?[]const u8 = null,

    const Self = @This();

    fn stringify(self: Self, buf: []u8) ![]u8 {
        var fba = std.heap.FixedBufferAllocator.init(buf);
        var string = std.ArrayList(u8).init(fba.allocator());
        try std.json.stringify(self, .{ .emit_null_optional_fields = false }, string.writer());
        return buf[0..string.items.len];
    }

    // stringify into connect operation
    // add CONNECT prefix and \r\n at end
    fn operation(self: Self, buf: []u8) ![]u8 {
        std.mem.copy(u8, buf, "CONNECT ");
        const offset = (try self.stringify(buf[8..])).len;
        std.mem.copy(u8, buf[8 + offset ..], "\r\n");
        return buf[0 .. offset + 8 + 2];
    }
};

test "Connect stringify" {
    const c = Connect{
        .verbose = true,
    };
    var buf: [4096]u8 = undefined;
    var str = try c.stringify(&buf);

    try std.testing.expectEqualStrings(
        \\{"verbose":true,"pedantic":false,"tls_required":false,"headers":false,"lang":"zig","protocol":1}
    , str);

    const c2 = Connect{
        .verbose = true,
        .jwt = "my_jwt_token",
        .sig = "my_signature",
    };
    str = try c2.stringify(&buf);

    try std.testing.expectEqualStrings(
        \\{"verbose":true,"pedantic":false,"tls_required":false,"headers":false,"lang":"zig","protocol":1,"jwt":"my_jwt_token","sig":"my_signature"}
    , str);
}

const libressl = @import("libressl");

// abstraction over raw net socket connection
// upgrade to tls
// reconnect logic
const Net = struct {
    const Options = struct {
        host: []const u8 = "localhost",
        port: u16 = 4222,
        max_reconnect: u8 = 60,
        reconnect_wait: usize = std.time.ns_per_s * 2,
        read_timeout: u32 = 1000, // milliseconds
    };

    allocator: Allocator,
    stream: std.net.Stream,
    reader: std.net.Stream.Reader,
    writer: std.net.Stream.Writer,

    ssl_stream: ?libressl.SslStream = null,
    ssl_writer: ?libressl.SslStream.Writer = null,
    ssl_reader: ?SyncReader = null,

    options: Net.Options,

    const Self = @This();

    pub fn init(allocator: Allocator, options: Net.Options) !Self {
        var stream = try std.net.tcpConnectToHost(allocator, options.host, options.port);
        var self = Self{
            .allocator = allocator,
            .stream = stream,
            .reader = stream.reader(),
            .writer = stream.writer(),
            .options = options,
        };
        try self.setReadTimeout();
        return self;
    }

    fn tcpClient(self: *Self) std.x.net.tcp.Client {
        return std.x.net.tcp.Client{ .socket = std.x.os.Socket{ .fd = self.stream.handle } };
    }

    fn setReadTimeout(self: *Self) !void {
        try self.tcpClient().setReadTimeout(self.options.read_timeout);
    }

    pub fn reconnect(self: *Self) !void {
        self.shutdown();
        const opt = self.options;
        var cnt: usize = 0;
        while (true) {
            log.debug("reconnecting {d} of {d}", .{ cnt, opt.max_reconnect });
            var stream = std.net.tcpConnectToHost(self.allocator, opt.host, opt.port) catch |err| {
                if (cnt < opt.max_reconnect) {
                    std.time.sleep(opt.reconnect_wait);
                    cnt += 1;
                    continue;
                }
                return err;
            };
            self.close();
            self.stream = stream;
            self.reader = stream.reader();
            self.writer = stream.writer();
            try self.setReadTimeout();
            break;
        }
    }

    pub fn close(self: *Self) void {
        if (self.ssl_stream != null) {
            self.ssl_stream.?.deinit();
            self.ssl_stream = null;
            self.ssl_reader = null;
            self.ssl_writer = null;
        } else {
            var tc = self.tcpClient();
            tc.shutdown(.both) catch {};
            tc.deinit();
        }
    }

    pub fn shutdown(self: *Self) void {
        var tc = self.tcpClient();
        tc.shutdown(.both) catch {};
    }

    pub fn write(self: *Self, buf: []const u8) !void {
        if (self.ssl_writer) |writer| {
            return try writer.writeAll(buf);
        }
        return try self.writer.writeAll(buf);
    }

    pub fn read(self: *Self, buf: []u8) !usize {
        if (self.ssl_reader) |reader| {
            return try reader.read(buf);
        }
        return try self.reader.read(buf);
    }

    pub fn wrapTLS(self: *Self) !void {
        var cfg = (libressl.TlsConfigurationParams{}).build() catch unreachable;
        var ss = try libressl.SslStream.wrapClientStream(cfg, self.stream, self.options.host);
        self.ssl_reader = syncReader(ss);
        self.ssl_writer = ss.writer();
        self.ssl_stream = ss;
    }

    // Extend ssl reader to return WouldBlock on read timeout.
    // Reader from SslStream is ignoring read timeout (TLS_WANT_POLLIN):
    //   https://github.com/haze/zig-libressl/blob/fe9c13da13ff5cdc22ea5c023ee6a16459bc89ab/src/SslStream.zig#L101
    pub const SyncReadError = error{ ReadFailure, WouldBlock };

    pub fn syncRead(self: libressl.SslStream, buffer: []u8) SyncReadError!usize {
        var output = libressl.tls.tls_read(self.tls_context, buffer.ptr, buffer.len);
        if (output == libressl.tls.TLS_WANT_POLLIN) {
            return error.WouldBlock;
        }
        if (output < 0) {
            return error.ReadFailure;
        }
        return @intCast(usize, output);
    }

    pub const SyncReader = std.io.Reader(libressl.SslStream, SyncReadError, syncRead);

    pub fn syncReader(ssl: libressl.SslStream) SyncReader {
        return SyncReader{ .context = ssl };
    }
};
