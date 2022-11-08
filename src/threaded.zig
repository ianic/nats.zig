const std = @import("std");
const Allocator = std.mem.Allocator;

const Parser = @import("../src/NonAllocatingParser.zig");
pub const Msg = Parser.Msg;

const sync = @import("sync.zig");
const Mpsc = @import("channel.zig").Mpsc;

const log = std.log.scoped(.nats);

pub const Options = struct {
    host: []const u8 = "localhost",
    port: u16 = 4222,
    creds_file_path: ?[]const u8 = null,
    trap_signals: bool = true,
};

pub fn connect(allocator: Allocator, options: Options) !*Conn {
    var conn = try allocator.create(Conn);
    errdefer allocator.destroy(conn);

    conn.* = .{
        .allocator = allocator,
        .conn = try sync.connect(allocator, .{
            .host = options.host,
            .port = options.port,
            .creds_file_path = options.creds_file_path,
            .with_reconnect = false,
        }),
    };
    errdefer conn.deinit();

    try conn.startLoops();
    if (options.trap_signals) {
        try setSignalHandler(conn);
    }
    return conn;
}

pub const EventTag = enum {
    msg,
    // err,
    status_changed,
};

pub const Event = union(EventTag) {
    msg: Msg,
    //err: Error,
    status_changed: StatusEvent,
};

pub const StatusEvent = struct {
    status: Status,
    connection_no: u16,
};

pub const Status = enum {
    connecting,
    connected,
    disconnected,
    closed,
};

pub const CommandTag = enum {
    sub,
    unsub,
    publish,
    close,
    exit,
};

pub const Command = union(CommandTag) {
    sub: []const u8,
    unsub: []const u8,
    publish: PublishCommand,
    close,
    exit: usize,
};

pub const PublishCommand = struct {
    subject: []const u8,
    payload: []const u8,
};

const EventChannel = Mpsc(Event, 1024);
const CommandChannel = Mpsc(Command, 1024);

pub const Conn = struct {
    const Self = @This();

    allocator: Allocator,
    conn: sync.Conn,
    connection_no: u16 = 1,

    mut: std.Thread.Mutex = std.Thread.Mutex{},
    event_chan: EventChannel = .{},
    command_chan: CommandChannel = .{},
    status: Status = .connected,
    unprocessed_cmd: ?Command = null,
    reader_trd: ?std.Thread = null,
    writer_trd: ?std.Thread = null,

    fn startLoops(self: *Self) !void {
        self.mut.lock();
        defer self.mut.unlock();
        if (self.reader_trd) |t| {
            t.join();
        }
        self.reader_trd = try std.Thread.spawn(.{}, Self.readLoop, .{ self, self.connection_no });
        if (self.writer_trd) |t| {
            t.join();
        }
        self.writer_trd = try std.Thread.spawn(.{}, Self.writeLoop, .{ self, self.connection_no });
        self.setStatus(.connected);
    }

    fn readLoop(self: *Self, connection_no: u16) void {
        defer std.log.info("{d} read loop ended", .{connection_no});
        self.readLoop_() catch |err| {
            std.log.info("{d} read loop error {}", .{ connection_no, err });
            if (self.canReconnect(connection_no)) {
                self.exitWriteLoop(connection_no);
                self.reconnect_();
            }
        };
    }

    fn readLoop_(self: *Self) !void {
        while (true) {
            if (try self.conn.read()) |msg| {
                // TODO make copy of the msg payload
                try self.event_chan.send(Event{ .msg = msg });
            }
            // TODO use read timeout
        }
    }

    fn reconnect_(self: *Self) void {
        std.log.info("{d} starting reconnect", .{self.connection_no});
        self.conn.reconnect() catch |err| {
            std.log.info("{d} reconnect error {}", .{ self.connection_no, err });
            self.close_(.disconnected);
            return;
        };
        self.startLoops() catch |err| {
            std.log.info("{d} start loops error {}", .{ self.connection_no, err });
            self.close_(.disconnected);
            return;
        };
    }

    fn canReconnect(self: *Self, connection_no: u16) bool {
        self.mut.lock();
        if (!(self.connection_no == connection_no and self.status == .connected)) {
            self.mut.unlock();
            return false;
        }
        self.connection_no += 1;
        self.setStatus(.connecting);
        self.mut.unlock();
        return true;
    }

    fn setStatus(self: *Self, status: Status) void {
        self.status = status;
        self.event_chan.send(Event{ .status_changed = .{ .status = status, .connection_no = self.connection_no } }) catch {};
    }

    fn writeLoop(self: *Self, connection_no: u16) void {
        defer std.log.info("{d} write loop ended", .{connection_no});
        self.writeLoop_(connection_no) catch |err| {
            std.log.info("{d} write loop error {}", .{ connection_no, err });
            if (self.canReconnect(connection_no)) {
                self.conn.net.shutdown();
                self.reconnect_();
            }
        };
    }

    fn exitWriteLoop(self: *Self, connection_no: u16) void {
        _ = self.command_chan.trySend(Command{ .exit = connection_no }) catch {
            unreachable;
        };
    }

    fn writeLoop_(self: *Self, connection_no: u16) !void {
        if (self.unprocessed_cmd) |cmd| {
            try self.processCmd(connection_no, cmd);
            self.unprocessed_cmd = null;
        }

        while (self.command_chan.recv()) |cmd| {
            self.processCmd(connection_no, cmd) catch |err| {
                if (err == error.BrokenPipe) {
                    self.unprocessed_cmd = cmd;
                    return err;
                } else {
                    log.warn("process cmd error: {}", .{err});
                }
            };
        }
    }

    fn processCmd(self: *Self, connection_no: u16, cmd: Command) !void {
        switch (cmd) {
            .sub => |subject| {
                _ = try self.conn.subscribe(subject);
            },
            .unsub => |subject| {
                _ = try self.conn.unsubscribe(subject);
            },
            .publish => |c| {
                try self.conn.publish(c.subject, c.payload);
                // TODO publish ack
                //self.read_chan.put();
            },
            .close => self.close_(.closed),
            .exit => |cn| {
                if (cn == connection_no) {
                    return;
                }
            },
        }
    }

    fn close_(self: *Self, status: Status) void {
        self.mut.lock();
        defer self.mut.unlock();

        self.conn.net.shutdown();
        self.setStatus(status);
        self.event_chan.close();
    }

    pub fn close(self: *Self) void {
        self.command_chan.send(Command.close) catch {
            unreachable;
        };
        self.command_chan.close();
    }

    pub fn subscribe(self: *Self, subject: []const u8) void {
        self.command_chan.send(Command{ .sub = subject }) catch {
            unreachable;
        };
    }

    pub fn unsubscribe(self: *Self, subject: []const u8) void {
        self.command_chan.send(Command{ .unsub = subject }) catch {
            unreachable;
        };
    }

    pub fn publish(self: *Self, subject: []const u8, payload: []const u8) void {
        // TODO: need to copy payload
        self.command_chan.send(Command{ .publish = .{ .subject = subject, .payload = payload } }) catch {
            unreachable;
        };
    }

    pub fn recv(self: *Self) ?Event {
        return self.event_chan.recv();
    }

    fn signal(self: *Self, sig: c_int) void {
        std.log.info("got signal {d}", .{sig});
        if (sig == std.os.SIG.INT or sig == std.os.SIG.TERM) {
            self.close();
        }
    }

    pub fn deinit(self: *Self) void {
        std.log.info("{d} deinit", .{self.connection_no});
        self.conn.deinit();
        if (self.reader_trd) |t| {
            t.join();
        }
        if (self.writer_trd) |t| {
            t.join();
        }
        self.allocator.destroy(self);
    }
};

// TODO: how to to this without global var signal_target
fn setSignalHandler(conn: *Conn) !void {
    signal_target = conn;
    var act = std.os.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask = std.os.empty_sigset,
        .flags = 0,
    };
    try std.os.sigaction(std.os.SIG.INT, &act, null);
    try std.os.sigaction(std.os.SIG.TERM, &act, null);
    try std.os.sigaction(std.os.SIG.USR1, &act, null);
    try std.os.sigaction(std.os.SIG.USR2, &act, null);
}

var signal_target: ?*Conn = null;

fn sigHandler(sig: c_int) callconv(.C) void {
    if (signal_target) |conn| {
        conn.signal(sig);
    }
}
