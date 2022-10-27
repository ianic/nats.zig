const std = @import("std");
const Allocator = std.mem.Allocator;

const Parser = @import("../src/NonAllocatingParser.zig");
pub const Msg = Parser.Msg;

const sync = @import("sync.zig");
const RingBuffer = @import("RingBuffer.zig").RingBuffer;

const log = std.log.scoped(.nats);

pub const Options = struct {
    host: []const u8 = "localhost",
    port: u16 = 4222,
    creds_file_path: ?[]const u8 = null,
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
    try conn.startLoops();
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

const EventChannel = RingBuffer(Event, 1024);
const CommandChannel = RingBuffer(Command, 1024);

pub const Conn = struct {
    const Self = @This();

    allocator: Allocator,
    conn: sync.Conn,
    connection_no: u16 = 1,

    mut: std.Thread.Mutex = std.Thread.Mutex{},
    event_chan: EventChannel = .{},
    command_chan: CommandChannel = .{},
    status: Status = .connected,

    fn startLoops(self: *Self) !void {
        self.mut.lock();
        defer self.mut.unlock();

        var reader_trd = try std.Thread.spawn(.{}, Self.readLoop, .{ self, self.connection_no });
        reader_trd.detach();
        var writer_trd = try std.Thread.spawn(.{}, Self.writeLoop, .{ self, self.connection_no });
        writer_trd.detach();
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
                self.event_chan.put(Event{ .msg = msg });
            }
            // TODO use read timeout
        }
    }

    fn reconnect_(self: *Self) void {
        std.log.info("{d} starting connect", .{self.connection_no});

        self.conn.reconnect() catch {
            self.close_(.disconnected);
            return;
        };
        self.startLoops() catch {
            // TODO;
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
        self.event_chan.put(Event{ .status_changed = .{ .status = status, .connection_no = self.connection_no } });
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
        self.command_chan.put(Command{ .exit = connection_no });
    }

    fn writeLoop_(self: *Self, connection_no: u16) !void {
        while (self.command_chan.get()) |cmd| {
            switch (cmd) {
                .sub => |subject| {
                    _ = try self.conn.subscribe(subject);
                },
                .unsub => |_| {
                    //try self.conn.unsubscribe(subject),
                    // TODO add implementation
                    //self.conn.unsubscribe()
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
    }

    fn close_(self: *Self, status: Status) void {
        self.mut.lock();
        defer self.mut.unlock();

        self.conn.net.shutdown();
        self.setStatus(status);
        self.event_chan.close();
    }

    pub fn close(self: *Self) void {
        self.command_chan.put(Command.close);
        self.command_chan.close();
    }

    pub fn subscribe(self: *Self, subject: []const u8) void {
        self.command_chan.put(Command{ .sub = subject });
    }

    pub fn unsubscribe(self: *Self, subject: []const u8) void {
        self.command_chan.put(Command{ .unsub = subject });
    }

    pub fn publish(self: *Self, subject: []const u8, payload: []const u8) void {
        // TODO: need to copy payload
        self.command_chan.put(Command{ .publish = .{ .subject = subject, .payload = payload } });
    }

    pub fn in(self: *Self) ?Event {
        return self.event_chan.get();
    }

    pub fn deinit(self: *Self) void {
        std.log.info("{d} deinit", .{self.connection_no});
        self.conn.deinit();
        self.allocator.destroy(self);
        // TODO should I join on the threads (they are detached right now)
    }
};
