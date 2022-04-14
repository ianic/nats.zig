const std = @import("std");
const event = std.event;
const time = std.time;
const log = std.log;
const nats = @import("nats");
const Allocator = std.mem.Allocator;

pub const io_mode = .evented;
//pub const event_loop_mode = .single_threaded;
pub const log_level: std.log.Level = .info;

const no_msgs: usize = 1024;

pub fn main() !void {
    defer {
        log.warn("done", .{});
        // TODO it didn't exit without this
        std.os.exit(0);
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var loop = event.Loop.instance.?;
    defer loop.deinit();

    var nc = try nats.connect(alloc, loop);
    defer nc.deinit();

    var subscriber = Subscriber{ .nc = nc, .sid = 0 };
    subscriber.sid = try nc.subscribe("test", &nats.MsgHandler.init(&subscriber));

    try loop.runDetached(alloc, publisher, .{nc});

    loop.run();
}

fn publisher(nc: *nats.Conn) void {
    var scratch: [no_msgs]u8 = undefined;

    var i: usize = 0;
    while (i<no_msgs) : (i += 1) {
        nc.publish("test", scratch[0..i]) catch unreachable;
    }
}

const Subscriber = struct {
    nc: *nats.Conn,
    sid: u64,
    msgs_count: u64 = 0,

    pub fn onMsg(self: *Subscriber, msg: nats.Msg) void {
        log.info("{d} subject: '{s}', data len: {d}", .{ self.msgs_count, msg.subject, msg.data().len });
        self.msgs_count += 1;
        if (self.msgs_count == no_msgs) {
            self.nc.close();
        }
    }
};
