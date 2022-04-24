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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(alloc);
    defer nc.deinit();
    var nc_frame = async nc.run();

    var subscriber = Subscriber{ .nc = nc, .sid = 0 };
    subscriber.sid = try nc.subscribe("test", &nats.MsgHandler.init(&subscriber));

    try publish(nc);

    try await nc_frame;
}

fn publish(nc: *nats.Conn) !void {
    var scratch: [no_msgs]u8 = undefined;

    var i: usize = 0;
    while (i<no_msgs) : (i += 1) {
        try nc.publish("test", scratch[0..i]);
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
            nosuspend {
                self.nc.close() catch {};
            }
        }
    }
};
