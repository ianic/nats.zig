const std = @import("std");
const event = std.event;
const nats = @import("nats");
const info = std.log.info;
const log = std.log;
const print = std.debug.print;

pub const io_mode = .evented;
//pub const event_loop_mode = .single_threaded;
pub const log_level: std.log.Level = .info;

pub fn main() !void {
    defer {
        log.debug("done", .{});
    }
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(alloc);
    defer nc.deinit();
    var nc_frame = async nc.run();

    var subscriber = Subscriber{ .nc = nc, .sid = 0 };
    subscriber.sid = try nc.subscribe("foo", &nats.MsgHandler.init(&subscriber));

    try await nc_frame;
}

const Subscriber = struct {
    nc: *nats.Conn,
    sid: u64,
    msgs_count: u64 = 0,

    pub fn onMsg(self: *Subscriber, msg: nats.Msg) void {
        info("msg subject: '{s}', data: '{s}'", .{ msg.subject, msg.data() });
        //print(".", .{});
        self.msgs_count += 1;
        //unsubscribe example
        // if (self.msgs_count >= 2) {
        //     nosuspend {
        //         self.nc.unSubscribe(self.sid) catch {};
        //     }
        //     self.nc.close();
        // }
    }
};
