const std = @import("std");
const event = std.event;
const nats = @import("nats");
const info = std.log.info;
const log = std.log;

pub const io_mode = .evented;
pub const event_loop_mode = .single_threaded;
pub const log_level: std.log.Level = .info;

pub fn main() !void {
    // TODO it didn't exit without this
    //defer std.os.exit(0);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var loop = event.Loop.instance.?;
    defer loop.deinit();

    var nc = try nats.connect(alloc, loop);
    defer nc.deinit();

    var handler = Handler{ .nc = nc, .sid = 0 };
    handler.sid = try nc.subscribe("foo", &nats.MsgHandler.init(&handler));

    loop.run();
}

const Handler = struct {
    nc: *nats.Conn,
    sid: u64,
    msgs_count: u64 = 0,

    pub fn onMsg(self: *Handler, msg: nats.Msg) void {
        info("msg subject: '{s}', data: '{s}'", .{ msg.subject, msg.data() });
        self.msgs_count += 1;
        // unsubscribe example
        // if (self.msgs_count >= 2) {
        //     self.nc.unSubscribe(self.sid) catch {};
        //     self.nc.close();
        // }
    }
};
