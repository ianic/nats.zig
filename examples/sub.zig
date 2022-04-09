const std = @import("std");
const event = std.event;
const print = std.debug.print;
const Allocator = std.mem.Allocator;
const nats = @import("nats");

pub const io_mode = .evented;
pub const event_loop_mode = .single_threaded;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var loop = event.Loop.instance.?;
    defer loop.deinit();

    var nc = try nats.connect(alloc);
    defer nc.deinit();

    try loop.runDetached(alloc, nats.run, .{&nc});

    var handler = Handler{ .nc = &nc, .sid = 0 };
    handler.sid = try nc.subscribe("foo", &nats.MsgHandler.init(&handler));

    loop.run();
}

const Handler = struct {
    nc: *nats.Conn,
    sid: u64,
    msgs_count: u64 = 0,

    pub fn onMsg(self: *Handler, msg: nats.Msg) void {
        print("onMsg subject: {s}, data: {s}\n", .{ msg.subject, msg.data() });
        self.msgs_count += 1;
        if (self.msgs_count >= 2) {
            self.nc.unSubscribe(self.sid) catch {};
        }
    }
};
