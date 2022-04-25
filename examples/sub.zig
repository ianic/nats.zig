const std = @import("std");
const nats = @import("nats");
const log = std.log;
const os = std.os;

pub const log_level: std.log.Level = .info;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var nc = try nats.connect(alloc);
    defer nc.deinit();
    try nats.closeOnTerm(nc);

    _ = try nc.subscribe("foo");
    while (nc.read()) |msg| {
        log.info("msg received: {s}", .{msg.data()});
        msg.deinit(alloc);
    }
}
