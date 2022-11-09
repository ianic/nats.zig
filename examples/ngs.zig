const std = @import("std");
const nats = @import("nats-sync");

pub const scope_levels = [_]std.log.ScopeLevel{
    .{ .scope = .nats, .level = .info },
};

// Example usage:
// ngs my_creds_file.creds my_subject

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = std.os.argv;
    if (argv.len < 3) {
        std.debug.print("USAGE: {s} [creds_file_path] [subject]\n", .{argv[0]});
        return;
    }
    var creds_file_path = std.mem.span(argv[1]);
    const subject = std.mem.span(argv[2]);

    var conn = try nats.connect(allocator, .{
        .host = "connect.ngs.global",
        .creds_file_path = creds_file_path,
    });
    defer conn.close();

    _ = try conn.subscribe(subject);

    // start publisher
    const no_msgs: u8 = 10;
    var trd = try std.Thread.spawn(.{}, publish, .{ &conn, subject, no_msgs });

    // subscriber
    var msgs: u8 = 0;
    while (true) {
        if (try conn.read()) |msg| {
            std.log.debug("got msg {d}", .{msg.payload.?[0]});
            msgs += 1;
            if (msgs == no_msgs) {
                break;
            }
        }
    }
    trd.join();
}

fn publish(conn: *nats.Conn, subject: []const u8, no_msgs: u8) void {
    var i: u8 = 0;
    while (i < no_msgs) : (i += 1) {
        conn.publish(subject, &[_]u8{i}) catch {
            unreachable;
        };
    }
}
