const std = @import("std");
const Thread = std.Thread;

pub fn RingBuffer(comptime T: type, buffer_size: usize) type {
    return struct {
        buffer: [buffer_size]T = undefined,
        mut: Thread.Mutex = Thread.Mutex{},
        reader_cw: Thread.Condition = Thread.Condition{},
        writer_cw: Thread.Condition = Thread.Condition{},
        reader_pos: u64 = 0,
        writer_pos: u64 = 0,
        const Self = @This();

        pub fn get(self: *Self) T {
            while (true) {
                self.mut.lock();
                if (self.writer_pos == self.reader_pos) {
                    self.reader_cw.wait(&self.mut);
                    if (self.writer_pos == self.reader_pos) {
                        self.mut.unlock();
                        continue;
                    }
                }
                var val = self.buffer[self.reader_pos % buffer_size];
                self.reader_pos += 1;
                self.writer_cw.signal();
                self.mut.unlock();
                return val;
            }
        }

        pub fn put(self: *Self, data: T) void {
            self.mut.lock();
            while (true) {
                if (self.writer_pos < self.reader_pos + buffer_size) {
                    self.buffer[self.writer_pos % buffer_size] = data;
                    self.writer_pos += 1;
                    self.reader_cw.signal();
                    self.mut.unlock();
                    return;
                }
                self.writer_cw.wait(&self.mut);
            }
        }
    };
}

/// tests

const expectEqual = std.testing.expectEqual;
const time = std.time;

test "non blocking" {
    var rb = RingBuffer(u8, 2){};
    try expectEqual(2, rb.buffer.len);
    rb.put(@intCast(u8, 100));
    try expectEqual(@intCast(u64, 1), rb.writer_pos);
    try expectEqual(@intCast(u64, 0), rb.reader_pos);

    rb.put(@intCast(u8, 101));
    try expectEqual(@intCast(u64, 2), rb.writer_pos);
    try expectEqual(@intCast(u64, 0), rb.reader_pos);

    try expectEqual(@intCast(u8, 100), rb.get());
    try expectEqual(@intCast(u64, 2), rb.writer_pos);
    try expectEqual(@intCast(u64, 1), rb.reader_pos);

    rb.put(@intCast(u8, 102));
    try expectEqual(@intCast(u64, 3), rb.writer_pos);
    try expectEqual(@intCast(u64, 1), rb.reader_pos);

    try expectEqual(@intCast(u8, 101), rb.get());
    try expectEqual(@intCast(u64, 3), rb.writer_pos);
    try expectEqual(@intCast(u64, 2), rb.reader_pos);

    var i: u64 = 2;
    while (i < 100) : (i += 1) {
        rb.put(@intCast(u8, 100 + i + 1));
        try expectEqual(@intCast(u64, i + 2), rb.writer_pos);
        try expectEqual(@intCast(u64, i), rb.reader_pos);

        try expectEqual(@intCast(u8, 100 + i), rb.get());
        try expectEqual(@intCast(u64, i + 2), rb.writer_pos);
        try expectEqual(@intCast(u64, i + 1), rb.reader_pos);
    }

    try expectEqual(@intCast(u8, 200), rb.get());
    try expectEqual(@intCast(u64, 101), rb.writer_pos);
    try expectEqual(@intCast(u64, 101), rb.reader_pos);
}

const TestRingBuffer = RingBuffer(u8, 2);

const TestWriter = struct {
    rb: *TestRingBuffer,
    sleep: usize = 0,
    fn loop(self: *@This()) void {
        var i: u8 = 0;
        while (i < 100) : (i += 1) {
            if (self.sleep > 0) {
                time.sleep(self.sleep * time.ns_per_ms);
            }
            self.rb.put(i);
        }
    }
};

const TestReader = struct {
    rb: *TestRingBuffer,
    sleep: usize = 0,
    last_val: u8 = 0,
    fn loop(self: *@This()) void {
        while (true) {
            var val = self.rb.get();
            if (self.sleep > 0) {
                time.sleep(self.sleep * time.ns_per_ms);
            }
            if (val>0) {
                expectEqual(self.last_val+1, val) catch unreachable;
            }
            self.last_val = val;
            if (val == 99) {
                return;
            }
        }
    }
};

test "slow consumer" {
    var rb = TestRingBuffer{};

    var writer = TestWriter{ .rb = &rb };
    var reader = TestReader{ .rb = &rb, .sleep = 1 };

    var writer_trd = try Thread.spawn(.{}, TestWriter.loop, .{&writer});
    var reader_trd = try Thread.spawn(.{}, TestReader.loop, .{&reader});

    Thread.join(writer_trd);
    Thread.join(reader_trd);
}

test "slow publisher" {
    var rb = TestRingBuffer{};

    var writer = TestWriter{ .rb = &rb, .sleep = 1 };
    var reader = TestReader{ .rb = &rb};

    var writer_trd = try Thread.spawn(.{}, TestWriter.loop, .{&writer});
    var reader_trd = try Thread.spawn(.{}, TestReader.loop, .{&reader});

    Thread.join(writer_trd);
    Thread.join(reader_trd);
}
