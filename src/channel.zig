const std = @import("std");
const Thread = std.Thread;

// Intended for use in multi threaded envrionment where one thread is writing to
// the Mpsc and another is reading. Both put and get operations are
// blocking. Put in the case when RB is full, get in case when there is no new
// messages.
//
// Uses conditon variables. Note to myself: cw.wait unlocks waits for signal and
// then locks provided mutex.
pub fn Mpsc(comptime T: type, comptime buffer_size: usize) type {
    return struct {
        const Self = @This();

        buffer: [buffer_size]T = undefined,
        mut: Thread.Mutex = Thread.Mutex{},
        cw: Thread.Condition = Thread.Condition{},
        reader_pos: usize = 0,
        writer_pos: usize = 0,
        closed: bool = false,

        // Use it in loop:
        // while (rb.recv()) |val| {...}
        // Will block when chan is emtpy, and return null (exit while) when chan is closed.
        pub fn recv(self: *Self) ?T {
            return self.recv_(0) catch {
                unreachable; // can't get timeout error
            };
        }

        pub const RecvResultTag = enum {
            data,
            timeout,
            closed,
        };

        pub const RecvResult = union(RecvResultTag) {
            data: T,
            timeout: void,
            closed: void,
        };

        pub fn recvTimeout(self: *Self, timeout_ns: usize) RecvResult {
            const val = self.recv_(timeout_ns) catch {
                return RecvResult.timeout;
            };
            if (val == null) {
                return .closed;
            }
            return .{ .data = val.? };
        }

        fn recv_(self: *Self, timeout_ns: u64) error{Timeout}!?T {
            while (true) {
                self.mut.lock();
                if (self.empty()) {
                    if (self.closed) { // returns null when closed
                        self.mut.unlock();
                        return null;
                    }
                    if (timeout_ns > 0) {
                        errdefer self.mut.unlock();
                        try self.cw.timedWait(&self.mut, timeout_ns);
                    } else {
                        self.cw.wait(&self.mut); // block until writer puts something
                    }
                    if (self.empty()) { // handle spurious wakeup
                        self.mut.unlock();
                        continue;
                    }
                }
                if (self.full()) {
                    self.cw.signal();
                }
                var val = self.buffer[self.reader_pos % buffer_size];
                self.reader_pos += 1;
                self.mut.unlock();
                return val;
            }
        }

        fn empty(self: *Self) bool {
            return self.writer_pos == self.reader_pos;
        }

        fn full(self: *Self) bool {
            return self.writer_pos == self.reader_pos + buffer_size;
        }

        // Will block when chan is full.
        // Panics if send after close.
        pub fn send(self: *Self, data: T) error{Closed}!void {
            _ = try self.send_(data, true);
        }

        // Returns false if chan is full and data is not pushed to the chan.
        pub fn trySend(self: *Self, data: T) error{Closed}!bool {
            return try self.send_(data, false);
        }

        // Returns false if send is non blocking and channel is full (can't send).
        fn send_(self: *Self, data: T, blocking: bool) error{Closed}!bool {
            self.mut.lock();
            if (self.closed) { // panics when closed
                self.mut.unlock();
                return error.Closed;
            }
            while (true) {
                if (self.full()) {
                    if (!blocking) {
                        self.mut.unlock();
                        return false;
                    }
                    self.cw.wait(&self.mut); // block until reader advances
                    continue;
                }
                if (self.empty()) {
                    self.cw.signal();
                }
                self.buffer[self.writer_pos % buffer_size] = data;
                self.writer_pos += 1;
                self.mut.unlock();
                return true;
            }
        }

        // Closes channel. You should not send to the chan after close.
        pub fn close(self: *Self) void {
            self.mut.lock();
            defer self.mut.unlock();
            if (self.closed) {
                return;
            }
            self.closed = true;
            self.cw.signal();
        }
    };
}

/// tests
const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;
const time = std.time;

test "non blocking" {
    var ch = Mpsc(u8, 2){};
    try expectEqual(2, ch.buffer.len);
    try ch.send(@intCast(u8, 100));
    try expectEqual(@intCast(usize, 1), ch.writer_pos);
    try expectEqual(@intCast(usize, 0), ch.reader_pos);

    try ch.send(@intCast(u8, 101));
    try expectEqual(@intCast(usize, 2), ch.writer_pos);
    try expectEqual(@intCast(usize, 0), ch.reader_pos);

    try expectEqual(@intCast(u8, 100), ch.recv().?);
    try expectEqual(@intCast(usize, 2), ch.writer_pos);
    try expectEqual(@intCast(usize, 1), ch.reader_pos);

    try ch.send(@intCast(u8, 102));
    try expectEqual(@intCast(usize, 3), ch.writer_pos);
    try expectEqual(@intCast(usize, 1), ch.reader_pos);

    try expectEqual(@intCast(u8, 101), ch.recv().?);
    try expectEqual(@intCast(usize, 3), ch.writer_pos);
    try expectEqual(@intCast(usize, 2), ch.reader_pos);

    var i: usize = 2;
    while (i < 100) : (i += 1) {
        try ch.send(@intCast(u8, 100 + i + 1));
        try expectEqual(@intCast(usize, i + 2), ch.writer_pos);
        try expectEqual(@intCast(usize, i), ch.reader_pos);

        try expectEqual(@intCast(u8, 100 + i), ch.recv().?);
        try expectEqual(@intCast(usize, i + 2), ch.writer_pos);
        try expectEqual(@intCast(usize, i + 1), ch.reader_pos);
    }

    try expectEqual(@intCast(u8, 200), ch.recv().?);
    try expectEqual(@intCast(usize, 101), ch.writer_pos);
    try expectEqual(@intCast(usize, 101), ch.reader_pos);
}

test "try send" {
    var ch = Mpsc(u8, 2){};
    try expect(try ch.trySend(@intCast(u8, 100)));
    try expect(try ch.trySend(@intCast(u8, 101)));
    try expect(!try ch.trySend(@intCast(u8, 102))); // full trySend returns false
    try expectEqual(@intCast(u8, 100), ch.recv().?);
    try expect(try ch.trySend(@intCast(u8, 102)));
}

test "recvTimeout results" {
    const Channel = Mpsc(u8, 2);
    var ch = Channel{};
    try expectEqual(Channel.RecvResult.timeout, ch.recvTimeout(1));

    try expect(try ch.trySend(@intCast(u8, 100)));
    const r = ch.recvTimeout(1);
    try expectEqual(Channel.RecvResult.data, r);
    try expectEqual(r.data, 100);

    try expect(try ch.trySend(@intCast(u8, 101)));
    ch.close();
    try expectEqual(Channel.RecvResult.data, ch.recvTimeout(1));
    try expectEqual(Channel.RecvResult.closed, ch.recvTimeout(1));
    try expectEqual(Channel.RecvResult.closed, ch.recvTimeout(1));
}

const TestMpsc = Mpsc(u8, 2);

const TestWriter = struct {
    ch: *TestMpsc,
    sleep: usize = 0,
    fn loop(self: *@This()) void {
        var i: u8 = 0;
        while (i < 100) : (i += 1) {
            if (self.sleep > 0) {
                time.sleep(self.sleep * time.ns_per_ms);
            }
            self.ch.send(i) catch {
                unreachable;
            };
        }
        self.ch.close();
    }
};

const TestReader = struct {
    ch: *TestMpsc,
    sleep: usize = 0,
    last_val: u8 = 0,
    fn loop(self: *@This()) void {
        while (self.ch.recv()) |val| {
            if (self.sleep > 0) {
                time.sleep(self.sleep * time.ns_per_ms);
            }
            if (val > 0) {
                expectEqual(self.last_val + 1, val) catch unreachable;
            }
            self.last_val = val;
        }
    }
};

test "slow consumer" {
    var ch = TestMpsc{};

    var writer = TestWriter{ .ch = &ch };
    var reader = TestReader{ .ch = &ch, .sleep = 1 };

    var writer_trd = try Thread.spawn(.{}, TestWriter.loop, .{&writer});
    var reader_trd = try Thread.spawn(.{}, TestReader.loop, .{&reader});

    Thread.join(writer_trd);
    Thread.join(reader_trd);
    try expectEqual(reader.last_val, 99);
}

test "slow publisher" {
    var ch = TestMpsc{};

    var writer = TestWriter{ .ch = &ch, .sleep = 1 };
    var reader = TestReader{ .ch = &ch };

    var writer_trd = try Thread.spawn(.{}, TestWriter.loop, .{&writer});
    var reader_trd = try Thread.spawn(.{}, TestReader.loop, .{&reader});

    Thread.join(writer_trd);
    Thread.join(reader_trd);
    try expectEqual(reader.last_val, 99);
}
