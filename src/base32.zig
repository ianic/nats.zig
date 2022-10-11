const std = @import("std");
const testing = std.testing;

pub const Encoder = struct {
    const Self = @This();

    buffer: []const u8,
    index: ?usize,
    bit_off: u3,

    /// Init the encoder.
    pub fn init(buffer: []const u8) Encoder {
        return .{
            .buffer = buffer,
            .index = 0,
            .bit_off = 0,
        };
    }

    /// Calculate the Base32-encoded size of an array of bytes.
    pub fn calcSize(source_len: usize) usize {
        const source_len_bits = source_len * 8;
        return source_len_bits / 5 + (if (source_len_bits % 5 > 0) @as(usize, 1) else 0);
    }

    /// Encode some data as Base32.
    /// Note that `dest.len` must at least be as big as `Encoder.calcSize(source.len)`.
    pub fn encode(dest: []u8, source: []const u8) []const u8 {
        const out_len = calcSize(source.len);
        std.debug.assert(dest.len >= out_len);

        var e = init(source);
        for (dest[0..out_len]) |*b| b.* = e.next() orelse unreachable;
        return dest[0..out_len];
    }

    /// Calculate the amount of bits can be read from `self.buffer[self.index]`,
    /// with a maximum of 5 and an offset of `self.bit_off`.
    fn frontBitsLen(self: *const Self) u3 {
        // bit_off   frontBitsLen
        // 0         5
        // 1         5
        // 2         5
        // 3         5
        // 4         4
        // 5         3
        // 6         2
        // 7         1
        return if (self.bit_off <= 3) 5 else 7 - self.bit_off + 1;
    }

    /// Get the bits of `self.buffer[self.index]`, read with an offset of `self.bit_off`,
    /// aligned to the left of the 5-bit unsigned integer.
    /// Returns null if `self.index` is null.
    /// An illustration of its behaviour, with `self.buffer[self.index]` being 0b10010111:
    /// | `self.bit_off` | `frontBits` |
    /// |----------------|-------------|
    /// | 0              | 0b10010     |
    /// | 1              | 0b00101     |
    /// | 2              | 0b01011     |
    /// | 3              | 0b10111     |
    /// | 4              | 0b01110     |
    /// | 5              | 0b11100     |
    /// | 6              | 0b11000     |
    /// | 7              | 0b10000     |
    fn frontBits(self: *const Self) ?u5 {
        const index = self.index orelse return null;
        const bits = self.buffer[index];
        if (self.bit_off >= 4) return @truncate(u5, bits << (self.bit_off - 3));
        return @truncate(u5, bits >> (3 - self.bit_off));
    }

    /// Get the bits of `self.buffer[self.index]` with the maximum amount specified by the `bits` parameter,
    /// aligned to the right of the 5-bit unsigned integer.
    /// Because a 5-bit integer is returned, not more than 5 bits can be read. `bits` must not be greater than 5.
    /// An illustration of its behaviour, with `self.buffer[self.index]` being 0b11101001:
    /// | `bits` | `backBits` |
    /// |--------|------------|
    /// | 0      | 0b00000    |
    /// | 1      | 0b00001    |
    /// | 2      | 0b00011    |
    /// | 3      | 0b00111    |
    /// | 4      | 0b01110    |
    /// | 5      | 0b11101    |
    fn backBits(self: *const Self, bits: u3) u5 {
        std.debug.assert(bits <= 5);
        if (bits == 0 or self.index == null) return 0;
        return @truncate(u5, self.buffer[self.index.?] >> (7 - bits + 1));
    }

    /// Get the next 5-bit integer, read from `self.buffer`.
    fn nextU5(self: *Self) ?u5 {
        // `self.buffer` is read 5 bits at a time by `nextU5`.
        // Because of the elements of `self.buffer` being 8 bits each, we need to
        // read from 2 bytes from `self.buffer` to return a whole u5.
        // `front_bits` are the bits that come first, read from `self.buffer[self.index]`.
        // `back_bits` are the bits that come last, read from `self.buffer[self.index + 1]`.
        // `back_bits` is only used when we can't read 5 bits from `self.buffer[self.index]`.

        const front_bits = self.frontBits() orelse return null;
        const n_front_bits = self.frontBitsLen();

        var back_bits: u5 = 0;
        if (self.bit_off >= 3) {
            // Next time we'll need to read from the next byte in `self.buffer`.
            // We may need to grab the back bits from that next byte for this call too (if it exist).
            self.bit_off -= 3; // same as self.bit_off + 5 - 8
            const new_index = self.index.? + 1;
            if (self.buffer.len > new_index) {
                self.index = new_index;
                back_bits = self.backBits(5 - n_front_bits);
            } else {
                self.index = null;
            }
        } else {
            // We need to read from the current byte in the next call to `nextU5` too.
            self.bit_off += 5;
        }

        return front_bits | back_bits;
    }

    /// Get the corresponding ASCII character for 5 bits of the input.
    fn char(unencoded: u5) u8 {
        return unencoded + (if (unencoded < 26) @as(u8, 'A') else '2' - 26);
    }

    /// Get the next byte of the encoded buffer.
    pub fn next(self: *Self) ?u8 {
        const unencoded = self.nextU5() orelse return null;
        return char(unencoded);
    }
};

pub const DecodeError = error{CorruptInput};

pub const Decoder = struct {
    const Self = @This();

    buffer: []const u8,
    index: ?usize,

    buf: u8,
    buf_len: u4,

    /// Init the decoder.
    pub fn init(buffer: []const u8) Self {
        return .{
            .buffer = buffer,
            .index = 0,
            .buf_len = 0,
            .buf = 0,
        };
    }

    /// Calculate the size of a Base32-encoded array of bytes.
    pub fn calcSize(source_len: usize) usize {
        return safeMulDiv(source_len, 5, 8);
    }

    /// Decode a slice of Base32-encoded data.
    /// Note that `dest.len` must at least be as big as `Decoder.calcSize(source.len)`.
    pub fn decode(dest: []u8, source: []const u8) DecodeError![]const u8 {
        const out_len = calcSize(source.len);
        std.debug.assert(dest.len >= out_len);

        var d = init(source);
        for (dest[0..out_len]) |*b| b.* = (try d.next()) orelse unreachable;
        return dest[0..out_len];
    }

    /// Get a character from the buffer.
    fn decodeChar(c: u8) DecodeError!u5 {
        if (c >= 'A' and c <= 'Z') {
            return @truncate(u5, c - @as(u8, 'A'));
        } else if (c >= '2' and c <= '9') {
            // '2' -> 26
            return @truncate(u5, c - @as(u8, '2') + 26);
        } else {
            return error.CorruptInput;
        }
    }

    /// Get the next 5-bit decoded character, read from `self.buffer`.
    fn nextU5(self: *Self) DecodeError!?u5 {
        const index = self.index orelse return null;
        self.index = if (index + 1 < self.buffer.len) index + 1 else null;
        return try decodeChar(self.buffer[index]);
    }

    /// Get the next byte of the decoded buffer.
    pub fn next(self: *Self) DecodeError!?u8 {
        while (true) {
            // Read a character and decode it.
            const c = (try self.nextU5()) orelse break;
            // Check how many bits we can write to the buffer.
            const buf_remaining_len = 8 - self.buf_len;
            // Calculate how many bytes we will write to the buffer (the decoded character represents 5 bits).
            const buf_write_len = if (buf_remaining_len > 5) 5 else buf_remaining_len;
            // Calculate how many bits of the decoded remain when we've written part of it to the buffer.
            const c_remaining_len = 5 - buf_write_len;
            // Write (the first) part of the decoded character to the buffer.
            self.buf |= (@as(u8, c) << 3) >> @truncate(u3, self.buf_len);
            self.buf_len += buf_write_len;
            if (self.buf_len == 8) {
                // The buffer is full, we can return a byte.
                const ret = self.buf;
                self.buf_len = c_remaining_len;
                self.buf = 0;
                if (buf_write_len != 5) {
                    // We didn't write the entire decoded character to the buffer.
                    // Write the remaining part to the beginning of the buffer.
                    self.buf = @as(u8, c) << @truncate(u3, buf_write_len + 3);
                }
                return ret;
            }
        }

        // We aren't able to read any characters anymore.
        // If the buffer doesn't contain any (actual) data we can stop decoding.
        // Otherwise, we can return what remains in the buffer, and stop decoding
        // after having done that.
        if (self.buf == 0 and self.buf_len < 5) return null;

        const ret = self.buf;
        self.buf_len = 0;
        self.buf = 0;

        return ret;
    }
};

// Taken from std.time.
// Calculate (a * b) / c without risk of overflowing too early because of the
// multiplication.
fn safeMulDiv(a: u64, b: u64, c: u64) u64 {
    const q = a / c;
    const r = a % c;
    // (a * b) / c == (a / c) * b + ((a % c) * b) / c
    return (q * b) + (r * b) / c;
}

test {
    const encoded = "ORUGS4ZANFZSAYJAORSXG5A";
    const decoded = "this is a test";

    var decode_buf: [Decoder.calcSize(encoded.len)]u8 = undefined;
    const decode_res = try Decoder.decode(&decode_buf, encoded);

    try testing.expectEqualStrings(decoded, decode_res);

    var encode_buf: [Encoder.calcSize(decoded.len)]u8 = undefined;
    const encode_res = Encoder.encode(&encode_buf, decoded);

    try testing.expectEqualStrings(encoded, encode_res);
}

test {
    const encoded = "SNAH7EH5X4P5R2M2RGF3LVAL6NRFIXLN2E67O6FNRUQ4JCQBPL64GEBPLY";
    const decoded = &[_]u8{ 0x93, 0x40, 0x7f, 0x90, 0xfd, 0xbf, 0x1f, 0xd8, 0xe9, 0x9a, 0x89, 0x8b, 0xb5, 0xd4, 0x0b, 0xf3, 0x62, 0x54, 0x5d, 0x6d, 0xd1, 0x3d, 0xf7, 0x78, 0xad, 0x8d, 0x21, 0xc4, 0x8a, 0x01, 0x7a, 0xfd, 0xc3, 0x10, 0x2f, 0x5e };

    var decode_buf: [Decoder.calcSize(encoded.len)]u8 = undefined;
    const decode_res = try Decoder.decode(&decode_buf, encoded);

    try testing.expectEqualSlices(u8, decoded, decode_res);

    var encode_buf: [Encoder.calcSize(decoded.len)]u8 = undefined;
    const encode_res = Encoder.encode(&encode_buf, decoded);

    try testing.expectEqualSlices(u8, encoded, encode_res);
}
