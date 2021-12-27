package com.zmh.fastlog.utils;

import javax.annotation.Nullable;

import static java.lang.System.arraycopy;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferUtils {

    private static byte[] ensureBufferLength(byte[] buffer, int expectLength) {
        if (isNull(buffer) || buffer.length < expectLength) {
            byte[] result = new byte[expectLength];
            if (nonNull(buffer)) {
                arraycopy(buffer, 0, result, 0, buffer.length);
            }
            return result;
        }
        return buffer;
    }

    public static String readString(byte[] bytes, @Nullable char[] tempBuffer) {
        int length = bytes.length >> 1;
        char[] buffer = (nonNull(tempBuffer) && tempBuffer.length >= length) ? tempBuffer : new char[length];

        int pos = 0;
        for (int i = 0; i < length; i++) {
            buffer[i] = (char) (((bytes[pos++] & 0x00FF) << 8) + (bytes[pos++] & 0x00FF));
        }
        return new String(buffer);
    }

    // Little Endian
    public static byte[] longToByte(long lng) {
        return new byte[]{
            (byte) lng,
            (byte) (lng >> 8),
            (byte) (lng >> 16),
            (byte) (lng >> 24),
            (byte) (lng >> 32),
            (byte) (lng >> 40),
            (byte) (lng >> 48),
            (byte) (lng >> 56)
        };
    }

    public static long byteToLong(byte[] b, int startPos) {
        return ((long) b[startPos++] & 0xff)
            | ((long) b[startPos++] & 0xff) << 8
            | ((long) b[startPos++] & 0xff) << 16
            | ((long) b[startPos++] & 0xff) << 24
            | ((long) b[startPos++] & 0xff) << 32
            | ((long) b[startPos++] & 0xff) << 40
            | ((long) b[startPos++] & 0xff) << 48
            | ((long) b[startPos] << 56)
            ;
    }

    // Big Endian
    public static byte[] longToByteBE(long lng) {
        return new byte[]{
            (byte) (lng >> 56),
            (byte) (lng >> 48),
            (byte) (lng >> 32),
            (byte) (lng >> 40),
            (byte) (lng >> 24),
            (byte) (lng >> 16),
            (byte) (lng >> 8),
            (byte) lng,
        };
    }

    public static long byteToLongBE(byte[] b, int startPos) {
        return ((long) b[startPos++] << 56)
            | ((long) b[startPos++] & 0xff) << 48
            | ((long) b[startPos++] & 0xff) << 40
            | ((long) b[startPos++] & 0xff) << 32
            | ((long) b[startPos++] & 0xff) << 24
            | ((long) b[startPos++] & 0xff) << 16
            | ((long) b[startPos++] & 0xff) << 8
            | ((long) b[startPos] & 0xff)
            ;
    }

    public static byte[] writeLongToBuffer(long lng, byte[] destBuffer, int startPos) {
        int expectLength = startPos + 8;
        byte[] result = ensureBufferLength(destBuffer, expectLength);
        result[startPos++] = (byte) lng;
        result[startPos++] = (byte) (lng >> 8);
        result[startPos++] = (byte) (lng >> 16);
        result[startPos++] = (byte) (lng >> 24);
        result[startPos++] = (byte) (lng >> 32);
        result[startPos++] = (byte) (lng >> 40);
        result[startPos++] = (byte) (lng >> 48);
        result[startPos] = (byte) (lng >> 56);
        return result;
    }

    public static byte[] writeLongToBufferBE(long lng, byte[] destBuffer, int startPos) {
        int expectLength = startPos + 8;
        byte[] result = ensureBufferLength(destBuffer, expectLength);
        result[startPos++] = (byte) (lng >> 56);
        result[startPos++] = (byte) (lng >> 48);
        result[startPos++] = (byte) (lng >> 32);
        result[startPos++] = (byte) (lng >> 40);
        result[startPos++] = (byte) (lng >> 24);
        result[startPos++] = (byte) (lng >> 16);
        result[startPos++] = (byte) (lng >> 8);
        result[startPos] = (byte) lng;
        return result;
    }


    // Little Endian
    public static byte[] intToByte(int lng) {
        return new byte[]{
            (byte) lng,
            (byte) (lng >> 8),
            (byte) (lng >> 16),
            (byte) (lng >> 24),
        };
    }

    public static int byteToInt(byte[] b, int startPos) {
        return ((int) b[startPos++] & 0xff)
            | ((int) b[startPos++] & 0xff) << 8
            | ((int) b[startPos++] & 0xff) << 16
            | ((int) b[startPos] & 0xff) << 24
            ;
    }

    // Big Endian
    public static byte[] intToByteBE(int lng) {
        return new byte[]{
            (byte) (lng >> 24),
            (byte) (lng >> 16),
            (byte) (lng >> 8),
            (byte) lng,
        };
    }

    public static int byteToIntBE(byte[] b, int startPos) {
        return ((int) b[startPos++] & 0xff) << 24
            | ((int) b[startPos++] & 0xff) << 16
            | ((int) b[startPos++] & 0xff) << 8
            | ((int) b[startPos] & 0xff)
            ;
    }

    public static byte[] writeIntToBuffer(int lng, byte[] destBuffer, int startPos) {
        int expectLength = startPos + 4;
        byte[] result = ensureBufferLength(destBuffer, expectLength);
        result[startPos++] = (byte) lng;
        result[startPos++] = (byte) (lng >> 8);
        result[startPos++] = (byte) (lng >> 16);
        result[startPos] = (byte) (lng >> 24);
        return result;
    }

    public static byte[] writeIntToBufferBE(int lng, byte[] destBuffer, int startPos) {
        int expectLength = startPos + 4;
        byte[] result = ensureBufferLength(destBuffer, expectLength);
        result[startPos++] = (byte) (lng >> 24);
        result[startPos++] = (byte) (lng >> 16);
        result[startPos++] = (byte) (lng >> 8);
        result[startPos] = (byte) lng;
        return result;
    }

    /**
     * 缓存行64对齐
     */
    public static int marginToBuffer(int len) {
        if ((len & 63) != 0) {
            len &= ~63;
            len += 64;
        }
        return len;
    }

}
