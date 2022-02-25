package com.zmh.fastlog.utils;

import lombok.Getter;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;

public class JsonCharBuilder {
    private final static char[] escapeTab = new char[256];

    static {
        escapeTab[0] = '0'; // 空字符(NULL) 000
        escapeTab[7] = 'a'; // 响铃(BEL) 007
        escapeTab[8] = 'b'; // 退格(BS) 008
        escapeTab[9] = 't'; // 水平制表(HT) 009
        escapeTab[10] = 'n'; // 换行(LF) 010
//        escapeTab[11] = 'v'; // 垂直制表(VT) 011
        escapeTab[12] = 'f'; // 换页(FF) 012
        escapeTab[13] = 'r'; // 回车(CR) 013
        escapeTab[34] = '"'; // 双引号字符 034
        escapeTab[92] = '\\'; // 反斜杠 092
    }

    @Getter
    private CharBuffer buffer;
    private char[] bufferArray;
    private int pos;
    private int utf8CharCount = 0;

    private boolean[] firstKey = new boolean[10];
    private boolean[] firstValue = new boolean[10];
    private boolean[] isObject = new boolean[10];
    private int firstElementPos = -1;

    public int capacity() {
        return bufferArray.length;
    }

    private void markFirstKey() {
        if (isObject() && !isFirstKey()) {
            bufferArray[pos++] = ',';
        }
        firstKey[firstElementPos] = false;
    }

    private void markFirstValue() {
        if (isArray() && !isFirstValue()) {
            comma();
        }
        firstValue[firstElementPos] = false;
    }

    private boolean isFirstKey() {
        return firstKey[firstElementPos];
    }

    private boolean isFirstValue() {
        return firstValue[firstElementPos];
    }

    private boolean isObject() {
        return isObject[firstElementPos];
    }

    private boolean isArray() {
        return !isObject();
    }

    public static JsonCharBuilder create() {
        return new JsonCharBuilder();
    }

    public static JsonCharBuilder create(int bufferLen) {
        return new JsonCharBuilder(bufferLen);
    }


    public JsonCharBuilder clear() {
        this.firstElementPos = -1;
        this.utf8CharCount = 0;
        if (isNull(this.buffer)) {
            this.bufferArray = new char[64];
            this.buffer = CharBuffer.wrap(bufferArray);
        }
        this.pos = 0;
        return this;
    }

    private JsonCharBuilder() {
        clear();
    }

    private JsonCharBuilder(int bufferLen) {
        bufferLen = marginToBuffer(bufferLen);
        this.bufferArray = new char[bufferLen];
        this.buffer = CharBuffer.wrap(this.bufferArray);
        this.pos = 0;
        clear();
    }


    private void ensureCapacity(int length) {
        int additionLength = pos + length;
        if (bufferArray.length < additionLength) {
            additionLength = marginToBuffer(additionLength);
            bufferArray = new char[additionLength];
            arraycopy(buffer.array(), 0, bufferArray, 0, buffer.capacity());
            buffer = CharBuffer.wrap(bufferArray);
        }
    }

    private void addAscii(char ascii) {
        char[] arr = this.bufferArray;
        if (pos >= arr.length) {
            ensureCapacity(1);
            arr = this.bufferArray;
        }
        arr[pos++] = ascii;
    }


    private JsonCharBuilder addAscii(char ascii, char ascii2) {
        char[] arr = this.bufferArray;
        int pos = this.pos;
        if (pos + 2 > arr.length) {
            ensureCapacity(2);
            arr = this.bufferArray;
        }
        arr[pos++] = ascii;
        arr[pos++] = ascii2;
        this.pos = pos;
        return this;
    }


    private final static char[] HEX_STRING = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private void writeString(String str) {
        int length = str.length();
        int len2 = length << 1;
        int pos = this.pos;
        char[] arr = this.bufferArray;
        int utf8 = 0;

        if (arr.length - pos < len2) {
            ensureCapacity(len2);
            arr = this.bufferArray;
        }

        int src = arr.length - length;
        str.getChars(0, length, arr, src);
        int srcPos = src;
        for (int i = 0; i < length; i++) {
            char c = arr[srcPos++];
            if (0 == (c & 0xFF80)) { // ascii, 0-7F,
                char escape = escapeTab[c];
                if (escape != 0) {
                    arr[pos++] = '\\';
                    arr[pos++] = escape;
                } else if (c < 32) { // control char
                    arr[pos++] = '\\';
                    arr[pos++] = 'u';
                    arr[pos++] = '0';
                    arr[pos++] = '0';
                    char hi = (char) (c >> 4);
                    char low = (char) (c & 0x0f);
                    arr[pos++] = HEX_STRING[hi];
                    arr[pos++] = HEX_STRING[low];
                } else {
                    arr[pos++] = c;
                }
            } else {
                utf8++;
                arr[pos++] = c;
            }
        }
        utf8CharCount += utf8;
        this.pos = pos;
    }

    public JsonCharBuilder beginObject() {
        if (firstElementPos >= 0 && isArray()) {
            if (isFirstKey()) {
                firstKey[firstElementPos] = false;
                firstValue[firstElementPos] = false;
            } else {
                comma();
            }
        }
        firstKey[++firstElementPos] = true;
        firstValue[firstElementPos] = true;
        isObject[firstElementPos] = true;
        addAscii('{');
        return this;
    }

    public JsonCharBuilder endObject() {
        firstElementPos--;
        addAscii('}');
        return this;
    }

    public JsonCharBuilder beginArray() {
        firstKey[++firstElementPos] = true;
        firstValue[firstElementPos] = true;
        isObject[firstElementPos] = false;
        addAscii('[');
        return this;
    }

    public JsonCharBuilder endArray() {
        firstElementPos--;
        addAscii(']');
        return this;
    }

    private void comma() {
        addAscii(',');
    }

    public JsonCharBuilder key(@NonNull String key) {
        int length = key.length();
        int len2 = (length << 1) + 4;
        char[] arr = this.bufferArray;
        if (arr.length - this.pos < len2) {
            ensureCapacity(len2);
            arr = this.bufferArray;
        }
        markFirstKey();
        arr[this.pos++] = '"';
        writeString(key);
        arr[this.pos++] = '"';
        arr[this.pos++] = ':';
        return this;
    }

    public JsonCharBuilder value(String value) {
        markFirstValue();
        if (isNull(value)) {
            writeString("null");
        } else {
            addAscii('"');
            writeString(value);
            addAscii('"');
        }
        return this;
    }

    public JsonCharBuilder value(long value) {
        markFirstValue();
        char[] arr = this.bufferArray;
        int pos = this.pos;
        if (pos + 20 > arr.length) {
            ensureCapacity(20);
            arr = this.bufferArray;
        }
        if (0 == value) {
            arr[this.pos++] = '0';
            return this;
        }
        if (value < 0) {
            arr[pos++] = '-';
            value = -value;
        }
        int begin = pos;
        while (value > 0) {
            int n = (int) (value % 10);
            value /= 10;
            arr[pos++] = (char) (n | 0x30);
        }
        this.pos = pos;
        int end = pos - 1;
        while (begin < end) {
            char t = arr[begin];
            arr[begin] = arr[end];
            arr[end] = t;
            begin++;
            end--;
        }
        return this;
    }

    public JsonCharBuilder object(String key, String value) {
        return beginObject().key(key).value(value).endObject();
    }

    public JsonCharBuilder object(String key, long value) {
        return beginObject().key(key).value(value).endObject();
    }

    public JsonCharBuilder kv(String key, String v) {
        return key(key).value(v);
    }

    public JsonCharBuilder kv(String key, long v) {
        return key(key).value(v);
    }

    public String toString() {
        return new String(bufferArray, 0, pos);
    }

    private CharsetEncoder encoder = UTF_8.newEncoder();
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    public ByteBuffer toByteBuffer(ByteBuffer bb) {
        if (isNull(bb)) {
            bb = EMPTY_BYTE_BUFFER;
        }
        ByteBuffer result = bb;
        int estaLen = pos + (utf8CharCount << 1);
        if (bb.remaining() < estaLen) {
            int len = bb.position() + estaLen;
            len = marginToBuffer(len);
            byte[] arr = new byte[len];
            System.arraycopy(bb.array(), 0, arr, 0, bb.capacity());
            result = ByteBuffer.wrap(arr);
            result.position(bb.position());
        }
        int limit = result.limit();
        buffer.position(0)
            .limit(pos);
        encoder.reset().encode(buffer, result, true);
        return result;
    }

}
