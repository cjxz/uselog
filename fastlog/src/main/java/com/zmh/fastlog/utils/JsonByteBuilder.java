package com.zmh.fastlog.utils;

import lombok.NonNull;

import java.nio.ByteBuffer;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.util.Objects.isNull;

public class JsonByteBuilder {

    private final static byte[] escapeTab = new byte[128];

    static {
        for (int i = 1; i < 31; i++) {
            escapeTab[i] = 1;
        }
        escapeTab[0] = 2; // 空字符(NULL) 000
        escapeTab[7] = 2; // a 响铃(BEL) 007
        escapeTab[8] = 2; // b 退格(BS) 008
        escapeTab[9] = 2; // t 水平制表(HT) 009
        escapeTab[10] = 2; // n 换行(LF) 010
        escapeTab[12] = 2; // f 换页(FF) 012
        escapeTab[13] = 2; // r 回车(CR) 013
        escapeTab[34] = 2; // " 双引号字符 034
        escapeTab[92] = 2; // \ 反斜杠 092
    }


    private byte[] bufferArray;
    private int pos;

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

    public static JsonByteBuilder create() {
        return new JsonByteBuilder();
    }

    public static JsonByteBuilder create(int bufferLen) {
        return new JsonByteBuilder(bufferLen);
    }


    public JsonByteBuilder clear() {
        this.firstElementPos = -1;
        this.bufferArray = new byte[256];

        this.pos = 0;
        return this;
    }

    private JsonByteBuilder() {
        clear();
    }

    private JsonByteBuilder(int bufferLen) {
        bufferLen = marginToBuffer(bufferLen);
        this.bufferArray = new byte[bufferLen];
        this.pos = 0;
        clear();
    }

    private void ensureCapacity(int length) {
        int additionLength = pos + length;
        if (bufferArray.length < additionLength) {
            byte[] src = bufferArray;

            additionLength += Math.min(additionLength, pos);
            additionLength = marginToBuffer(additionLength);
            bufferArray = new byte[additionLength];

            System.arraycopy(src, 0, bufferArray, 0, pos);
        }
    }

    private void addAscii(byte c) {
        if (this.bufferArray.length - this.pos < 1) {
            ensureCapacity(1);
        }
        this.bufferArray[pos++] = c;
    }

    private final static byte[] HEX_BYTE = {48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70};

    private void writeString(String str) {
        int sLength = str.length();

        byte[] arr = this.bufferArray;
        int pos = this.pos;

        int len = sLength << 2;
        if (arr.length - pos < len) {
            ensureCapacity(len);
            arr = this.bufferArray;
        }

        for (int sIndex = 0; sIndex < sLength; sIndex++) {
            char c = str.charAt(sIndex);

            if (c < '\u0080') {
                byte b = (byte) c;

                if (escapeTab[b] == 0) {
                    arr[pos++] = b;
                } else if (escapeTab[b] == 2) {
                    arr[pos++] = 92; // 92 = \
                    arr[pos++] = b;
                } else {
                    arr[pos++] = 92; // 92 = \
                    arr[pos++] = 117; // 117 = u
                    arr[pos++] = 48; // 48 = 0
                    arr[pos++] = 48; // 48 = 0
                    byte hi = (byte) (b >> 4);
                    byte low = (byte) (b & 0x0f);
                    arr[pos++] = HEX_BYTE[hi];
                    arr[pos++] = HEX_BYTE[low];
                }
            } else if (c < '\u0800') {
                arr[pos++] = (byte) (192 | c >>> 6);
                arr[pos++] = (byte) (128 | c & 63);
            } else if (c < '\ud800' || c > '\udfff') {
                arr[pos++] = (byte) (224 | c >>> 12);
                arr[pos++] = (byte) (128 | c >>> 6 & 63);
                arr[pos++] = (byte) (128 | c & 63);
            } else {
                int cp = 0;
                if (++sIndex < sLength) cp = Character.toCodePoint(c, str.charAt(sIndex));
                if ((cp >= 1 << 16) && (cp < 1 << 21)) {
                    arr[pos++] = (byte) (240 | cp >>> 18);
                    arr[pos++] = (byte) (128 | cp >>> 12 & 63);
                    arr[pos++] = (byte) (128 | cp >>> 6 & 63);
                    arr[pos++] = (byte) (128 | cp & 63);
                } else
                    arr[pos++] = (byte) '?';
            }
        }

        this.pos = pos;
    }

    public JsonByteBuilder beginObject() {
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
        addAscii((byte) '{');
        return this;
    }

    public JsonByteBuilder endObject() {
        firstElementPos--;
        addAscii((byte) '}');
        return this;
    }

    public JsonByteBuilder beginArray() {
        firstKey[++firstElementPos] = true;
        firstValue[firstElementPos] = true;
        isObject[firstElementPos] = false;
        addAscii((byte) '[');
        return this;
    }

    public JsonByteBuilder endArray() {
        firstElementPos--;
        addAscii((byte) ']');
        return this;
    }

    private void comma() {
        addAscii((byte) ',');
    }

    public JsonByteBuilder key(@NonNull String key) {
        int len = key.length() << 2;
        if (this.bufferArray.length - this.pos < len) {
            ensureCapacity(len);
        }
        markFirstKey();
        addAscii((byte) '"');
        writeString(key);
        addAscii((byte) '"');
        addAscii((byte) ':');
        return this;
    }

    public JsonByteBuilder value(String value) {
        markFirstValue();
        if (isNull(value)) {
            writeString("null");
        } else {
            addAscii((byte) '"');
            writeString(value);
            addAscii((byte) '"');
        }
        return this;
    }

    public JsonByteBuilder value(long value) {
        markFirstValue();
        byte[] arr = this.bufferArray;
        int pos = this.pos;
        if (pos + 20 > arr.length) {
            ensureCapacity(20);
            arr = this.bufferArray;
        }
        if (0 == value) {
            arr[pos++] = 48; // 48 = 0
            this.pos = pos;
            return this;
        }
        if (value < 0) {
            arr[pos++] = 45; // 45 = -
            value = -value;
        }
        int begin = pos;
        while (value > 0) {
            int n = (int) (value % 10);
            value /= 10;
            arr[pos++] = (byte) (n | 0x30);
        }
        this.pos = pos;
        int end = pos - 1;
        while (begin < end) {
            byte t = arr[begin];
            arr[begin] = arr[end];
            arr[end] = t;
            begin++;
            end--;
        }
        return this;
    }

    public JsonByteBuilder object(String key, String value) {
        return beginObject().key(key).value(value).endObject();
    }

    public JsonByteBuilder object(String key, long value) {
        return beginObject().key(key).value(value).endObject();
    }

    public JsonByteBuilder kv(String key, String v) {
        return key(key).value(v);
    }

    public JsonByteBuilder kv(String key, long v) {
        return key(key).value(v);
    }

    public String toString() {
        return new String(bufferArray, 0, pos);
    }

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    public ByteBuffer toByteBuffer(ByteBuffer bb) {
        if (isNull(bb)) {
            bb = EMPTY_BYTE_BUFFER;
        }
        ByteBuffer result = bb;
        if (bb.remaining() < pos) {
            int len = bb.position() + pos;
            len = marginToBuffer(len);
            byte[] arr = new byte[len];
            System.arraycopy(bb.array(), 0, arr, 0, bb.capacity());
            result = ByteBuffer.wrap(arr);
            result.position(bb.position());
        }
        result.put(bufferArray, 0, pos);
        return result;
    }
}
