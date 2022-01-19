package com.zmh.fastlog.utils;

import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonByteBuilderTest { //todo zmh 加一个ensure的单元测试

    @Test
    @SneakyThrows
    public void test() {
        JsonByteBuilder json = JsonByteBuilder.create()
            .beginObject()
            .key("hi\"wo").value(1234567890)
            .key("key2").value("value\2")
            .key("key3").value("value3")
            .endObject();

        String s = json.toString();
        assertEquals("{\"hi\\\"wo\":1234567890,\"key2\":\"value\\u0002\",\"key3\":\"value3\"}", s);
    }

    @Test
    public void testNullObject() {
        JsonByteBuilder builder = JsonByteBuilder.create()
            .beginObject()
            .key("hi").value(null)
            .endObject();
        String s = builder.toString();
        assertEquals("{\"hi\":null}", s);

        builder.clear()
            .beginObject()
            .key("hi").value(null)
            .endObject();
        s = builder.toString();
        assertEquals("{\"hi\":null}", s);

        builder.clear()
            .beginObject()
            .key("hi").value(null)
            .key("ok").value(null)
            .endObject();
        s = builder.toString();
        assertEquals("{\"hi\":null,\"ok\":null}", s);
    }

    @Test
    public void testCJK() {
        String json = JsonByteBuilder.create()
            .beginObject()
            .key("the中文key").value("enn.中文..value")
            .endObject()
            .toString();
        assertEquals("{\"the中文key\":\"enn.中文..value\"}", json);
    }

    @Test
    public void testCut() {
        String json = JsonByteBuilder.create()
            .beginObject()
            .key("key").value("valuevalue中文", 11)
            .endObject()
            .toString();
        assertEquals("{\"key\":\"valuevalue中\"}", json);
    }
}
