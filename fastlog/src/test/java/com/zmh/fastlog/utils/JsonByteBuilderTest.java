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
            .key("key3").beginArray().value(-123).value("456").value(789).endArray()
            .key("key4").beginArray().endArray()
            .key("key5").value("value5")
            .endObject();

        String s = json.toString();
        assertEquals("{\"hi\\\"wo\":1234567890,\"key2\":\"value\\u0002\",\"key3\":[-123,\"456\",789],\"key4\":[],\"key5\":\"value5\"}", s);
    }

    @Test
    public void objInObj() {
        JsonByteBuilder json = JsonByteBuilder.create()
            .beginObject()
            .key("k1").beginObject()
            /**/.kv("k11", "v11")
            /**/.kv("k12", "v12")
            .endObject()
            .key("k2").beginObject()
            /**/.kv("k21", "v21")
            /**/.kv("k22", 22)
            .endObject()
            .key("k3").beginArray()
            /*0*/.object("k31", "v32")
            /*1*/.object("k32", "v32")
            /*2*/.object("k33", 33)
            /*4*/.value("ooo")
            /*5*/.value(123)
            .endArray();

        String s = json.toString();
        assertEquals("{\"k1\":{\"k11\":\"v11\",\"k12\":\"v12\"},\"k2\":{\"k21\":\"v21\",\"k22\":22},\"k3\":[{\"k31\":\"v32\"},{\"k32\":\"v32\"},{\"k33\":33},\"ooo\",123]", s);
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
