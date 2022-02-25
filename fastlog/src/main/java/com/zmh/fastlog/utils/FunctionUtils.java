package com.zmh.fastlog.utils;


public class FunctionUtils {
    public static <T> void Nop(T arg) {

    }

    public static <T, R> R returnNull(T arg) {
        return null;
    }

    public static <T> T identity(T arg) {
        return arg;
    }
}
