package com.zmh.fastlog.utils;

import lombok.Lombok;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.nonNull;
import static lombok.AccessLevel.PRIVATE;

/**
 * @author hupeng.net@hotmail.com
 */
@NoArgsConstructor(access = PRIVATE)
@Slf4j
public final class ExceptionUtil {

    public static <T> Supplier<T> sneakyCatch(RethrowSupplier<T> s) {
        return () -> {
            try {
                return s.get();
            } catch (Throwable e) {
                log.error("execute error, but sneaky catch", e);
            }
            return null;
        };
    }


    public static <T> Consumer<? super T> sneakyCatch(RethrowConsumer<? super T> s) {
        return it -> {
            try {
                s.accept(it);
            } catch (Throwable e) {
                log.error("execute error, but sneaky catch", e);
            }
        };
    }

    public static <T, R> Function<T, R> sneakyCatch(RethrowFunction<T, R> fun) {
        return it -> {
            try {
                return fun.apply(it);
            } catch (Throwable e) {
                log.error("execute error, but sneaky catch", e);
            }
            return null;
        };
    }

    public static <T, U> BiConsumer<T, U> sneakyCatch(RethrowBiConsumer<? super T, ? super U> s) {
        return (t, u) -> {
            try {
                s.accept(t, u);
            } catch (Throwable e) {
                log.error("execute error, but sneaky catch", e);
            }
        };
    }

    public static Runnable runWithCatch(RethrowRunnable fun) {
        return () -> {
            try {
                fun.run();
            } catch (Throwable e) {
                log.error("execute error, but sneaky catch", e);
            }
        };
    }

    public static <T> Supplier<T> rethrow(RethrowSupplier<T> s) {
        return () -> {
            try {
                return s.get();
            } catch (Throwable e) {
                throw Lombok.sneakyThrow(e);
            }
        };
    }


    public static <T, R> Function<T, R> rethrowFun(RethrowFunction<T, R> fun) {
        return it -> {
            try {
                return fun.apply(it);
            } catch (Throwable e) {
                throw Lombok.sneakyThrow(e);
            }
        };
    }

    public static <T> Consumer<? super T> rethrow(RethrowConsumer<? super T> s) {
        return a -> {
            try {
                s.accept(a);
            } catch (Throwable e) {
                throw Lombok.sneakyThrow(e);
            }
        };
    }

    public static <T, U> BiConsumer<? super T, ? super U> rethrow(RethrowBiConsumer<? super T, ? super U> s) {
        return (t, u) -> {
            try {
                s.accept(t, u);
            } catch (Throwable e) {
                throw Lombok.sneakyThrow(e);
            }
        };
    }

    public interface RethrowConsumer<T> {
        void accept(T t) throws Throwable;
    }

    public interface RethrowBiConsumer<T, U> {
        void accept(T t, U u) throws Throwable;
    }

    public interface RethrowSupplier<T> {
        T get() throws Throwable;
    }

    public interface RethrowFunction<T, R> {
        R apply(T t) throws Throwable;
    }

    public interface RethrowAction {
        void apply() throws Throwable;
    }

    public interface RethrowRunnable {
        void run() throws Throwable;
    }

    public static void sneakyInvoke(RethrowAction action) {
        if (nonNull(action)) {
            try {
                action.apply();
            } catch (Throwable ignore) {
            }
        }
    }
}
