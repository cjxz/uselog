package com.zmh.fastlog.utils;

import lombok.AllArgsConstructor;

import javax.validation.constraints.NotNull;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;


public class InstanceUtils {

    //region OrElseReturn
    @NotNull
    @SuppressWarnings("unchecked")
    public static <O, T, R> OrElseReturn<O, R> ifInstanceOf(O a, Class<T> klass, Function<T, R> function) {
        if (isInstanceOf(a, klass)) {
            R result = function.apply((T) a);
            return new OrElseReturn<>(true, a, result);
        }
        return new OrElseReturn<>(false, a, null);
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static <O, T> OrElse<O> ifInstanceOf(O a, Class<T> klass, Consumer<T> trueConsumer) {
        if (isInstanceOf(a, klass)) {
            trueConsumer.accept((T) a);
            return new OrElse<>(true, a);
        }
        return new OrElse<>(false, a);
    }

    public static <O, T> OrElseReturn<O, T> convertIfInstanceOf(O a, Class<T> klass) {
        return ifInstanceOf(a, klass, (Function<T, T>) FunctionUtils::identity);
    }

    public static <O> boolean isInstanceOf(O a, Class klass) {
        return a != null && klass.isInstance(a);
    }


    public static <T> OrElse<T> invokeIfNotNull(T a, Consumer<T> consumer) {
        if (Objects.nonNull(a)) {
            consumer.accept(a);
            return new OrElse<>(true, a);
        }
        return new OrElse<>(false, a);
    }

    public static <T, R> OrElseReturn<T, R> getIfNotNull(T a, Function<T, R> function) {
        if (Objects.nonNull(a)) {
            R result = function.apply(a);
            return new OrElseReturn<>(true, a, result);
        }
        return new OrElseReturn<>(false, null, null);
    }

    public static <O> InstanceWrapper<O> wrap(O o) {
        return new InstanceWrapper<>(o);
    }

    @AllArgsConstructor
    public static class InstanceWrapper<O> {
        private final O o;

        public <T, R> OrElseReturn<O, R> ifInstanceOf(Class<T> klass, Function<T, R> function) {
            return InstanceUtils.ifInstanceOf(o, klass, function);
        }

        public <T> OrElse<O> ifInstanceOf(Class<T> klass, Consumer<T> trueConsumer) {
            return InstanceUtils.ifInstanceOf(o, klass, trueConsumer);
        }

        public <T> OrElseReturn<O, T> convertIfInstanceOf(Class<T> klass) {
            return InstanceUtils.convertIfInstanceOf(o, klass);
        }
    }

    public static class OrElse<T> {
        private final boolean condition;
        private final T obj;

        OrElse(boolean condition, T obj) {
            this.condition = condition;
            this.obj = obj;
        }

        public OrElse<T> orElse(Consumer<T> consumer) {
            if (!condition) {
                consumer.accept(obj);
            }
            return this;
        }

        public OrElse<T> orElseNotNull(Consumer<T> consumer) {
            if (Objects.nonNull(this.obj)) {
                orElse(consumer);
            }
            return this;
        }

        public <T2> OrElse<T> orElseIfInstanceOf(Class<T2> klass, Consumer<T2> consumer) {
            if (!condition) {
                return InstanceUtils.ifInstanceOf(this.obj, klass, consumer);
            }
            return this;
        }
    }

    public static class OrElseReturn<T, R> {
        private final boolean condition;
        private final T obj;
        private R result;

        OrElseReturn(boolean condition, T obj, R result) {
            this.condition = condition;
            this.obj = obj;
            this.result = result;
        }

        public OrElseReturn<T, R> orElse(R value) {
            if (!condition) {
                result = value;
            }
            return this;
        }

        public OrElseReturn<T, R> orElse(Supplier<R> supplier) {
            if (!condition) {
                result = supplier.get();
            }
            return this;
        }

        public OrElseReturn<T, R> orElse(Function<T, R> function) {
            if (!condition) {
                result = function.apply(obj);
            }
            return this;
        }

        public OrElseReturn<T, R> orElseNotNull(Function<T, R> function) {
            if (Objects.nonNull(this.obj)) {
                return orElse(function);
            }
            return this;
        }

        public OrElseReturn<T, R> orElseNull(Supplier<R> supplier) {
            if (isNull(this.obj)) {
                return orElse(supplier);
            }
            return this;
        }

        public <T2> OrElseReturn<T, R> orElseIfInstanceOf(Class<T2> klass, Function<T2, R> function) {
            if (!condition) {
                return InstanceUtils.ifInstanceOf(this.obj, klass, function)
                    .orElse(() -> result);
            }
            return this;
        }

        public R value() {
            return result;
        }

        public R valueOrDefault(R defaultValue) {
            return defaultIfNull(value(), defaultValue);
        }
    }
    //endregion

    public static <T, R> R safeGet(T obj, Function<T, R> fun) {
        if (isNull(obj) || isNull(fun)) {
            return null;
        }
        return fun.apply(obj);
    }

    public static <T, R1, R2> R2 safeGet(T obj, Function<T, R1> fun, Function<R1, R2> fun2) {
        return safeGet(safeGet(obj, fun), fun2);
    }


    // safeClose
    public static <T extends AutoCloseable> T safeClose(T object) {
        if (Objects.nonNull(object)) {
            try {
                object.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return object;
    }

}
