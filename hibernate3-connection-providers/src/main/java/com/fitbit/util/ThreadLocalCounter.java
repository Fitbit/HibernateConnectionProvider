package com.fitbit.util;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The purpose of this class is to provide performance efficient way to
 * increment/decrement value when used with ThreadLocal. Otherwise we have to use primitive
 * wrapper classes like Integer or Long and accrue boxing/unboxing or we can use AtomicLong but
 * this is still unnecessary because counter is per thread.
 */
@ThreadSafe
public class ThreadLocalCounter extends ThreadLocal<Counter> {

    @Override
    protected Counter initialValue() {
        return new Counter(0);
    }

    public int getValue() {
        return get().getValue();
    }

    public int incrementAndGet() {
        return get().incrementAndGet();
    }

    public int decrementAndGet() {
        return get().decrementAndGet();
    }

    public void setValue(int value) {
        get().setValue(value);
    }
}
