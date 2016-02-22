package com.fitbit.util;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The purpose of this class is to provide performance efficient way to
 * increment/decrement value when used with ThreadLocal. Otherwise we have to use primitive
 * wrapper classes like Integer or Long and accrue boxing/unboxing. <br/>
 * Implementation is not thread safe.
 * Because it is normally stored in a ThreadLocal we should not worry about
 * concurrency much.
 */
@NotThreadSafe
public class Counter {

    private int counter;

    public Counter(int initialValue) {
        counter = initialValue;
    }

    public int getValue() {
        return counter;
    }

    public int incrementAndGet() {
        return ++counter;
    }

    public int decrementAndGet() {
        return --counter;
    }

    public void setValue(int value) {
        counter = value;
    }
}
