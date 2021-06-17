package com.jetbrains.test.channels;

/**
 * Functional interface to facilitate consumers that can throw an error
 * @param <T> - the type of the input to the operation
 * @param <E> - the type of the exception that could be thrown
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Throwable> {
    void accept(T t) throws E;
}
