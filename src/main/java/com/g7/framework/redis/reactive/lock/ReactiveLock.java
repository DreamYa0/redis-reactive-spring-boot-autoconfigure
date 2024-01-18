package com.g7.framework.redis.reactive.lock;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

/**
 * The Reactive lock.
 * @author dreamyao
 * @date 11 /26/21.
 */
public interface ReactiveLock {

    /**
     * 尝试获取锁一次
     * @param <T>      类型
     * @param function 执行的操作
     * @return executable Mono
     */
    <T> Mono<T> tryLock(@NotNull Function<Boolean, Mono<T>> function);

    /**
     * 尝试获取锁一次
     * @param <T>      类型
     * @param function 执行的操作
     * @return executable Flux
     */
    <T> Flux<T> tryLockMany(@NotNull Function<Boolean, Flux<T>> function);

    /**
     * 尝试在给定的持续时间内获取锁。
     * @param <T>      类型
     * @param duration 超时时间
     * @param function 执行的操作
     * @return executable Mono
     */
    <T> Mono<T> lock(@NotNull Duration duration, @NotNull Function<Boolean, Mono<T>> function);

    /**
     * 尝试在给定的持续时间内获取锁。
     * @param <T>      类型
     * @param duration 超时时间
     * @param function 执行的操作
     * @return executable Flux
     */
    <T> Flux<T> lockMany(@NotNull Duration duration, @NotNull Function<Boolean, Flux<T>> function);

}
