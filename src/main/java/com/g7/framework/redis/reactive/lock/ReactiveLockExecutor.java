package com.g7.framework.redis.reactive.lock;

import reactor.core.publisher.Mono;

/**
 * The Reactive lock executor.
 * @author dreamyao
 * @date 2022/3/1 4:09 下午
 */
public interface ReactiveLockExecutor {

    /**
     * 加锁的时间 毫秒
     * @return long
     */
    long lockAt();

    /**
     * 是否处理中
     * @return mono
     */
    Mono<Boolean> processing();

    /**
     * 获取锁
     * @return mono
     */
    Mono<Boolean> obtain();

    /**
     * 释放锁
     * @return mono
     */
    Mono<Boolean> release();
}
