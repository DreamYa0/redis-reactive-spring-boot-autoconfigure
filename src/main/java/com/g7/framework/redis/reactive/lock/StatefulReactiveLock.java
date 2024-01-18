package com.g7.framework.redis.reactive.lock;

import reactor.core.publisher.Mono;

/**
 * 有状态的 reactive lock.
 * @author dreamyao
 * @date 2022/3/1 4:09 下午
 */
public interface StatefulReactiveLock extends ReactiveLock {

    /**
     * 最新锁定时间
     * @return long
     */
    long latestLockTime();

    /**
     * 是否处理中
     * @return mono
     */
    Mono<Boolean> processing();
}
