package com.g7.framework.redis.reactive.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 抽象自动清理反应锁注册表
 * @author dream yao
 * @date 2022/3/1 4:09 下午
 */
public abstract class AbstractAutoCleanupReactiveLockRegistry implements ReactiveLockRegistry, InitializingBean,
        DisposableBean {

    protected static final Duration DEFAULT_EXPIRE_EVICT_IDLE = Duration.ofMinutes(3);
    protected static final Duration DEFAULT_MAX_LOCK_LIFETIME = Duration.ofMinutes(10);
    private static final Logger logger = LoggerFactory.getLogger(AbstractAutoCleanupReactiveLockRegistry.class);
    private final Scheduler scheduler = Schedulers.newSingle("redis-lock-evict", true);
    private final ConcurrentMap<String, StatefulReactiveLock> lockRegistry = new ConcurrentHashMap<>(16);
    private final Duration expireEvictIdle;
    private final Duration maxLockLifeTime;

    /**
     * 实例化一个新的抽象自动清理反应锁注册表
     * @param expireEvictIdle 过期驱逐空闲
     * @param maxLockLifeTime 最大锁定寿命
     */
    public AbstractAutoCleanupReactiveLockRegistry(Duration expireEvictIdle, Duration maxLockLifeTime) {
        this.expireEvictIdle = expireEvictIdle;
        this.maxLockLifeTime = maxLockLifeTime;
    }

    /**
     * new reactive lock
     * @param lockKey the lock key
     * @return stateful reactive lock
     */
    protected abstract StatefulReactiveLock newReactiveLock(String lockKey);

    @Override
    public ReactiveLock obtain(String lockKey) {
        ReactiveLock reactiveLock = lockRegistry.get(lockKey);
        if (reactiveLock != null) {
            return reactiveLock;
        }
        return lockRegistry.computeIfAbsent(lockKey, this::newReactiveLock);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("initialize auto remove unused lock execution");
        }
        Flux.interval(expireEvictIdle, scheduler)
                .flatMap(value -> {
                    long now = System.currentTimeMillis();
                    if (logger.isTraceEnabled()) {
                        logger.trace("Auto remove unused lock ,evict triggered");
                    }
                    return Flux.fromIterable(this.lockRegistry.entrySet())
                            .filter(entry -> now - entry.getValue().latestLockTime() > maxLockLifeTime.toMillis())
                            .flatMap(entry -> entry.getValue()
                                    .processing()
                                    .filter(processing -> !processing)
                                    .doOnNext(processing -> {
                                        this.lockRegistry.remove(entry.getKey());
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("auto remove unused lock,lock info:{}", entry);
                                        }
                                    })
                                    .onErrorResume(throwable -> {
                                        logger.error("auto remove unused locks occur exception,lock info: " + entry,
                                                throwable);
                                        return Mono.empty();
                                    })
                            );
                })
                .subscribe();
    }

    @Override
    public void destroy() throws Exception {
        if (!this.scheduler.isDisposed()) {
            if (logger.isDebugEnabled()) {
                logger.debug("shutdown auto remove unused lock execution");
            }
            this.scheduler.dispose();
        }
    }
}
