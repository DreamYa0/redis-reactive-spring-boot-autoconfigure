package com.g7.framework.redis.reactive.lock;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Duration;
import java.util.Objects;

/**
 * The Redis reactive lock registry.
 * @author dreamyao
 * @date 2022/3/1 4:09 下午
 */
public class RedisReactiveLockRegistry extends AbstractAutoCleanupReactiveLockRegistry {

    private static final String DEFAULT_KEY_PREFIX = "redis_reactive_lock";
    private final ReactiveStringRedisTemplate reactiveStringRedisTemplate;
    private final Duration maxLockLifeTime;
    private final String keyPrefix;

    public RedisReactiveLockRegistry(ReactiveStringRedisTemplate reactiveStringRedisTemplate, String keyPrefix) {
        this(reactiveStringRedisTemplate, DEFAULT_EXPIRE_EVICT_IDLE,
                DEFAULT_MAX_LOCK_LIFETIME, keyPrefix);
    }

    /**
     * Instantiates a new Redis reactive lock registry.
     * @param reactiveStringRedisTemplate redis Template
     * @param expireEvictIdle             过期驱逐空闲时间
     * @param maxLockLifeTime             最大锁定寿命
     * @param keyPrefix                   the key prefix
     */
    public RedisReactiveLockRegistry(ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                                     Duration expireEvictIdle,
                                     Duration maxLockLifeTime,
                                     String keyPrefix) {
        super(expireEvictIdle, maxLockLifeTime);
        this.reactiveStringRedisTemplate = reactiveStringRedisTemplate;
        this.maxLockLifeTime = maxLockLifeTime;
        if (Objects.nonNull(keyPrefix) && keyPrefix.length() > 0) {
            this.keyPrefix = keyPrefix;
        } else {
            this.keyPrefix = DEFAULT_KEY_PREFIX;
        }
    }

    @Override
    protected StatefulReactiveLock newReactiveLock(String lockKey) {
        return new RedisReactiveLock(reactiveStringRedisTemplate,
                keyPrefix + ':' + lockKey, maxLockLifeTime);
    }
}
