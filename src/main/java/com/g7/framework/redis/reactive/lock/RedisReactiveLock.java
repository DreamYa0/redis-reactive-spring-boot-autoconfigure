package com.g7.framework.redis.reactive.lock;

import io.lettuce.core.internal.LettuceLists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * redis锁
 * @author dreamyao
 * @date 2022/3/1 4:09 下午
 */
public class RedisReactiveLock extends AbstractReactiveLock {

    private static final Logger logger = LoggerFactory.getLogger(RedisReactiveLock.class);

    private static final Duration DEFAULT_EXPIRE_AFTER = Duration.ofSeconds(60);
    private static final String OBTAIN_LOCK_SCRIPT = "local lockSet = redis.call('SETNX', KEYS[1], ARGV[1])\n" +
            "if lockSet == 1 then\n" +
            "  redis.call('PEXPIRE', KEYS[1], ARGV[2])\n" +
            "  return true\n" +
            "else\n" +
            "  return false\n" +
            "end";
    private final RedisScript<Boolean> obtainLockScript = new DefaultRedisScript<>(OBTAIN_LOCK_SCRIPT,
            Boolean.class);
    private final ReactiveStringRedisTemplate reactiveStringRedisTemplate;
    private final ReactiveLockExecutor reactiveLockExecutor;
    private volatile boolean unlinkAvailable = true;

    public RedisReactiveLock(ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                             String lockKey,
                             Duration expireAfter) {
        Assert.notNull(reactiveStringRedisTemplate,
                "ReactiveStringRedisTemplate cannot be null");
        Assert.notNull(lockKey, "'lockKey' cannot be null");
        this.reactiveStringRedisTemplate = reactiveStringRedisTemplate;
        this.reactiveLockExecutor = new RedisReactiveLockExecutor(lockKey,
                expireAfter == null ? DEFAULT_EXPIRE_AFTER : expireAfter);
    }

    @Override
    protected ReactiveLockExecutor getReactiveLockExecutor() {
        return this.reactiveLockExecutor;
    }

    @Override
    public long latestLockTime() {
        return this.reactiveLockExecutor.lockAt();
    }

    @Override
    public Mono<Boolean> processing() {
        return this.reactiveLockExecutor.processing();
    }

    private class RedisReactiveLockExecutor implements ReactiveLockExecutor {

        private final String lockId = UUID.randomUUID().toString();
        private final String lockKey;
        private final long expireAfter;
        private final AtomicBoolean localLock = new AtomicBoolean(false);
        private volatile long lockedAt;

        public RedisReactiveLockExecutor(String lockKey, Duration expireAfter) {
            Assert.notNull(lockKey, "'lockKey' cannot be null");
            this.lockKey = lockKey;
            this.expireAfter = expireAfter.toMillis();
        }

        @Override
        public long lockAt() {
            return this.lockedAt;
        }

        @Override
        public Mono<Boolean> processing() {
            return RedisReactiveLock.this.reactiveStringRedisTemplate.opsForValue()
                    .get(this.lockKey)
                    .map(this.lockId::equals)
                    .defaultIfEmpty(false);
        }

        @Override
        public Mono<Boolean> obtain() {
            return Mono.just(this.localLock)
                    .map(lock -> lock.compareAndSet(false, true))
                    .filter(localLockResult -> localLockResult)
                    .flatMap(localLockResult -> Mono
                            .from(RedisReactiveLock.this.reactiveStringRedisTemplate.execute(
                                    RedisReactiveLock.this.obtainLockScript,
                                    Collections.singletonList(this.lockKey),
                                    LettuceLists.newList(this.lockId,
                                            String.valueOf(this.expireAfter)))
                            )
                            .map(success -> {
                                boolean result = Boolean.TRUE.equals(success);
                                if (result) {
                                    this.lockedAt = System.currentTimeMillis();
                                }
                                return result;
                            })
                            .doFinally(signal -> this.localLock.compareAndSet(true, false))

                    )
                    .switchIfEmpty(Mono.just(false));
        }

        @Override
        public Mono<Boolean> release() {
            return Mono.just(this.localLock)
                    .map(lock -> lock.compareAndSet(false, true))
                    .filter(localReleaseResult -> localReleaseResult)
                    .flatMap(localReleaseResult -> Mono.just(RedisReactiveLock.this.unlinkAvailable)
                            .filter(unlink -> unlink)
                            .flatMap(unlink -> this.processing()
                                    .filter(processing -> processing)
                                    .flatMap(processing -> RedisReactiveLock.this.reactiveStringRedisTemplate
                                            .unlink(this.lockKey)
                                            .doOnError(throwable -> {
                                                RedisReactiveLock.this.unlinkAvailable = false;
                                                if (logger.isDebugEnabled()) {
                                                    logger.debug("The UNLINK command has failed " +
                                                            "(not supported on the Redis server?); " +
                                                            "falling back to the regular DELETE command", throwable);
                                                } else {
                                                    logger.warn("The UNLINK command has failed (not " +
                                                            "supported on the Redis server?); " +
                                                            "falling back to the regular DELETE command: " +
                                                            throwable.getMessage());
                                                }
                                            })
                                            .map(unlinkResult -> unlinkResult > 0)
                                            .onErrorResume(throwable ->
                                                    RedisReactiveLock.this.reactiveStringRedisTemplate.delete(
                                                                    this.lockKey)
                                                            .map(deleteResult -> deleteResult > 0)
                                            )
                                    )
                                    .switchIfEmpty(Mono.defer(() -> {
                                        logger.warn("Lock({}) was released in the store due to expiration." +
                                                "The integrity of data protected by this lock may have been" +
                                                " compromised.", this.lockKey);
                                        return Mono.just(false);
                                    }))
                            )
                            .doFinally(signal -> this.localLock.compareAndSet(true, false))
                            .switchIfEmpty(Mono.just(false))
                    )
                    .switchIfEmpty(Mono.just(false));
        }

        @Override
        public String toString() {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd@HH:mm:ss.SSS");
            return "RedisReactiveLockExecutor [lockKey=" + this.lockKey
                    + ",lockedAt=" + dateFormat.format(new Date(this.lockedAt))
                    + ", lockId=" + this.lockId
                    + "]";
        }
    }
}
