package com.g7.framework.redis.reactive.lock;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * 抽象 reactive lock.
 * @author dreamyao
 * @date 2022/3/1 4:09 下午
 */
public abstract class AbstractReactiveLock implements StatefulReactiveLock {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReactiveLock.class);

    @Override
    public <T> Mono<T> tryLock(@NotNull Function<Boolean, Mono<T>> function) {
        return executeMono(null, function);
    }

    @Override
    public <T> Flux<T> tryLockMany(@NotNull Function<Boolean, Flux<T>> function) {
        return executeFlux(null, function);
    }

    @Override
    public <T> Mono<T> lock(@NotNull Duration duration,
                            @NotNull Function<Boolean, Mono<T>> function) {
        return executeMono(duration, function);
    }

    @Override
    public <T> Flux<T> lockMany(@NotNull Duration duration,
                                @NotNull Function<Boolean, Flux<T>> function) {
        return executeFlux(duration, function);
    }

    protected abstract ReactiveLockExecutor getReactiveLockExecutor();

    /**
     * execute with flux
     * @param <T>            类型
     * @param lockExpireTime 锁过期时间
     * @param function       执行操作
     * @return flux
     */
    private  <T> Flux<T> executeFlux(@Nullable Duration lockExpireTime,
                                     Function<Boolean, Flux<T>> function) {

        if (Objects.isNull(lockExpireTime) || lockExpireTime.isNegative()) {
            return Flux.usingWhen(
                    Mono.just(getReactiveLockExecutor()),
                    reactiveLockExecutor -> reactiveLockExecutor.obtain()
                            .filter(result -> result)
                            .defaultIfEmpty(false)
                            .doOnNext(lockResult ->
                                    logger.info("flux execution try lock once,lock result:{}", lockResult))
                            .flatMapMany(function),
                    reactiveLockExecutor -> reactiveLockExecutor.release()
                            .doOnNext(releaseResult ->
                                    logger.info("flux execution(normal release) release result:{}", releaseResult)),
                    (reactiveLockExecutor, err) -> reactiveLockExecutor.release()
                            .doOnNext(releaseResult ->
                                    logger.info("flux execution(when error release) release result:{}",
                                            releaseResult)),
                    reactiveLockExecutor -> reactiveLockExecutor.release()
                            .doOnNext(releaseResult ->
                                    logger.info("flux execution(when async cancel release),release result:{}",
                                            releaseResult))
            );
        }

        return Flux.usingWhen(
                Mono.just(getReactiveLockExecutor()),
                reactiveLockExecutor -> reactiveLockExecutor.obtain()
                        .filter(result -> result)
                        .repeatWhenEmpty(Repeat.<T>onlyIf(repeatContext -> true)
                                .timeout(lockExpireTime)
                                .fixedBackoff(Duration.ofMillis(100))
                                .doOnRepeat(objectRepeatContext -> {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("flux execution repeat acquire lock repeat content:{}",
                                                objectRepeatContext);
                                    }
                                })
                        )
                        .defaultIfEmpty(false)
                        .doOnNext(lockResult -> logger.info("[flux execution] try lock,lock result:{}",
                                lockResult))
                        .flatMapMany(function),
                reactiveLockExecutor -> reactiveLockExecutor.release()
                        .doOnNext(releaseResult ->
                                logger.info("flux execution(normal release) release result:{}", releaseResult)),
                (reactiveLockExecutor, err) -> reactiveLockExecutor.release()
                        .doOnNext(releaseResult ->
                                logger.info("flux execution(when error release) release result:{}", releaseResult)),
                reactiveLockExecutor -> reactiveLockExecutor.release()
                        .doOnNext(releaseResult ->
                                logger.info("flux execution(when async cancel release),release result:{}",
                                        releaseResult))
        );
    }

    /**
     * execute with mono
     * @param <T>            类型
     * @param lockExpireTime 锁过期时间
     * @param function       执行操作
     * @return mono
     */
    private  <T> Mono<T> executeMono(@Nullable Duration lockExpireTime,
                                     Function<Boolean, Mono<T>> function) {

        if (Objects.isNull(lockExpireTime) || lockExpireTime.isNegative()) {
            return Mono.usingWhen(
                    Mono.just(getReactiveLockExecutor()),
                    reactiveLockExecutor -> reactiveLockExecutor.obtain()
                            .filter(result -> result)
                            .defaultIfEmpty(false)
                            .doOnNext(lockResult ->
                                    logger.info("mono execution try lock once,lock result:{}", lockResult))
                            .flatMap(function),
                    reactiveLockExecutor -> reactiveLockExecutor.release()
                            .doOnNext(releaseResult ->
                                    logger.info("mono execution(normal release) release result:{}", releaseResult)),
                    (reactiveLockExecutor, err) -> reactiveLockExecutor.release()
                            .doOnNext(releaseResult ->
                                    logger.info("mono execution(when error release)release result:{}",
                                            releaseResult)),
                    reactiveLockExecutor -> reactiveLockExecutor.release()
                            .doOnNext(releaseResult ->
                                    logger.info("mono execution(when async cancel release),release result:{}",
                                            releaseResult))
            );
        }

        return Mono.usingWhen(
                Mono.just(getReactiveLockExecutor()),
                reactiveLockExecutor -> reactiveLockExecutor.obtain()
                        .filter(result -> result)
                        .repeatWhenEmpty(Repeat.<T>onlyIf(repeatContext -> true)
                                .timeout(lockExpireTime)
                                .fixedBackoff(Duration.ofMillis(100))
                                .doOnRepeat(objectRepeatContext -> {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("mono execution repeat acquire lock repeat content:{}",
                                                objectRepeatContext);
                                    }
                                })
                        )
                        .defaultIfEmpty(false)
                        .doOnNext(lockResult -> logger.info("[mono execution try lock,lock result:{}",
                                lockResult))
                        .flatMap(function),
                reactiveLockExecutor -> reactiveLockExecutor.release()
                        .doOnNext(releaseResult ->
                                logger.info("mono execution(normal release) release result:{}", releaseResult)),
                (reactiveLockExecutor, err) -> reactiveLockExecutor.release()
                        .doOnNext(releaseResult ->
                                logger.info("mono execution(when error release)release result:{}", releaseResult)),
                reactiveLockExecutor -> reactiveLockExecutor.release()
                        .doOnNext(releaseResult ->
                                logger.info("mono execution(when async cancel release),release result:{}",
                                        releaseResult))
        );
    }
}
