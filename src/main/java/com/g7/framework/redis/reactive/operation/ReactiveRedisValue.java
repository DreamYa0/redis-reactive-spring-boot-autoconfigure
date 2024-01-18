package com.g7.framework.redis.reactive.operation;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 操作对象
 */
public class ReactiveRedisValue implements ReactiveValueOperations<String, Object> {

    private static final Logger logger = LoggerFactory.getLogger(ReactiveRedisValue.class);

    private final ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    public ReactiveRedisValue(ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;
    }

    @NotNull
    @Override
    public Mono<Boolean> set(@NotNull String key, @NotNull Object value) {
        return reactiveRedisTemplate.opsForValue().set(key, value)
                .doOnSuccess(res -> logger.info("set success key is {} value is {}", key, value));
    }

    @NotNull
    @Override
    public Mono<Boolean> set(@NotNull String key, @NotNull Object value, @NotNull Duration timeout) {
        return reactiveRedisTemplate.opsForValue().set(key, value, timeout)
                .doOnSuccess(res -> logger.info("set success key is {} value is {} " +
                        "expiration time is {} ms", key, value, timeout.toMillis()));
    }

    @NotNull
    @Override
    public Mono<Boolean> setIfAbsent(@NotNull String key, @NotNull Object value) {
        return reactiveRedisTemplate.opsForValue().setIfAbsent(key, value)
                .doOnSuccess(res -> logger.info("set if absent success key is {} value is {}",
                        key, value));
    }

    @NotNull
    @Override
    public Mono<Boolean> setIfAbsent(@NotNull String key, @NotNull Object value, @NotNull Duration timeout) {
        return reactiveRedisTemplate.opsForValue().setIfAbsent(key, value, timeout)
                .doOnSuccess(res -> logger.info("set if absent success key is {} value is {} " +
                        "expiration time is {} ms", key, value, timeout.toMillis()));
    }

    @NotNull
    @Override
    public Mono<Boolean> setIfPresent(@NotNull String key, @NotNull Object value) {
        return reactiveRedisTemplate.opsForValue().setIfPresent(key, value)
                .doOnSuccess(res -> logger.info("set if present success key is {} value is {}",
                        key, value));
    }

    @NotNull
    @Override
    public Mono<Boolean> setIfPresent(@NotNull String key, @NotNull Object value, @NotNull Duration timeout) {
        return reactiveRedisTemplate.opsForValue().setIfPresent(key, value, timeout)
                .doOnSuccess(res -> logger.info("set if present success key is {} value is {} " +
                        "expiration time is {} ms", key, value, timeout.toMillis()));
    }

    @NotNull
    @Override
    public Mono<Boolean> multiSet(@NotNull Map<? extends String, ?> map) {
        return reactiveRedisTemplate.opsForValue().multiSet(map);
    }

    @NotNull
    @Override
    public Mono<Boolean> multiSetIfAbsent(@NotNull Map<? extends String, ?> map) {
        return reactiveRedisTemplate.opsForValue().multiSetIfAbsent(map);
    }

    @SuppressWarnings("unchecked")
    public <T> Mono<T> getAs(Object key) {
        return get(key).map(obj -> (T) obj)
                .doOnSuccess(value -> logger.info("get success key is {} value is {}", key, value));
    }

    @NotNull
    @Override
    public Mono<Object> get(@NotNull Object key) {
        return reactiveRedisTemplate.opsForValue().get(key)
                .doOnSuccess(value -> logger.info("get success key is {} value is {}", key, value));
    }

    @SuppressWarnings("unchecked")
    public <T> Mono<T> getAndDeleteAs(@NotNull String key) {
        return getAndDelete(key).map(obj -> (T) obj)
                .doOnSuccess(value -> logger.info("get and delete success key is {} value is {}",
                        key, value));
    }

    @NotNull
    @Override
    public Mono<Object> getAndDelete(@NotNull String key) {
        return reactiveRedisTemplate.opsForValue().getAndDelete(key)
                .doOnSuccess(value -> logger.info("get and delete success key is {} value is {}",
                        key, value));
    }

    @SuppressWarnings("unchecked")
    public <T> Mono<T> getAndExpireAs(@NotNull String key, @NotNull Duration timeout) {
        return getAndExpire(key, timeout).map(obj -> (T) obj)
                .doOnSuccess(value -> logger.info("get and expire success key is {} value is {} " +
                        "expiration time is {} ms", key, value, timeout.toMillis()));
    }

    @NotNull
    @Override
    public Mono<Object> getAndExpire(@NotNull String key, @NotNull Duration timeout) {
        return reactiveRedisTemplate.opsForValue().getAndExpire(key, timeout)
                .doOnSuccess(value -> logger.info("get and expire success key is {} value is {} " +
                        "expiration time is {} ms", key, value, timeout.toMillis()));
    }

    @SuppressWarnings("unchecked")
    public <T> Mono<T> getAndPersistAs(@NotNull String key) {
        return getAndPersist(key).map(obj -> (T) obj)
                .doOnSuccess(value -> logger.info("get and persist success key is {} value is {}",
                        key, value));
    }

    @NotNull
    @Override
    public Mono<Object> getAndPersist(@NotNull String key) {
        return reactiveRedisTemplate.opsForValue().getAndPersist(key)
                .doOnSuccess(value -> logger.info("get and persist success key is {} value is {}",
                        key, value))
                ;
    }

    @SuppressWarnings("unchecked")
    public <T> Mono<T> getAndSetAs(@NotNull String key, @NotNull Object value) {
        return getAndSet(key, value).map(obj -> (T) obj)
                .doOnSuccess(cache -> logger.info("get and set success key is {} value is {}",
                        key, cache));
    }

    @NotNull
    @Override
    public Mono<Object> getAndSet(@NotNull String key, @NotNull Object value) {
        return reactiveRedisTemplate.opsForValue().getAndSet(key, value)
                .doOnSuccess(cache -> logger.info("get and set success key is {} value is {}",
                        key, cache));
    }

    @SuppressWarnings("unchecked")
    public <T> Mono<List<T>> multiGetAs(@NotNull Collection<String> keys) {
        return multiGet(keys)
                .map(list -> list.stream().map(obj -> (T) obj)
                        .collect(Collectors.toList()))
                .doOnSuccess(cache -> logger.info("multi get success key is {} value is {}",
                        keys, cache));
    }

    @NotNull
    @Override
    public Mono<List<Object>> multiGet(@NotNull Collection<String> keys) {
        return reactiveRedisTemplate.opsForValue().multiGet(keys)
                .doOnSuccess(cache -> logger.info("multi get success key is {} value is {}",
                        keys, cache));
    }

    @NotNull
    @Override
    public Mono<Long> increment(@NotNull String key) {
        return reactiveRedisTemplate.opsForValue().increment(key)
                .doOnSuccess(cache -> logger.info("increment success key is {} value is {}",
                        key, cache));
    }

    @NotNull
    @Override
    public Mono<Long> increment(@NotNull String key, long delta) {
        return reactiveRedisTemplate.opsForValue().increment(key, delta)
                .doOnSuccess(cache -> logger.info("increment success key is {} value is {} delta is {}",
                        key, cache, delta));
    }

    @NotNull
    @Override
    public Mono<Double> increment(@NotNull String key, double delta) {
        return reactiveRedisTemplate.opsForValue().increment(key, delta)
                .doOnSuccess(cache -> logger.info("increment success key is {} value is {} delta is {}",
                        key, cache, delta));
    }

    @NotNull
    @Override
    public Mono<Long> decrement(@NotNull String key) {
        return reactiveRedisTemplate.opsForValue().decrement(key)
                .doOnSuccess(cache -> logger.info("decrement success key is {} value is {}",
                        key, cache));
    }

    @NotNull
    @Override
    public Mono<Long> decrement(@NotNull String key, long delta) {
        return reactiveRedisTemplate.opsForValue().decrement(key, delta)
                .doOnSuccess(cache -> logger.info("decrement success key is {} value is {} delta is {}",
                        key, cache, delta));
    }

    @NotNull
    @Override
    public Mono<Long> append(@NotNull String key, @NotNull String value) {
        return reactiveRedisTemplate.opsForValue().append(key, value)
                .doOnSuccess(cache -> logger.info("append success key is {} value is {}",
                        key, value));
    }

    @NotNull
    @Override
    public Mono<String> get(@NotNull String key, long start, long end) {
        return reactiveRedisTemplate.opsForValue().get(key, start, end)
                .doOnSuccess(cache -> logger.info("get success key is {} start is {} end is {} value is {}",
                        key, start, end, cache));
    }

    @NotNull
    @Override
    public Mono<Long> set(@NotNull String key, @NotNull Object value, long offset) {
        return reactiveRedisTemplate.opsForValue().set(key, value, offset)
                .doOnSuccess(cache -> logger.info("set success key is {} value is {} offset is {}",
                        key, value, offset));
    }

    @NotNull
    @Override
    public Mono<Long> size(@NotNull String key) {
        return reactiveRedisTemplate.opsForValue().size(key)
                .doOnSuccess(cache -> logger.info("size success key is {} value is {}",
                        key, cache));
    }

    @NotNull
    @Override
    public Mono<Boolean> setBit(@NotNull String key, long offset, boolean value) {
        return reactiveRedisTemplate.opsForValue().setBit(key, offset, value)
                .doOnSuccess(cache -> logger.info("set bit success key is {} value is {} offset is {}",
                        key, value, offset));
    }

    @NotNull
    @Override
    public Mono<Boolean> getBit(@NotNull String key, long offset) {
        return reactiveRedisTemplate.opsForValue().getBit(key, offset)
                .doOnSuccess(cache -> logger.info("get bit success key is {} value is {} offset is {}",
                        key, cache, offset));
    }

    @NotNull
    @Override
    public Mono<List<Long>> bitField(@NotNull String key, @NotNull BitFieldSubCommands command) {
        return reactiveRedisTemplate.opsForValue().bitField(key, command);
    }

    @NotNull
    public Mono<Boolean> delete(@NotNull String name) {
        return reactiveRedisTemplate.opsForValue().delete(name)
                .doOnSuccess(res -> logger.info("delete success key is {}", name));
    }
}
