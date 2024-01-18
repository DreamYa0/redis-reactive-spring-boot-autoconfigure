package com.g7.framework.redis.reactive.operation

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.redis.connection.ReactiveListCommands
import org.springframework.data.redis.core.ReactiveListOperations
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * @author dreamyao
 * @title
 * @date 2022/3/1 4:09 下午
 * @since 1.0.0
 */
class ReactiveRedisList(private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Any>) :
    ReactiveListOperations<String, Any> {

    private val logger: Logger = LoggerFactory.getLogger(ReactiveRedisList::class.java)

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeAs(key: String, start: Long, end: Long): Flux<T> {
        return range(key, start, end).map { it as T }
    }

    override fun range(key: String, start: Long, end: Long): Flux<Any> {
        return reactiveRedisTemplate.opsForList().range(key, start, end)
    }

    override fun trim(key: String, start: Long, end: Long): Mono<Boolean> {
        return reactiveRedisTemplate.opsForList().trim(key, start, end)
    }

    override fun size(key: String): Mono<Long> {
        return reactiveRedisTemplate.opsForList().size(key)
    }

    override fun leftPush(key: String, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().leftPush(key, value)
    }

    override fun leftPush(key: String, pivot: Any, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().leftPush(key, pivot, value)
    }

    override fun leftPushAll(key: String, vararg values: Any?): Mono<Long> {
        return reactiveRedisTemplate.opsForList().leftPushAll(key, *values)
    }

    override fun leftPushAll(key: String, values: MutableCollection<Any>): Mono<Long> {
        // return reactiveRedisTemplate.opsForList().leftPushAll(key, values)
        TODO()
    }

    override fun leftPushIfPresent(key: String, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().leftPushIfPresent(key, value)
    }

    override fun rightPush(key: String, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().rightPush(key, value)
    }

    override fun rightPush(key: String, pivot: Any, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().rightPush(key, pivot, value)
    }

    override fun rightPushAll(key: String, vararg values: Any?): Mono<Long> {
        return reactiveRedisTemplate.opsForList().rightPushAll(key, *values)
    }

    override fun rightPushAll(key: String, values: MutableCollection<Any>): Mono<Long> {
        // return reactiveRedisTemplate.opsForList().rightPushAll(key, values)
        TODO()
    }

    override fun rightPushIfPresent(key: String, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().rightPushIfPresent(key, value)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> moveAs(
        sourceKey: String,
        from: ReactiveListCommands.Direction,
        destinationKey: String,
        to: ReactiveListCommands.Direction
    ): Mono<T> {
        return move(sourceKey, from, destinationKey, to).map { it as T }
    }

    override fun move(
        sourceKey: String,
        from: ReactiveListCommands.Direction,
        destinationKey: String,
        to: ReactiveListCommands.Direction
    ): Mono<Any> {
        return reactiveRedisTemplate.opsForList().move(sourceKey, from, destinationKey, to)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> moveAs(
        sourceKey: String,
        from: ReactiveListCommands.Direction,
        destinationKey: String,
        to: ReactiveListCommands.Direction,
        timeout: Duration
    ): Mono<T> {
        return move(sourceKey, from, destinationKey, to, timeout).map { it as T }
    }

    override fun move(
        sourceKey: String,
        from: ReactiveListCommands.Direction,
        destinationKey: String,
        to: ReactiveListCommands.Direction,
        timeout: Duration
    ): Mono<Any> {
        return reactiveRedisTemplate.opsForList().move(sourceKey, from, destinationKey, to, timeout)
    }

    override fun set(key: String, index: Long, value: Any): Mono<Boolean> {
        return reactiveRedisTemplate.opsForList().set(key, index, value)
    }

    override fun remove(key: String, count: Long, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().remove(key, count, value)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> indexAs(key: String, index: Long): Mono<T> {
        return index(key, index).map { it as T }
    }

    override fun index(key: String, index: Long): Mono<Any> {
        return reactiveRedisTemplate.opsForList().index(key, index)
    }

    override fun indexOf(key: String, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().indexOf(key, value)
    }

    override fun lastIndexOf(key: String, value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForList().lastIndexOf(key, value)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> leftPopAs(key: String): Mono<T> {
        return leftPop(key).map { it as T }
    }

    override fun leftPop(key: String): Mono<Any> {
        return reactiveRedisTemplate.opsForList().leftPop(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> leftPopAs(key: String, timeout: Duration): Mono<T> {
        return leftPop(key, timeout).map { it as T }
    }

    override fun leftPop(key: String, timeout: Duration): Mono<Any> {
        return reactiveRedisTemplate.opsForList().leftPop(key, timeout)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rightPopAs(key: String): Mono<T> {
        return rightPop(key).map { it as T }
    }

    override fun rightPop(key: String): Mono<Any> {
        return reactiveRedisTemplate.opsForList().rightPop(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rightPopAs(key: String, timeout: Duration): Mono<T> {
        return rightPop(key, timeout).map { it as T }
    }

    override fun rightPop(key: String, timeout: Duration): Mono<Any> {
        return reactiveRedisTemplate.opsForList().rightPop(key, timeout)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rightPopAndLeftPushAs(sourceKey: String, destinationKey: String): Mono<T> {
        return rightPopAndLeftPush(sourceKey, destinationKey).map { it as T }
    }

    override fun rightPopAndLeftPush(sourceKey: String, destinationKey: String): Mono<Any> {
        return reactiveRedisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rightPopAndLeftPushAs(sourceKey: String, destinationKey: String, timeout: Duration): Mono<T> {
        return rightPopAndLeftPush(sourceKey, destinationKey, timeout).map { it as T }
    }

    override fun rightPopAndLeftPush(sourceKey: String, destinationKey: String, timeout: Duration): Mono<Any> {
        return reactiveRedisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout)
    }

    override fun delete(key: String): Mono<Boolean> {
        return reactiveRedisTemplate.opsForList().delete(key)
    }
}