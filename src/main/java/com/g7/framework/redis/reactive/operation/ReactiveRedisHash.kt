package com.g7.framework.redis.reactive.operation

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveHashOperations
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ScanOptions
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * @author dreamyao
 * @title
 * @date 2022/3/1 4:09 下午
 * @since 1.0.0
 */
class ReactiveRedisHash(private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Any>) :
    ReactiveHashOperations<String, String, Any> {

    private val logger: Logger = LoggerFactory.getLogger(ReactiveRedisHash::class.java)

    override fun remove(key: String, vararg hashKeys: Any?): Mono<Long> {
        return reactiveRedisTemplate.opsForHash<String, String>().remove(key, hashKeys)
    }

    override fun hasKey(key: String, hashKey: Any): Mono<Boolean> {
        return reactiveRedisTemplate.opsForHash<String, String>().hasKey(key, hashKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> getAs(key: String, hashKey: String): Mono<T> {
        return get(key, hashKey).map { it as T }
    }

    override fun get(key: String, hashKey: Any): Mono<Any> {
        return reactiveRedisTemplate.opsForHash<String, Any>().get(key, hashKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> multiGetAs(key: String, hashKeys: MutableCollection<String>): Mono<MutableList<T>> {
        return multiGet(key, hashKeys).map { it as MutableList<T> }
    }

    override fun multiGet(key: String, hashKeys: MutableCollection<String>): Mono<MutableList<Any>> {
        return reactiveRedisTemplate.opsForHash<String, Any>().multiGet(key, hashKeys)
    }

    override fun increment(key: String, hashKey: String, delta: Long): Mono<Long> {
        return reactiveRedisTemplate.opsForHash<String, String>().increment(key, hashKey, delta)
    }

    override fun increment(key: String, hashKey: String, delta: Double): Mono<Double> {
        return reactiveRedisTemplate.opsForHash<String, String>().increment(key, hashKey, delta)
    }

    override fun randomKey(key: String): Mono<String> {
        return reactiveRedisTemplate.opsForHash<String, String>().randomKey(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomEntryAs(key: String): Mono<MutableMap.MutableEntry<String, T>> {
        return randomEntry(key).map { it as MutableMap.MutableEntry<String, T> }
    }

    override fun randomEntry(key: String): Mono<MutableMap.MutableEntry<String, Any>> {
        return reactiveRedisTemplate.opsForHash<String, Any>().randomEntry(key)
    }

    override fun randomKeys(key: String, count: Long): Flux<String> {
        return reactiveRedisTemplate.opsForHash<String, String>().randomKeys(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomEntriesAs(key: String, count: Long): Flux<MutableMap.MutableEntry<String, T>> {
        return randomEntries(key, count).map { it as MutableMap.MutableEntry<String, T> }
    }

    override fun randomEntries(key: String, count: Long): Flux<MutableMap.MutableEntry<String, Any>> {
        return reactiveRedisTemplate.opsForHash<String, Any>().randomEntries(key, count)
    }

    override fun keys(key: String): Flux<String> {
        return reactiveRedisTemplate.opsForHash<String, String>().keys(key)
    }

    override fun size(key: String): Mono<Long> {
        return reactiveRedisTemplate.opsForHash<String, String>().size(key)
    }

    override fun putAll(key: String, map: MutableMap<out String, out Any>): Mono<Boolean> {
        return reactiveRedisTemplate.opsForHash<String, Any>().putAll(key, map)
    }

    override fun put(key: String, hashKey: String, value: Any): Mono<Boolean> {
        return reactiveRedisTemplate.opsForHash<String, Any>().put(key, hashKey, value)
    }

    override fun putIfAbsent(key: String, hashKey: String, value: Any): Mono<Boolean> {
        return reactiveRedisTemplate.opsForHash<String, Any>().putIfAbsent(key, hashKey, value)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> valuesAs(key: String): Flux<T> {
        return values(key).map { it as T }
    }

    override fun values(key: String): Flux<Any> {
        return reactiveRedisTemplate.opsForHash<String, Any>().values(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> entriesAs(key: String): Flux<MutableMap.MutableEntry<String, T>> {
        return entries(key).map { it as MutableMap.MutableEntry<String, T> }
    }

    override fun entries(key: String): Flux<MutableMap.MutableEntry<String, Any>> {
        return reactiveRedisTemplate.opsForHash<String, Any>().entries(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> scanAs(key: String, options: ScanOptions): Flux<MutableMap.MutableEntry<String, T>> {
        return scan(key, options).map { it as MutableMap.MutableEntry<String, T> }
    }

    override fun scan(key: String, options: ScanOptions): Flux<MutableMap.MutableEntry<String, Any>> {
        return reactiveRedisTemplate.opsForHash<String, Any>().scan(key, options)
    }

    override fun delete(key: String): Mono<Boolean> {
        return reactiveRedisTemplate.opsForHash<String, String>().delete(key)
    }
}