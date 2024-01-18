package com.g7.framework.redis.reactive.operation

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ReactiveSetOperations
import org.springframework.data.redis.core.ScanOptions
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * @author dreamyao
 * @title
 * @date 2022/3/1 4:08 下午
 * @since 1.0.0
 */
class ReactiveRedisSet(private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Any>) :
    ReactiveSetOperations<String, Any> {

    private val logger: Logger = LoggerFactory.getLogger(ReactiveRedisSet::class.java)

    override fun add(key: String, vararg value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().add(key, value).doOnSuccess {
            logger.info("set cache success key is [{}] value is [{}]", key, value)
        }
    }

    override fun remove(key: String, vararg value: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().remove(key, value).doOnSuccess {
            logger.info("remove cache success key is [{}] value is [{}]", key, value)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popAs(key: String): Mono<T> {
        return pop(key).map { it as T }
    }

    override fun pop(key: String): Mono<Any> {
        return reactiveRedisTemplate.opsForSet().pop(key).doOnSuccess {
            logger.info("pop cache success key is [{}] value is [{}]", key, it)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popAs(key: String, count: Long): Flux<T> {
        return pop(key, count).map { it as T }
    }

    override fun pop(key: String, count: Long): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().pop(key, count).doOnNext {
            logger.info("pop cache success key is [{}] value is [{}]", key, it)
        }
    }

    override fun move(sourceKey: String, value: Any, destKey: String): Mono<Boolean> {
        return reactiveRedisTemplate.opsForSet().move(sourceKey, value, destKey).doOnSuccess {
            logger.info(
                "move cache success source key is [{}] , dest key is [{}] value is [{}]", sourceKey, destKey, it
            )
        }
    }

    override fun size(key: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().size(key).doOnSuccess {
            logger.info("cache key is [{}] size is [{}]", key, it)
        }
    }

    override fun isMember(key: String, o: Any): Mono<Boolean> {
        return reactiveRedisTemplate.opsForSet().isMember(key, o)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> isMemberAs(key: String, vararg objects: Any?): Mono<MutableMap<T, Boolean>> {
        return isMember(key, *objects).map { it as MutableMap<T, Boolean> }
    }

    override fun isMember(key: String, vararg objects: Any?): Mono<MutableMap<Any, Boolean>> {
        return reactiveRedisTemplate.opsForSet().isMember(key, *objects)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> intersectAs(key: String, otherKey: String): Flux<T> {
        return intersect(key, otherKey).map { it as T }
    }

    override fun intersect(key: String, otherKey: String): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().intersect(key, otherKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> intersectAs(key: String, otherKeys: MutableCollection<String>): Flux<T> {
        return intersect(key, otherKeys).map { it as T }
    }

    override fun intersect(key: String, otherKeys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().intersect(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> intersectAs(keys: MutableCollection<String>): Flux<T> {
        return intersect(keys).map { it as T }
    }

    override fun intersect(keys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().intersect(keys)
    }

    override fun intersectAndStore(key: String, otherKey: String, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().intersectAndStore(key, otherKey, destKey)
    }

    override fun intersectAndStore(key: String, otherKeys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().intersectAndStore(key, otherKeys, destKey)
    }

    override fun intersectAndStore(keys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().intersectAndStore(keys, destKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> unionAs(key: String, otherKey: String): Flux<T> {
        return union(key, otherKey).map { it as T }
    }

    override fun union(key: String, otherKey: String): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().union(key, otherKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> unionAs(key: String, otherKeys: MutableCollection<String>): Flux<T> {
        return union(key, otherKeys).map { it as T }
    }

    override fun union(key: String, otherKeys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().union(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> unionAs(keys: MutableCollection<String>): Flux<T> {
        return union(keys).map { it as T }
    }

    override fun union(keys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().union(keys)
    }

    override fun unionAndStore(key: String, otherKey: String, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().unionAndStore(key, otherKey, destKey)
    }

    override fun unionAndStore(key: String, otherKeys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().unionAndStore(key, otherKeys, destKey)
    }

    override fun unionAndStore(keys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().unionAndStore(keys, destKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> differenceAs(key: String, otherKey: String): Flux<T> {
        return difference(key, otherKey).map { it as T }
    }

    override fun difference(key: String, otherKey: String): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().difference(key, otherKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> differenceAs(key: String, otherKeys: MutableCollection<String>): Flux<T> {
        return difference(key, otherKeys).map { it as T }
    }

    override fun difference(key: String, otherKeys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().difference(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> differenceAs(keys: MutableCollection<String>): Flux<T> {
        return difference(keys).map { it as T }
    }

    override fun difference(keys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().difference(keys)
    }

    override fun differenceAndStore(key: String, otherKey: String, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().differenceAndStore(key, otherKey, destKey)
    }

    override fun differenceAndStore(key: String, otherKeys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().differenceAndStore(key, otherKeys, destKey)
    }

    override fun differenceAndStore(keys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForSet().differenceAndStore(keys, destKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> membersAs(key: String): Flux<T> {
        return members(key).map { it as T }
    }

    override fun members(key: String): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().members(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> scanAs(key: String, options: ScanOptions): Flux<T> {
        return scan(key, options).map { it as T }
    }

    override fun scan(key: String, options: ScanOptions): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().scan(key, options)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomMemberAs(key: String): Mono<T> {
        return randomMember(key).map { it as T }
    }

    override fun randomMember(key: String): Mono<Any> {
        return reactiveRedisTemplate.opsForSet().randomMember(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> distinctRandomMembersAs(key: String, count: Long): Flux<T> {
        return distinctRandomMembers(key, count).map { it as T }
    }

    override fun distinctRandomMembers(key: String, count: Long): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().distinctRandomMembers(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomMembersAs(key: String, count: Long): Flux<T> {
        return randomMembers(key, count).map { it as T }
    }

    override fun randomMembers(key: String, count: Long): Flux<Any> {
        return reactiveRedisTemplate.opsForSet().randomMembers(key, count)
    }

    override fun delete(key: String): Mono<Boolean> {
        return reactiveRedisTemplate.opsForSet().delete(key)
    }
}