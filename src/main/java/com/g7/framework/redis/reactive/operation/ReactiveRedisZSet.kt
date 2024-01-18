package com.g7.framework.redis.reactive.operation

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.RedisZSetCommands
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ReactiveZSetOperations
import org.springframework.data.redis.core.ScanOptions
import org.springframework.data.redis.core.ZSetOperations
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * @author dreamyao
 * @title
 * @date 2022/3/1 4:08 下午
 * @since 1.0.0
 */
class ReactiveRedisZSet(private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Any>) :
    ReactiveZSetOperations<String, Any> {

    private val logger: Logger = LoggerFactory.getLogger(ReactiveRedisZSet::class.java)

    override fun add(key: String, value: Any, score: Double): Mono<Boolean> {
        return reactiveRedisTemplate.opsForZSet().add(key, value, score).doOnSuccess {
            logger.info("add cache key is [{}] value is [{}] score is [{}]", key, value, score)
        }
    }

    override fun addAll(key: String, tuples: MutableCollection<out ZSetOperations.TypedTuple<Any>>): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().addAll(key, tuples)
    }

    override fun remove(key: String, vararg values: Any?): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().remove(key, values)
    }

    override fun incrementScore(key: String, value: Any, delta: Double): Mono<Double> {
        return reactiveRedisTemplate.opsForZSet().incrementScore(key, value, delta)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomMemberAs(key: String): Mono<T> {
        return randomMember(key).map { it as T }
    }

    override fun randomMember(key: String): Mono<Any> {
        return reactiveRedisTemplate.opsForZSet().randomMember(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> distinctRandomMembersAs(key: String, count: Long): Flux<T> {
        return distinctRandomMembers(key, count).map { it as T }
    }

    override fun distinctRandomMembers(key: String, count: Long): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().distinctRandomMembers(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomMembersAs(key: String, count: Long): Flux<T> {
        return randomMembers(key, count).map { it as T }
    }

    override fun randomMembers(key: String, count: Long): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().randomMembers(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomMemberWithScoreAs(key: String): Mono<ZSetOperations.TypedTuple<T>> {
        return randomMemberWithScore(key).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun randomMemberWithScore(key: String): Mono<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().randomMemberWithScore(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> distinctRandomMembersWithScoreAs(key: String, count: Long): Flux<ZSetOperations.TypedTuple<T>> {
        return distinctRandomMembersWithScore(key, count).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun distinctRandomMembersWithScore(key: String, count: Long): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().distinctRandomMembersWithScore(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> randomMembersWithScoreAs(key: String, count: Long): Flux<ZSetOperations.TypedTuple<T>> {
        return randomMembersWithScore(key, count).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun randomMembersWithScore(key: String, count: Long): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().randomMembersWithScore(key, count)
    }

    override fun rank(key: String, o: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().rank(key, o)
    }

    override fun reverseRank(key: String, o: Any): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().reverseRank(key, o)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeAs(key: String, range: Range<Long>): Flux<T> {
        return range(key, range).map { it as T }
    }

    override fun range(key: String, range: Range<Long>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().range(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeWithScoresAs(key: String, range: Range<Long>): Flux<ZSetOperations.TypedTuple<T>> {
        return rangeWithScores(key, range).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun rangeWithScores(key: String, range: Range<Long>): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().rangeWithScores(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeByScoreAs(key: String, range: Range<Double>): Flux<T> {
        return rangeByScore(key, range).map { it as T }
    }

    override fun rangeByScore(key: String, range: Range<Double>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().rangeByScore(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeByScoreAs(key: String, range: Range<Double>, limit: RedisZSetCommands.Limit): Flux<T> {
        return rangeByScore(key, range, limit).map { it as T }
    }

    override fun rangeByScore(key: String, range: Range<Double>, limit: RedisZSetCommands.Limit): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().rangeByScore(key, range, limit)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeByScoreWithScoresAs(key: String, range: Range<Double>): Flux<ZSetOperations.TypedTuple<T>> {
        return rangeByScoreWithScores(key, range).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun rangeByScoreWithScores(key: String, range: Range<Double>): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().rangeByScoreWithScores(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeByScoreWithScoresAs(
        key: String,
        range: Range<Double>,
        limit: RedisZSetCommands.Limit
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return rangeByScoreWithScores(key, range, limit).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun rangeByScoreWithScores(
        key: String,
        range: Range<Double>,
        limit: RedisZSetCommands.Limit
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().rangeByScoreWithScores(key, range, limit)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeAs(key: String, range: Range<Long>): Flux<T> {
        return reverseRange(key, range).map { it as T }
    }

    override fun reverseRange(key: String, range: Range<Long>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().reverseRange(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeWithScoresAs(key: String, range: Range<Long>): Flux<ZSetOperations.TypedTuple<T>> {
        return reverseRangeWithScores(key, range).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun reverseRangeWithScores(key: String, range: Range<Long>): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeWithScores(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeByScoreAs(key: String, range: Range<Double>): Flux<T> {
        return reverseRangeByScore(key, range).map { it as T }
    }

    override fun reverseRangeByScore(key: String, range: Range<Double>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeByScore(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeByScoreAs(key: String, range: Range<Double>, limit: RedisZSetCommands.Limit): Flux<T> {
        return reverseRangeByScore(key, range, limit).map { it as T }
    }

    override fun reverseRangeByScore(key: String, range: Range<Double>, limit: RedisZSetCommands.Limit): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeByScore(key, range, limit)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeByScoreWithScoresAs(
        key: String,
        range: Range<Double>
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return reverseRangeByScoreWithScores(key, range).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun reverseRangeByScoreWithScores(
        key: String,
        range: Range<Double>
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeByScoreWithScoresAs(
        key: String,
        range: Range<Double>,
        limit: RedisZSetCommands.Limit
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return reverseRangeByScoreWithScores(key, range, limit).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun reverseRangeByScoreWithScores(
        key: String,
        range: Range<Double>,
        limit: RedisZSetCommands.Limit
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, range, limit)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> scanAs(key: String, options: ScanOptions): Flux<ZSetOperations.TypedTuple<T>> {
        return scan(key, options).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun scan(key: String, options: ScanOptions): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().scan(key, options)
    }

    override fun count(key: String, range: Range<Double>): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().count(key, range)
    }

    override fun lexCount(key: String, range: Range<String>): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().lexCount(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popMinAs(key: String): Mono<ZSetOperations.TypedTuple<T>> {
        return popMin(key).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun popMin(key: String): Mono<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().popMin(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popMinAs(key: String, count: Long): Flux<ZSetOperations.TypedTuple<T>> {
        return popMin(key, count).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun popMin(key: String, count: Long): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().popMin(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popMinAs(key: String, timeout: Duration): Mono<ZSetOperations.TypedTuple<T>> {
        return popMin(key, timeout).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun popMin(key: String, timeout: Duration): Mono<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().popMin(key, timeout)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popMaxAs(key: String): Mono<ZSetOperations.TypedTuple<T>> {
        return popMax(key).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun popMax(key: String): Mono<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().popMax(key)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popMaxAs(key: String, count: Long): Flux<ZSetOperations.TypedTuple<T>> {
        return popMax(key, count).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun popMax(key: String, count: Long): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().popMax(key, count)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> popMaxAs(key: String, timeout: Duration): Mono<ZSetOperations.TypedTuple<T>> {
        return popMax(key, timeout).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun popMax(key: String, timeout: Duration): Mono<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().popMax(key, timeout)
    }

    override fun size(key: String): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().size(key)
    }

    override fun score(key: String, o: Any): Mono<Double> {
        return reactiveRedisTemplate.opsForZSet().score(key, o)
    }

    override fun score(key: String, vararg o: Any?): Mono<MutableList<Double>> {
        return reactiveRedisTemplate.opsForZSet().score(key, *o)
    }

    override fun removeRange(key: String, range: Range<Long>): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().removeRange(key, range)
    }

    override fun removeRangeByLex(key: String, range: Range<String>): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().removeRangeByLex(key, range)
    }

    override fun removeRangeByScore(key: String, range: Range<Double>): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().removeRangeByScore(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> differenceAs(key: String, otherKeys: MutableCollection<String>): Flux<T> {
        return difference(key, otherKeys).map { it as T }
    }

    override fun difference(key: String, otherKeys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().difference(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> differenceWithScoresAs(
        key: String,
        otherKeys: MutableCollection<String>
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return differenceWithScores(key, otherKeys).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun differenceWithScores(
        key: String,
        otherKeys: MutableCollection<String>
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().differenceWithScores(key, otherKeys)
    }

    override fun differenceAndStore(key: String, otherKeys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().differenceAndStore(key, otherKeys, destKey)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> intersectAs(key: String, otherKeys: MutableCollection<String>): Flux<T> {
        return intersect(key, otherKeys).map { it as T }
    }

    override fun intersect(key: String, otherKeys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().intersect(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> intersectWithScoresAs(
        key: String,
        otherKeys: MutableCollection<String>
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return intersectWithScores(key, otherKeys).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun intersectWithScores(
        key: String,
        otherKeys: MutableCollection<String>
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().intersectWithScores(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> intersectWithScoresAs(
        key: String,
        otherKeys: MutableCollection<String>,
        aggregate: RedisZSetCommands.Aggregate,
        weights: RedisZSetCommands.Weights
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return intersectWithScores(key, otherKeys, aggregate, weights).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun intersectWithScores(
        key: String,
        otherKeys: MutableCollection<String>,
        aggregate: RedisZSetCommands.Aggregate,
        weights: RedisZSetCommands.Weights
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().intersectWithScores(key, otherKeys, aggregate, weights)
    }

    override fun intersectAndStore(key: String, otherKeys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().intersectAndStore(key, otherKeys, destKey)
    }

    override fun intersectAndStore(
        key: String,
        otherKeys: MutableCollection<String>,
        destKey: String,
        aggregate: RedisZSetCommands.Aggregate,
        weights: RedisZSetCommands.Weights
    ): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().intersectAndStore(key, otherKeys, destKey, aggregate, weights)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> unionAs(key: String, otherKeys: MutableCollection<String>): Flux<T> {
        return union(key, otherKeys).map { it as T }
    }

    override fun union(key: String, otherKeys: MutableCollection<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().union(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> unionWithScoresAs(
        key: String,
        otherKeys: MutableCollection<String>
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return unionWithScores(key, otherKeys).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun unionWithScores(
        key: String,
        otherKeys: MutableCollection<String>
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().unionWithScores(key, otherKeys)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> unionWithScoresAs(
        key: String,
        otherKeys: MutableCollection<String>,
        aggregate: RedisZSetCommands.Aggregate,
        weights: RedisZSetCommands.Weights
    ): Flux<ZSetOperations.TypedTuple<T>> {
        return unionWithScores(key, otherKeys, aggregate, weights).map { it as ZSetOperations.TypedTuple<T> }
    }

    override fun unionWithScores(
        key: String,
        otherKeys: MutableCollection<String>,
        aggregate: RedisZSetCommands.Aggregate,
        weights: RedisZSetCommands.Weights
    ): Flux<ZSetOperations.TypedTuple<Any>> {
        return reactiveRedisTemplate.opsForZSet().unionWithScores(key, otherKeys, aggregate, weights)
    }

    override fun unionAndStore(key: String, otherKey: String, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().unionAndStore(key, otherKey, destKey)
    }

    override fun unionAndStore(key: String, otherKeys: MutableCollection<String>, destKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().unionAndStore(key, otherKeys, destKey)
    }

    override fun unionAndStore(
        key: String,
        otherKeys: MutableCollection<String>,
        destKey: String,
        aggregate: RedisZSetCommands.Aggregate,
        weights: RedisZSetCommands.Weights
    ): Mono<Long> {
        return reactiveRedisTemplate.opsForZSet().unionAndStore(key, otherKeys, destKey, aggregate, weights)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeByLexAs(key: String, range: Range<String>): Flux<T> {
        return rangeByLex(key, range).map { it as T }
    }

    override fun rangeByLex(key: String, range: Range<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().rangeByLex(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> rangeByLexAs(key: String, range: Range<String>, limit: RedisZSetCommands.Limit): Flux<T> {
        return rangeByLex(key, range, limit).map { it as T }
    }

    override fun rangeByLex(key: String, range: Range<String>, limit: RedisZSetCommands.Limit): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().rangeByLex(key, range, limit)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeByLexAs(key: String, range: Range<String>): Flux<T> {
        return reverseRangeByLex(key, range).map { it as T }
    }

    override fun reverseRangeByLex(key: String, range: Range<String>): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeByLex(key, range)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> reverseRangeByLexAs(key: String, range: Range<String>, limit: RedisZSetCommands.Limit): Flux<T> {
        return reverseRangeByLex(key, range, limit).map { it as T }
    }

    override fun reverseRangeByLex(key: String, range: Range<String>, limit: RedisZSetCommands.Limit): Flux<Any> {
        return reactiveRedisTemplate.opsForZSet().reverseRangeByLex(key, range, limit)
    }

    override fun delete(key: String): Mono<Boolean> {
        return reactiveRedisTemplate.opsForZSet().delete(key)
    }
}