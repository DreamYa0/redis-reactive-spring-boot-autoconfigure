package com.g7.framework.redis.reactive;

import com.g7.framework.redis.reactive.lock.ReactiveLockRegistry;
import com.g7.framework.redis.reactive.lock.RedisReactiveLockRegistry;
import com.g7.framework.redis.reactive.operation.*;
import com.g7.framework.redis.reactive.properties.RedisReactiveLockProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import reactor.core.publisher.Flux;

/**
 * @author dreamyao
 * @title
 * @date 2022/1/21 1:40 下午
 * @since 1.0.0
 */
@AutoConfiguration
@ConditionalOnClass({ReactiveRedisConnectionFactory.class, ReactiveRedisTemplate.class,
        ReactiveLockRegistry.class, Flux.class})
@AutoConfigureAfter(RedisAutoConfiguration.class)
@EnableConfigurationProperties(RedisReactiveLockProperties.class)
public class ReactiveRedisAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ReactiveRedisAutoConfiguration.class);

    @Bean
    @Primary
    @ConditionalOnMissingBean(name = "reactiveRedisTemplate")
    @ConditionalOnBean(ReactiveRedisConnectionFactory.class)
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(
            ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {

        final StringRedisSerializer stringSerializer = new StringRedisSerializer();
        final GenericJackson2JsonRedisSerializer jacksonSerializer = new GenericJackson2JsonRedisSerializer();

        RedisSerializationContext<String, Object> serializationContext = RedisSerializationContext
                .<String, Object>newSerializationContext()
                .key(stringSerializer)
                .value(jacksonSerializer)
                .hashKey(stringSerializer)
                .hashValue(jacksonSerializer)
                .build();

        return new ReactiveRedisTemplate<>(reactiveRedisConnectionFactory,
                serializationContext);
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean(name = "reactiveStringRedisTemplate")
    @ConditionalOnBean(ReactiveRedisConnectionFactory.class)
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(
            ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
        return new ReactiveStringRedisTemplate(reactiveRedisConnectionFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(name = "reactiveStringRedisTemplate")
    public ReactiveLockRegistry reactiveLockRegistry(ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                                                     RedisReactiveLockProperties redisReactiveLockProperties) {
        ReactiveLockRegistry redisReactiveLockRegistry = new RedisReactiveLockRegistry(
                reactiveStringRedisTemplate,
                redisReactiveLockProperties.getExpireEvictIdle(),
                redisReactiveLockProperties.getExpireAfter(),
                redisReactiveLockProperties.getRegistryKeyPrefix());
        logger.info("load reactive redis reactive lock registry success,registry key prefix:{}," +
                        "default expire duration:{}", redisReactiveLockProperties.getRegistryKeyPrefix(),
                redisReactiveLockProperties.getExpireAfter());
        return redisReactiveLockRegistry;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ReactiveRedisTemplate.class)
    public ReactiveRedisValue reactiveRedisValue(
            @Autowired ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        return new ReactiveRedisValue(reactiveRedisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ReactiveRedisTemplate.class)
    public ReactiveRedisHash reactiveRedisHash(
            @Autowired ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        return new ReactiveRedisHash(reactiveRedisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ReactiveRedisTemplate.class)
    public ReactiveRedisList reactiveRedisList(
            @Autowired ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        return new ReactiveRedisList(reactiveRedisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ReactiveRedisTemplate.class)
    public ReactiveRedisSet reactiveRedisSet(
            @Autowired ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        return new ReactiveRedisSet(reactiveRedisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ReactiveRedisTemplate.class)
    public ReactiveRedisZSet reactiveRedisZSet(
            @Autowired ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        return new ReactiveRedisZSet(reactiveRedisTemplate);
    }
}
