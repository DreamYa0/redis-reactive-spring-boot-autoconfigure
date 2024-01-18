package com.g7.framework.redis.reactive.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Redis Reactive Lock Properties Configuration
 * @author dreamyao
 * @date 2021/03/14
 */
@ConfigurationProperties(prefix = "spring.reactive.redis.lock")
public class RedisReactiveLockProperties {

    /**
     * global registry key prefix
     */
    private String registryKeyPrefix;

    /**
     * 最大过期时间
     */
    private Duration expireAfter = Duration.ofMinutes(1);

    /**
     * expire evict 空闲时间
     */
    private Duration expireEvictIdle = Duration.ofSeconds(3);

    public String getRegistryKeyPrefix() {
        return registryKeyPrefix;
    }

    public void setRegistryKeyPrefix(String registryKeyPrefix) {
        this.registryKeyPrefix = registryKeyPrefix;
    }

    public Duration getExpireAfter() {
        return expireAfter;
    }

    public void setExpireAfter(Duration expireAfter) {
        this.expireAfter = expireAfter;
    }

    public Duration getExpireEvictIdle() {
        return expireEvictIdle;
    }

    public void setExpireEvictIdle(Duration expireEvictIdle) {
        this.expireEvictIdle = expireEvictIdle;
    }

    @Override
    public String toString() {
        return "RedisReactiveLockProperties{" +
                "registryKeyPrefix='" + registryKeyPrefix + '\'' +
                ", expireAfter=" + expireAfter +
                ", expireEvictIdle=" + expireEvictIdle +
                '}';
    }
}
