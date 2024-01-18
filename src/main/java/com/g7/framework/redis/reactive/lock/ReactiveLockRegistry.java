package com.g7.framework.redis.reactive.lock;

/**
 * Reactive lock 注册表
 * @author dreamyao
 * @date 2022/3/1 4:09 下午
 */
public interface ReactiveLockRegistry {

    /**
     * 模式锁的KEY (unique)
     */
    String DEFAULT_LOCK_KEY = "@@reactive_lock_default_key@@";

    /**
     * 获取锁对象 使用默认锁KEY
     * @return reactive lock
     */
    default ReactiveLock obtain() {
        return obtain(DEFAULT_LOCK_KEY);
    }

    /**
     * 获取锁对象
     * @param lockKey 锁KEY
     * @return reactive lock
     */
    ReactiveLock obtain(String lockKey);
}
