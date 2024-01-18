# Reactive Redis使用

如果需要使用redis首先需要在common模块的pom.xml文件中加入如下maven坐标

```xml
<dependency>
    <groupId>com.g7.framework</groupId>
    <artifactId>redis-reactive-spring-boot-autoconfigure</artifactId>
</dependency>
```

### 使用示例

```java
@Autowired
private ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate;
 
 
@GetMapping("/redis/json")
public Mono<Result<Boolean>> saveJson() {
    OpLog opLog = new OpLog();
    opLog.setHandle(0);
    opLog.setOrgroot("201XW3");
    opLog.setOrgcode("201XW30101");
    opLog.setLogId("121212121212121");
    opLog.setOneLevel(0);
    opLog.setTwoLevel(0);
    opLog.setThreeLevel(0);
    opLog.setOperatorId("21212121212121");
    opLog.setOperatorType(0);
    opLog.setOperatorUserName("dream");
    opLog.setOperatorRealName("yao");
    opLog.setOperatorOrgCode("201XW30101");
    opLog.setOperatorOrgName("201XW30101");
    opLog.setDescription("reactive redis test");
    opLog.setWaybillId("21212121212121");
    opLog.setWaybillNo("21212121212121");
    opLog.setId(0L);
    opLog.setGmtCreate(LocalDateTime.now());
    opLog.setGmtModified(LocalDateTime.now());
    logger.info("trace链路追踪测试");
    return reactiveRedisTemplate.opsForValue().set("ntocc-reactor-demo:demo_json_test", opLog)
            .map(bl -> Result.<Boolean>create().success(bl));
}
```

也可以注入 如下bean来实现redis操作

```java
ReactiveRedisHash
ReactiveRedisList
ReactiveRedisSet
ReactiveRedisValue
ReactiveRedisZSet
```

### 分布式锁操作

注入如下bean来实现分布式锁操作

```java
ReactiveLockRegistry
```