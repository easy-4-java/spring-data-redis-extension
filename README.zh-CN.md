# spring-data-redis-extension

[English](./README.md) | [简体中文](./README.zh-CN.md)

[![Java](https://img.shields.io/badge/Java-8-orange)](https://github.com/easy-4-java/spring-data-redis-extension) [![License](https://img.shields.io/badge/license-Apache%202.0-green)](https://www.apache.org/licenses/LICENSE-2.0.txt)

spring-data-redis-extension（"Redis Operation Template SDK"）是 Spring Data Redis 的配套库，独立于 Spring Boot。

## 目录

- [1. Project Overview](#1-project-overview)
- [2. Features & Status](#2-features--status)
- [3. Requirements & Compatibility](#3-requirements--compatibility)
- [4. Architecture & Modules](#4-architecture--modules)
- [5. Installation](#5-installation)
- [6. Quick Start](#6-quick-start)
- [7. Configuration](#7-configuration)
- [8. Core Usage / API](#8-core-usage--api)
- [9. Testing & Build](#9-testing--build)
- [10. Versioning & Branches](#10-versioning--branches)
- [11. Contributing & License](#11-contributing--license)

## 1. Project Overview

`spring-data-redis-extension`（"Redis Operation Template SDK"）是 Spring Data Redis 的配套库，独立于 Spring Boot。它在标准 `RedisTemplate` / `ReactiveRedisTemplate` 之上提供更高级的操作模板，基于 geodesy 库增加地理距离辅助方法，内置可复用的 Lua 脚本（加锁、计数器）与发布/订阅监听注解——应用代码可基于纯 Spring Framework 使用更友好的 API。

它是 Spring Data Redis 之上的库层——不提供 Redis 连接自动配置，也不是 Spring Boot Starter。

典型场景：

| 场景 | 本模块提供的组件 |
|:---|:---|
| 高层键值操作 | `RedisOperationTemplate`（set/get、expire、hasKey、rename、type 等） |
| 响应式操作 | `ReactiveRedisOperationTemplate`（基于 Mono/Flux） |
| 地理距离与附近用户查询 | `GeoTemplate` / `ReactiveGeoTemplate` |
| Redis 加锁 / 计数 Lua 脚本 | `RedisLua`（LOCK、UNLOCK、INCR、DECR 等） |
| 发布/订阅监听注解 | `RedisChannelTopic` / `RedisPatternTopic` + `MessageListenerAdapter` |
| 键名与工具常量 | `RedisKey`、`RedisKeyConstant`、`MapUtils`、`RedisOperationException` |

## 2. Features & Status

项目状态：`1.0.x.*` 预发布开发线（快照版本）；在首个正式 Release 标签之前，公开 API 仍在稳定过程中。

| 能力 | 状态 | 说明 |
|:---|:---|:---|
| 键值操作模板 | 稳定 | `RedisOperationTemplate extends AbstractOperations<String, Object>`——`set`、`get`、`getString`、`setNx`、`expire`、`expireAt`、`getExpire`、`hasKey`、`keys`、`persist`、`randomKey`、`rename`、`renameIfAbsent`、`type`、`setRange` 等 |
| 响应式操作模板 | 稳定 | `ReactiveRedisOperationTemplate`——`expire`、`expireAt`、`getExpire`、`hasKey`、`getKey`、`getVagueKey`、raw-key 辅助方法 |
| 地理模板 | 稳定 | `GeoTemplate`——`geoAdd`、`distance`、`distanceValue`、`getCircleUsersByDistance`、`getCircleUsersByRadius`，以及基于 geodesy 的纯 Java `getDistance`（球面/椭球） |
| 响应式地理模板 | 稳定 | `ReactiveGeoTemplate`（地理辅助方法的响应式变体） |
| Lua 脚本 | 稳定 | `RedisLua`——`LOCK_LUA_SCRIPT`、`UNLOCK_LUA_SCRIPT`、`INCR_*`、`DECR_*`、`DIV_SCRIPT` 等 |
| 发布/订阅注解 | 稳定 | `@RedisChannelTopic` / `@RedisPatternTopic` 与 `MessageListenerAdapter` 监听接口 |
| 键名辅助 | 稳定 | `RedisKey`、`RedisKeyConstant`、`MapUtils`、`RedisOperationException` |

## 3. Requirements & Compatibility

| 要求 | 版本 |
|:---|:---|
| JDK | 8+ |
| Maven | 3.6+ |
| Spring Data Redis | 由 `spring-data-bom` 管理（已在 POM 声明） |
| Spring Framework | 由 `spring-framework-bom` 管理（spring-context、spring-tx） |
| Reactor | reactor-core（响应式模板所需） |
| geodesy | org.gavaghan:geodesy（地理距离计算） |
| Guava | guava（缓存/集合） |

版本线：

| 分支 | JDK | 版本模式 | 说明 |
|:---|:---|:---|:---|
| `feature/1.0.x` | 8 | `1.0.x.*` | 当前开发线；Spring 5.x 时代 |
| `feature/2.0.x` | 17 | `2.0.x.*` | 下一条版本线 |
| `feature/3.0.x` | 21 | `3.0.x.*` | 未来版本线 |

## 4. Architecture & Modules

```
Application code
        |
        v
+------------------------------------+
| RedisOperationTemplate /           |
| ReactiveRedisOperationTemplate     |
|  (key-value operations)            |
+------------------------------------+
        |
        v
+------------------------------------+
| GeoTemplate / ReactiveGeoTemplate  |
|  (geoAdd, distance, circle query)  |
+------------------------------------+
        |
        v
Spring Data Redis (RedisTemplate / ReactiveRedisTemplate)
        |
        v
Redis server
```

本工程为单 jar 模块，关键类：

| 类 | 包 | 职责 |
|:---|:---|:---|
| `RedisOperationTemplate` | `org.springframework.data.redis.core` | 基于 `RedisTemplate<String, Object>` 的高层键值操作 |
| `ReactiveRedisOperationTemplate` | `org.springframework.data.redis.core` | 基于 `ReactiveRedisTemplate<String, Object>` 的响应式操作 |
| `GeoTemplate` / `ReactiveGeoTemplate` | `org.springframework.data.redis.core` | 地理操作与 geodesy 距离辅助 |
| `RedisLua`、`RedisKey`、`RedisKeyConstant`、`MapUtils`、`RedisOperationException` | `io.github.easy4j.redistpl.core` | Lua 脚本、键名常量与工具 |
| `RedisChannelTopic`、`RedisPatternTopic`、`MessageListenerAdapter` | `io.github.easy4j.redistpl.core.annotation` / `.connection` | 发布/订阅主题注解与监听适配器 |

## 5. Installation

制品发布到 easy4j 私有仓库与 GitHub Releases，暂未发布 Maven Central。

Maven：

```xml
<dependency>
    <groupId>io.github.easy4j</groupId>
    <artifactId>spring-data-redis-extension</artifactId>
    <version>1.0.x.20260630-SNAPSHOT</version>
</dependency>
```

Gradle：

```groovy
implementation 'io.github.easy4j:spring-data-redis-extension:1.0.x.20260630-SNAPSHOT'
```

## 6. Quick Start

包装已有的 `RedisTemplate` 并使用操作模板：

```java
import org.springframework.data.redis.core.RedisOperationTemplate;
import org.springframework.data.redis.core.RedisTemplate;

// RedisTemplate<String, Object> redisTemplate = ...; // 由应用创建
RedisOperationTemplate operationTemplate = new RedisOperationTemplate(redisTemplate);

operationTemplate.set("demo:key", "hello");
Object value = operationTemplate.get("demo:key");
System.out.println(value);                             // hello

operationTemplate.expire("demo:key", 60);              // 60 秒
System.out.println(operationTemplate.hasKey("demo:key")); // true
```

预期结果：操作经由被包装的 `RedisTemplate` 执行；序列化遵循所配置 `RedisTemplate` 的序列化器，扩展层提供便捷重载（`set`、`expire`、`hasKey` 等）。

## 7. Configuration

纯库——无配置文件与属性前缀。模板接收应用已配置好的 `RedisTemplate<String, Object>` / `ReactiveRedisTemplate<String, Object>`；连接设置与序列化器由应用自身的 Redis 基础设施配置。

## 8. Core Usage / API

`GeoTemplate` 地理操作：

```java
import org.springframework.data.redis.core.GeoTemplate;

GeoTemplate geo = new GeoTemplate(redisTemplate);

geo.geoAdd("geo:users", 116.397128, 39.916527, "user-1001");
geo.geoAdd("geo:users", 121.473701, 31.230416, "user-1002");

double meters = geo.distanceValue("user-1001", "user-1002");
// 指定距离内的附近用户
geo.getCircleUsersByDistance("user-1001", 500_000);
```

响应式用法：

```java
import org.springframework.data.redis.core.ReactiveRedisOperationTemplate;

ReactiveRedisOperationTemplate reactive = new ReactiveRedisOperationTemplate(reactiveRedisTemplate);

reactive.set("demo:key", "hello")
        .then(reactive.get("demo:key"))
        .subscribe(value -> System.out.println(value)); // hello
```

## 9. Testing & Build

构建：

```bash
mvn clean verify
```

- 构建配置了 JaCoCo Maven 插件：覆盖率报告生成于 `target/site/jacoco/index.html`，并配置了 BUNDLE 行覆盖率 90% 的校验规则（`haltOnFailure=false`，即只报告不阻断构建）；
- 当前仓库本模块暂无单元测试，覆盖率以 JaCoCo 报告为准；
- `central` Maven Profile（`mvn -Pcentral deploy`）附加 GPG 签名、源码包与 Javadoc 包用于发布。

## 10. Versioning & Branches

维护三条并行版本线：

| 分支 | JDK | 版本模式 |
|:---|:---|:---|
| `feature/1.0.x` | 8 | `1.0.x.*` |
| `feature/2.0.x` | 17 | `2.0.x.*` |
| `feature/3.0.x` | 21 | `3.0.x.*` |

维护策略：`1.0.x` 为当前活跃开发线（当前快照 `1.0.x.20260630-SNAPSHOT`）；`2.0.x` 与 `3.0.x` 为面向更新 JDK 的前向移植线。快照按需构建，正式 Release 通过 GitHub Releases 分发。

## 11. Contributing & License

- Fork 仓库并提交 Pull Request；`1.0.x` 版本线保持 JDK 8 兼容；
- Bug 反馈与功能建议通过 GitHub Issues 跟踪；
- 基于 [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) 开源。
