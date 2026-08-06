# spring-data-redis-extension

![Java](https://img.shields.io/badge/Java-21-orange) ![License](https://img.shields.io/badge/License-Apache%202.0-blue)

[1. Project Overview](#1-project-overview) | [2. Features & Status](#2-features--status) | [3. Requirements & Compatibility](#3-requirements--compatibility) | [4. Architecture & Modules](#4-architecture--modules) | [5. Installation](#5-installation) | [6. Quick Start](#6-quick-start) | [7. Configuration](#7-configuration) | [8. Core Usage / API](#8-core-usage--api) | [9. Testing & Build](#9-testing--build) | [10. Versioning & Branches](#10-versioning--branches) | [11. Contributing & License](#11-contributing--license)

## 1. Project Overview

`spring-data-redis-extension` ("Redis Operation Template SDK") is a Spring Data Redis companion library, independent of Spring Boot. It wraps the standard `RedisTemplate` / `ReactiveRedisTemplate` with higher-level operation templates, adds geo-distance helpers based on the geodesy library, ships reusable Lua scripts (locking, counters) and pub/sub listener annotations — so application code works against a friendlier API while staying on the plain Spring Framework.

It is a library layer on top of Spring Data Redis — it does not provide Redis connection auto-configuration and is not a Spring Boot starter.

Typical scenarios:

| Scenario | What this module contributes |
|:---|:---|
| High-level key/value operations | `RedisOperationTemplate` (set/get, expire, hasKey, rename, type, ...) |
| Reactive (WebFlux-style) operations | `ReactiveRedisOperationTemplate` (Mono/Flux based) |
| Geo distance and nearby-user queries | `GeoTemplate` / `ReactiveGeoTemplate` |
| Redis lock / counter Lua scripts | `RedisLua` (LOCK, UNLOCK, INCR, DECR, ...) |
| Pub/sub listener annotations | `RedisChannelTopic` / `RedisPatternTopic` + `MessageListenerAdapter` |
| Key/utility constants | `RedisKey`, `RedisKeyConstant`, `MapUtils`, `RedisOperationException` |

## 2. Features & Status

Project status: pre-release development line (`1.0.x.*` snapshots); public API is still stabilizing until the first tagged release.

| Capability | Status | Notes |
|:---|:---|:---|
| Key-value operation template | Stable | `RedisOperationTemplate extends AbstractOperations<String, Object>` — `set`, `get`, `getString`, `setNx`, `expire`, `expireAt`, `getExpire`, `hasKey`, `keys`, `persist`, `randomKey`, `rename`, `renameIfAbsent`, `type`, `setRange`, ... |
| Reactive operation template | Stable | `ReactiveRedisOperationTemplate` — `expire`, `expireAt`, `getExpire`, `hasKey`, `getKey`, `getVagueKey`, raw-key helpers |
| Geo template | Stable | `GeoTemplate` — `geoAdd`, `distance`, `distanceValue`, `getCircleUsersByDistance`, `getCircleUsersByRadius`, plus pure-Java `getDistance` (sphere/ellipsoid) via geodesy |
| Reactive geo template | Stable | `ReactiveGeoTemplate` (reactive variants of the geo helpers) |
| Lua scripts | Stable | `RedisLua` — `LOCK_LUA_SCRIPT`, `UNLOCK_LUA_SCRIPT`, `INCR_*`, `DECR_*`, `DIV_SCRIPT`, ... |
| Pub/sub annotations | Stable | `@RedisChannelTopic` / `@RedisPatternTopic` and the `MessageListenerAdapter` listener interface |
| Key helpers | Stable | `RedisKey`, `RedisKeyConstant`, `MapUtils`, `RedisOperationException` |

## 3. Requirements & Compatibility

| Requirement | Version |
|:---|:---|
| JDK | 21+ |
| Maven | 3.6+ |
| Spring Data Redis | managed by `spring-data-bom` (declared in the POM) |
| Spring Framework | managed by `spring-framework-bom` (spring-context, spring-tx) |
| Reactor | reactor-core (for the reactive templates) |
| geodesy | org.gavaghan:geodesy (geo distance calculations) |
| Guava | guava (caches/collections) |

Version lines:

| Branch | JDK | Version pattern | Notes |
|:---|:---|:---|:---|
| `feature/1.0.x` | 8 | `1.0.x.*` | Current line; Spring 5.x era |
| `feature/2.0.x` | 17 | `2.0.x.*` | Next line |
| `feature/3.0.x` | 21 | `3.0.x.*` | Future line |

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

The project is a single jar module. Key classes:

| Class | Package | Responsibility |
|:---|:---|:---|
| `RedisOperationTemplate` | `org.springframework.data.redis.core` | High-level key/value operations over `RedisTemplate<String, Object>` |
| `ReactiveRedisOperationTemplate` | `org.springframework.data.redis.core` | Reactive operations over `ReactiveRedisTemplate<String, Object>` |
| `GeoTemplate` / `ReactiveGeoTemplate` | `org.springframework.data.redis.core` | Geo operations and geodesy distance helpers |
| `RedisLua`, `RedisKey`, `RedisKeyConstant`, `MapUtils`, `RedisOperationException` | `io.github.easy4j.redistpl.core` | Lua scripts, key constants and utilities |
| `RedisChannelTopic`, `RedisPatternTopic`, `MessageListenerAdapter` | `io.github.easy4j.redistpl.core.annotation` / `.connection` | Pub/sub topic annotations and listener adapter |

## 5. Installation

Artifacts are published to the easy4j private repository and GitHub Releases; the project is not yet on Maven Central.

Maven:

```xml
<dependency>
    <groupId>io.github.easy4j</groupId>
    <artifactId>spring-data-redis-extension</artifactId>
    <version>3.0.x.x.20260630-SNAPSHOT</version>
</dependency>
```

Gradle:

```groovy
implementation 'io.github.easy4j:spring-data-redis-extension:3.0.x.x.20260630-SNAPSHOT'
```

## 6. Quick Start

Wrap an existing `RedisTemplate` and use the operation template:

```java
import org.springframework.data.redis.core.RedisOperationTemplate;
import org.springframework.data.redis.core.RedisTemplate;

// RedisTemplate<String, Object> redisTemplate = ...; // created by the application
RedisOperationTemplate operationTemplate = new RedisOperationTemplate(redisTemplate);

operationTemplate.set("demo:key", "hello");
Object value = operationTemplate.get("demo:key");
System.out.println(value);                             // hello

operationTemplate.expire("demo:key", 60);              // 60 seconds
System.out.println(operationTemplate.hasKey("demo:key")); // true
```

Expected result: the operations execute against the wrapped `RedisTemplate`; serialization follows the configured `RedisTemplate` serializers, and the extension adds the convenient overloads (`set`, `expire`, `hasKey`, ...).

## 7. Configuration

Pure library — no configuration files or property prefixes. The templates receive an already-configured `RedisTemplate<String, Object>` / `ReactiveRedisTemplate<String, Object>` from the application; connection settings and serializers are configured by the application's own Redis infrastructure.

## 8. Core Usage / API

Geo operations with `GeoTemplate`:

```java
import org.springframework.data.redis.core.GeoTemplate;

GeoTemplate geo = new GeoTemplate(redisTemplate);

geo.geoAdd("geo:users", 116.397128, 39.916527, "user-1001");
geo.geoAdd("geo:users", 121.473701, 31.230416, "user-1002");

double meters = geo.distanceValue("user-1001", "user-1002");
// nearby users within the given distance
geo.getCircleUsersByDistance("user-1001", 500_000);
```

Reactive usage:

```java
import org.springframework.data.redis.core.ReactiveRedisOperationTemplate;

ReactiveRedisOperationTemplate reactive = new ReactiveRedisOperationTemplate(reactiveRedisTemplate);

reactive.set("demo:key", "hello")
        .then(reactive.get("demo:key"))
        .subscribe(value -> System.out.println(value)); // hello
```

## 9. Testing & Build

Build:

```bash
mvn clean verify
```

- The build is configured with the JaCoCo Maven plugin: a coverage report is generated at `target/site/jacoco/index.html` and a rule checks the bundle line coverage against a 90% minimum (`haltOnFailure=false`, so the check reports but does not fail the build).
- The repository currently ships no unit tests for this module; coverage is tracked via the JaCoCo report.
- The `central` Maven profile (`mvn -Pcentral deploy`) attaches GPG signatures, sources and Javadoc jars for publishing.

## 10. Versioning & Branches

Three parallel version lines are maintained:

| Branch | JDK | Version pattern |
|:---|:---|:---|
| `feature/1.0.x` | 8 | `1.0.x.*` |
| `feature/2.0.x` | 17 | `2.0.x.*` |
| `feature/3.0.x` | 21 | `3.0.x.*` |

Maintenance policy: the `1.0.x` line is the actively developed line (current snapshot `3.0.x.x.20260630-SNAPSHOT`); `2.0.x` and `3.0.x` are forward porting lines targeting newer JDKs. Snapshots are built on demand; tagged releases are distributed via GitHub Releases.

## 11. Contributing & License

- Fork the repository and open a pull request; keep the `1.0.x` line compatible with JDK 8.
- Bug reports and feature requests are tracked via GitHub Issues.
- Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
