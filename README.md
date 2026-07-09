# spring-data-redis-extension

基于 Spring Data Redis 的纯 Java 扩展层，承载 Redis 模板、Geo 模板、Reactive 模板和注解，不依赖 Spring Boot Starter。

## Maven

```xml
<dependency>
  <groupId>io.github.hiwepy</groupId>
  <artifactId>spring-data-redis-extension</artifactId>
  <version>3.0.x.20260630-SNAPSHOT</version>
</dependency>
```

## 版本线

| 分支 | 版本前缀 | JDK | 说明 |
|------|----------|-----|------|
| `feature/1.0.x` | `1.0.x.*` | 8 | 对齐 Boot 2.x |
| `feature/2.0.x` | `2.0.x.*` | 17 | 对齐 Boot 3.x |
| `feature/3.0.x` | `3.0.x.*` | 21 | 对齐 Boot 4.x |

## 约束

- 不依赖 `spring-boot-starter-*`
- 仅依赖 `spring-data-redis`、`spring-context`、`spring-tx` 等基础库
- 不包含自动配置类

## License

Apache License 2.0
