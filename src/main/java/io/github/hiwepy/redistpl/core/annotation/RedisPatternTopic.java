package io.github.hiwepy.redistpl.core.annotation;

import java.lang.annotation.*;

/**
 * Redis 模式订阅主题注解。
 * <p>
 * 标注在 {@link org.springframework.data.redis.connection.MessageListener} 实现类上，
 * 用于声明该监听器订阅的 Redis Pattern（模式匹配）主题。
 * <p>
 * 与 {@link RedisChannelTopic} 不同，本注解支持通配符模式匹配（如 {@code news.*}），
 * 可以同时订阅多个匹配的频道。
 *
 * @author wandl
 * @see RedisChannelTopic
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RedisPatternTopic {

	/**
	 * Redis 频道匹配模式，支持通配符（如 {@code news.*}、{@code order:*}）
	 *
	 * @return 频道匹配模式
	 */
	String value();

}
