package io.github.easy4j.redistpl.core.annotation;

import java.lang.annotation.*;

/**
 * Redis 频道订阅主题注解。
 * <p>
 * 标注在 {@link org.springframework.data.redis.connection.MessageListener} 实现类上，
 * 用于声明该监听器订阅的 Redis Channel（频道）名称。
 * <p>
 * 配合 Spring 的 {@link org.springframework.data.redis.listener.RedisMessageListenerContainer} 使用，
 * 容器启动时会自动扫描带有此注解的监听器，并将其注册到对应的频道。
 *
 * @author wandl
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RedisChannelTopic {

	/**
	 * Redis 频道名称
	 *
	 * @return 频道名称
	 */
	String value();

}
