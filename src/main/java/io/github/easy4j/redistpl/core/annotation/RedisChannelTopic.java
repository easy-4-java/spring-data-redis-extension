package io.github.easy4j.redistpl.core.annotation;

import java.lang.annotation.*;

/**
 * Declares the Redis <em>channel</em> topic a
 * {@link org.springframework.data.redis.connection.MessageListener}
 * implementation subscribes to.
 *
 * <p>When the surrounding
 * {@link org.springframework.data.redis.listener.RedisMessageListenerContainer}
 * starts, the framework scans for listeners annotated with this type and
 * registers each one against the channel named by {@link #value()}.</p>
 *
 * <p>For wildcard matching use {@link RedisPatternTopic} instead.</p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 3.0.0
 * @see RedisPatternTopic
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RedisChannelTopic {

	/**
	 * The exact Redis channel name to subscribe to.
	 *
	 * @return the channel name; never {@code null} or empty when used
	 */
	String value();

}