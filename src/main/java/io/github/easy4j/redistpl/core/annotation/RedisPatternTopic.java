package io.github.easy4j.redistpl.core.annotation;

import java.lang.annotation.*;

/**
 * Declares the Redis <em>pattern</em> topic a
 * {@link org.springframework.data.redis.connection.MessageListener}
 * implementation subscribes to.
 *
 * <p>Unlike {@link RedisChannelTopic}, which binds a listener to a single
 * fixed channel, this annotation supports wildcard patterns (for example
 * {@code "news.*"} or {@code "order:*"}) so the same listener can receive
 * messages from every channel that matches the pattern.</p>
 *
 * <p>The annotation is intended to be detected at container startup by a
 * scanner that automatically registers the annotated listener with the
 * project's {@link org.springframework.data.redis.listener.RedisMessageListenerContainer}.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 3.0.0
 * @see RedisChannelTopic
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RedisPatternTopic {

	/**
	 * The Redis channel-matching pattern to subscribe to. Supports wildcards
	 * such as {@code "news.*"} and {@code "order:*"}.
	 *
	 * @return the pattern string; never {@code null} or empty when used
	 */
	String value();

}