package io.github.easy4j.redistpl.core;

import org.springframework.dao.NonTransientDataAccessException;

/**
 * Unchecked exception thrown when a Redis operation performed by one of the
 * {@code spring-data-redis-extension} templates fails irrecoverably.
 *
 * <p>The exception extends Spring's {@link NonTransientDataAccessException} so
 * it is treated as a non-transient failure by Spring's exception translation
 * layer. Retrying the same operation without addressing the underlying cause
 * (for example a serialization mismatch, a connection timeout, or an invalid
 * Lua script) is unlikely to succeed.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 3.0.0
 * @see NonTransientDataAccessException
 */
@SuppressWarnings("serial")
public class RedisOperationException extends NonTransientDataAccessException {

	/**
	 * Builds a new {@link RedisOperationException} that wraps the supplied
	 * cause.
	 *
	 * @param msg   the human-readable detail message; may be {@code null}
	 * @param cause the underlying cause; may be {@code null}
	 */
	public RedisOperationException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * Builds a new {@link RedisOperationException} with only a detail message.
	 *
	 * @param msg the human-readable detail message; may be {@code null}
	 */
	public RedisOperationException(String msg) {
		super(msg);
	}

}