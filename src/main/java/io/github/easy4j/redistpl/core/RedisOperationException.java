package io.github.easy4j.redistpl.core;

import org.springframework.dao.NonTransientDataAccessException;

/**
 * Redis 操作异常类，用于封装 Redis 操作过程中发生的非瞬态数据访问异常。
 * <p>
 * 继承自 {@link NonTransientDataAccessException}，表示该异常不是由临时性问题引起的，
 * 重试操作通常不会成功。
 *
 * @author wandl
 */
@SuppressWarnings("serial")
public class RedisOperationException extends NonTransientDataAccessException {

	/**
	 * 构造一个包含错误信息和原因的 Redis 操作异常
	 *
	 * @param msg   错误信息
	 * @param cause 异常原因
	 */
	public RedisOperationException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * 构造一个仅包含错误信息的 Redis 操作异常
	 *
	 * @param msg 错误信息
	 */
	public RedisOperationException(String msg) {
		super(msg);
	}

}
