package io.github.easy4j.redistpl.core;

import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * Enumeration of Redis cache-key generation strategies used across the
 * {@code spring-data-redis-extension} toolkit.
 *
 * <p>Each enum value pairs a human-readable description with a
 * {@link Function} that produces the fully-qualified Redis cache key. The
 * factory methods {@link #getKey()} and {@link #getKey(Object)} simply delegate
 * to that function, while the static helpers {@link #getKeyStr(Object...)} and
 * {@link #getThreadKeyStr(String, Object...)} are the recommended low-level
 * utilities for ad-hoc key composition.</p>
 *
 * <p>The generated key format is
 * {@code rds:&lt;module&gt;:&lt;param1&gt;:&lt;param2&gt;:...} where segments are
 * delimited by {@link #DELIMITER} and a global {@link #REDIS_PREFIX} is added
 * automatically by {@link #getKeyStr(Object...)}. {@code null} or blank
 * arguments are skipped.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 3.0.0
 * @see RedisKeyConstant
 * @see RedisKey#getKeyStr(Object...)
 */
public enum RedisKey {

	/**
	 * Cache for user geo-location coordinates.
	 *
	 * <p>Generated key: {@code rds:geo:location}.</p>
	 */
	GEO_LOCATION_KEY("用户坐标", (userId) -> {
		return getKeyStr(RedisKeyConstant.GEO_LOCATION_KEY);
	}),

	/**
	 * Cache for the region code associated with a user location.
	 *
	 * <p>Generated key: {@code rds:ip:region:<ip>}.</p>
	 */
	IP_REGION_INFO("用户坐标对应的地区编码缓存", (ip) -> {
		return getKeyStr(RedisKeyConstant.IP_REGION_KEY, ip);
	}),

	/**
	 * Cache for the geo-coordinates associated with an IP address.
	 *
	 * <p>Generated key: {@code rds:ip:location:<ip>}.</p>
	 */
	IP_LOCATION_INFO("用户坐标对应的地理位置缓存", (ip) -> {
		return getKeyStr(RedisKeyConstant.IP_LOCATION_KEY, ip);
	}),

	/**
	 * Cache for IP coordinates served by the Baidu third-party provider.
	 *
	 * <p>Generated key: {@code rds:baidu:ip:location:<ip>}.</p>
	 */
	IP_LOCATION_BAIDU_INFO("IP坐标缓存（百度服务缓存）", (ip) -> {
		return getKeyStr(RedisKeyConstant.IP_BAIDU_LOCATION_KEY, ip);
	}),

	/**
	 * Cache for IP coordinates served by the PCOnline provider.
	 *
	 * <p>Generated key: {@code rds:pconline:ip:location:<ip>}.</p>
	 */
	IP_LOCATION_PCONLINE_INFO("IP坐标缓存（太平洋网络）", (ip) -> {
		return getKeyStr(RedisKeyConstant.IP_PCONLINE_LOCATION_KEY, ip);
	})

	;

	/** Human-readable description of the cache scenario. */
	private String desc;

	/** Key-generation function; accepts a parameter and returns the full Redis key. */
	private Function<Object, String> function;

	/**
	 * Build an enum instance.
	 *
	 * @param desc     human-readable description of the cache scenario
	 * @param function key-generation function; never {@code null}
	 */
	RedisKey(String desc, Function<Object, String> function) {
		this.desc = desc;
		this.function = function;
	}

	/**
	 * Returns the human-readable description of this cache scenario.
	 *
	 * @return the description text; never {@code null}
	 */
	public String getDesc() {
		return desc;
	}

	/**
	 * Generates the full Redis key for this scenario using {@code null} as the
	 * single argument to the underlying key-generation function.
	 *
	 * @return the assembled Redis key (typically prefixed with {@link #REDIS_PREFIX})
	 */
	public String getKey() {
		return this.function.apply(null);
	}

	/**
	 * Generates the full Redis key for this scenario by passing the supplied
	 * argument to the underlying key-generation function.
	 *
	 * @param key the cache-key fragment to embed in the final key
	 * @return the assembled Redis key
	 */
	public String getKey(Object key) {
		return this.function.apply(key);
	}

	/**
	 * Global Redis key prefix ({@code "rds"}). Automatically prepended by
	 * {@link #getKeyStr(Object...)}.
	 */
	public static String REDIS_PREFIX = "rds";

	/**
	 * Delimiter separating segments of a generated Redis key ({@code ":"}).
	 */
	public final static String DELIMITER = ":";

	/**
	 * Assembles a complete Redis cache key from the supplied components.
	 *
	 * <p>The {@link #REDIS_PREFIX} is added as the first segment and segments
	 * are joined with {@link #DELIMITER}. {@code null} or blank arguments are
	 * skipped so callers can safely pass through partial data.</p>
	 *
	 * @param args the key segments; {@code null} or blank values are ignored
	 * @return the concatenated key in the form
	 *         {@code rds:arg1:arg2:arg3}; never {@code null}
	 */
	public static String getKeyStr(Object... args) {
		StringJoiner tempKey = new StringJoiner(DELIMITER);
		tempKey.add(REDIS_PREFIX);
		for (Object s : args) {
			if (Objects.isNull(s) || !StringUtils.hasText(s.toString())) {
				continue;
			}
			tempKey.add(s.toString());
		}
		return tempKey.toString();
	}

	/**
	 * Assembles a Redis cache key that includes the current thread id between
	 * the supplied prefix and any additional arguments.
	 *
	 * <p>Useful for thread-local cache isolation. {@code null} or blank
	 * arguments after the thread id are skipped.</p>
	 *
	 * @param prefix the key prefix that will become the first segment
	 * @param args   the key segments following the thread id; {@code null} or
	 *               blank values are ignored
	 * @return the concatenated key in the form
	 *         {@code prefix:threadId:arg1:arg2}; never {@code null}
	 */
	public static String getThreadKeyStr(String prefix, Object... args) {

		StringJoiner tempKey = new StringJoiner(DELIMITER);
		tempKey.add(prefix);
		tempKey.add(String.valueOf(Thread.currentThread().getId()));
		for (Object s : args) {
			if (Objects.isNull(s) || !StringUtils.hasText(s.toString())) {
				continue;
			}
			tempKey.add(s.toString());
		}
		return tempKey.toString();
	}

	/**
	 * Entry point for manual smoke-testing of {@link #getKeyStr(Object...)}.
	 *
	 * @param args ignored command-line arguments
	 */
	public static void main(String[] args) {
		System.out.println(getKeyStr(233,""));
	}


}