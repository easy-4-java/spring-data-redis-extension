package io.github.easy4j.redistpl.core;

/**
 * Holder of the canonical Redis cache key prefixes used by the
 * {@code spring-data-redis-extension} toolkit.
 *
 * <p>Centralising the prefixes in a single type ensures every business scenario
 * (geo-location, IP region lookup, IP geo lookup, third-party providers such as
 * Baidu and PCOnline) shares a consistent naming convention and never
 * duplicates raw string literals. The constants are intended to be referenced
 * from {@link RedisKey} enum values when generating a full key via
 * {@link RedisKey#getKeyStr(Object...)}.</p>
 *
 * <p>This class is {@code abstract} and cannot be instantiated &mdash; it only
 * exposes {@code public static final} string fields.</p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 3.0.0
 * @see RedisKey
 * @see RedisKey#getKeyStr(Object...)
 */
public abstract class RedisKeyConstant {

	/**
	 * Prefix for cached user geo-location entries ({@code "geo:location"}).
	 *
	 * <p>Backs the {@link RedisKey#GEO_LOCATION_KEY} enum value.</p>
	 */
	public final static String GEO_LOCATION_KEY = "geo:location";

	/**
	 * Prefix for cached IP-to-region mappings ({@code "ip:region"}).
	 *
	 * <p>Backs the {@link RedisKey#IP_REGION_INFO} enum value.</p>
	 */
	public final static String IP_REGION_KEY = "ip:region";

	/**
	 * Prefix for cached IP-to-coordinates mappings ({@code "ip:location"}).
	 *
	 * <p>Backs the {@link RedisKey#IP_LOCATION_INFO} enum value.</p>
	 */
	public final static String IP_LOCATION_KEY = "ip:location";

	/**
	 * Prefix for cached IP coordinates served by the Baidu provider
	 * ({@code "baidu:ip:location"}).
	 *
	 * <p>Backs the {@link RedisKey#IP_LOCATION_BAIDU_INFO} enum value.</p>
	 */
	public final static String IP_BAIDU_LOCATION_KEY = "baidu:ip:location";

	/**
	 * Prefix for cached IP coordinates served by PCOnline
	 * ({@code "pconline:ip:location"}).
	 *
	 * <p>Backs the {@link RedisKey#IP_LOCATION_PCONLINE_INFO} enum value.</p>
	 */
	public final static String IP_PCONLINE_LOCATION_KEY = "pconline:ip:location";

}