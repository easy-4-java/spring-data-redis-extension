package io.github.hiwepy.redistpl.core;

/**
 * Redis 缓存 Key 常量定义抽象类。
 * <p>
 * 集中管理各类业务场景对应的 Redis 缓存 Key 前缀常量，
 * 配合 {@link RedisKey} 枚举使用，实现缓存 Key 的统一规范管理。
 *
 * @author wandl
 */
public abstract class RedisKeyConstant {

	/**
	 * 用户坐标缓存
	 */
	public final static String GEO_LOCATION_KEY = "geo:location";
	/**
	 * IP坐标缓存
	 */
	public final static String IP_REGION_KEY = "ip:region";
	/**
	 * IP坐标缓存
	 */
	public final static String IP_LOCATION_KEY = "ip:location";
	/**
	 * IP坐标缓存（百度服务缓存）
	 */
	public final static String IP_BAIDU_LOCATION_KEY = "baidu:ip:location";
	/**
	 * IP坐标缓存（太平洋网络）
	 */
	public final static String IP_PCONLINE_LOCATION_KEY = "pconline:ip:location";

}
