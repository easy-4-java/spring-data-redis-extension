package io.github.easy4j.redistpl.core;

import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * Redis 缓存 Key 生成枚举。
 * <p>
 * 通过枚举 + 函数式接口的方式，为不同业务场景提供统一的 Redis 缓存 Key 生成规则。
 * 每个枚举值对应一种缓存场景，通过 {@link #getKey(Object)} 方法生成完整的缓存 Key。
 * <p>
 * Key 生成格式为：{@code rds:模块:参数1:参数2:...}，各部分之间使用冒号(:)分隔。
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @see RedisKeyConstant
 */
public enum RedisKey {

	/**
	 * 用户坐标缓存
	 */
    GEO_LOCATION_KEY("用户坐标", (userId)->{
		return getKeyStr(RedisKeyConstant.GEO_LOCATION_KEY);
    }),
    /**
     * IP地区编码缓存
     */
    IP_REGION_INFO("用户坐标对应的地区编码缓存", (ip)->{
        return getKeyStr(RedisKeyConstant.IP_REGION_KEY, ip);
    }),
    /**
     * IP坐标缓存
     */
    IP_LOCATION_INFO("用户坐标对应的地理位置缓存", (ip)->{
        return getKeyStr(RedisKeyConstant.IP_LOCATION_KEY, ip);
    }),
    /**
     * IP坐标缓存（百度服务缓存）
     */
    IP_LOCATION_BAIDU_INFO("IP坐标缓存（百度服务缓存）", (ip)->{
        return getKeyStr(RedisKeyConstant.IP_BAIDU_LOCATION_KEY, ip);
    }),
    /**
     * IP坐标缓存（太平洋网络）
     */
    IP_LOCATION_PCONLINE_INFO("IP坐标缓存（太平洋网络）", (ip)->{
        return getKeyStr(RedisKeyConstant.IP_PCONLINE_LOCATION_KEY, ip);
    })

	;

	/** 缓存场景描述 */
	private String desc;
	/** Key 生成函数，接收参数返回完整的 Redis Key */
    private Function<Object, String> function;

    /**
     * 构造枚举实例
     *
     * @param desc     缓存场景描述
     * @param function Key 生成函数
     */
    RedisKey(String desc, Function<Object, String> function) {
        this.desc = desc;
        this.function = function;
    }

    /**
     * 获取缓存场景描述
     *
     * @return 场景描述文本
     */
    public String getDesc() {
		return desc;
	}

    /**
     * 1、获取全名称key
     * @return 无参数组合后的redis缓存key
     */
    public String getKey() {
        return this.function.apply(null);
    }

    /**
     * 1、获取全名称key
     * @param key 缓存key的部分值
     * @return key参数组合后的redis缓存key
     */
    public String getKey(Object key) {
        return this.function.apply(key);
    }

    /** Redis Key 全局前缀 */
    public static String REDIS_PREFIX = "rds";
    /** Redis Key 各部分之间的分隔符 */
    public final static String DELIMITER = ":";

    /**
     * 拼接生成完整的 Redis 缓存 Key。
     * <p>
     * 自动添加全局前缀 {@link #REDIS_PREFIX}，各参数之间使用 {@link #DELIMITER} 分隔，
     * 空值或空白值的参数将被跳过。
     *
     * @param args Key 的各组成部分
     * @return 拼接后的完整 Redis Key，格式如：{@code rds:arg1:arg2:arg3}
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
     * 拼接生成包含线程 ID 的 Redis 缓存 Key。
     * <p>
     * 在指定前缀和参数之间自动插入当前线程 ID，用于线程级别的缓存隔离。
     * 空值或空白值的参数将被跳过。
     *
     * @param prefix Key 前缀
     * @param args   Key 的各组成部分
     * @return 拼接后的 Redis Key，格式如：{@code prefix:threadId:arg1:arg2}
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

    public static void main(String[] args) {
        System.out.println(getKeyStr(233,""));
    }


}
