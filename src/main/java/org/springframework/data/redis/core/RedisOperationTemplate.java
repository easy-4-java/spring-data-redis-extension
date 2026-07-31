package org.springframework.data.redis.core;

import io.github.easy4j.redistpl.core.MapUtils;
import io.github.easy4j.redistpl.core.RedisKey;
import io.github.easy4j.redistpl.core.RedisLua;
import io.github.easy4j.redistpl.core.RedisOperationException;
import io.github.easy4j.redistpl.core.annotation.RedisChannelTopic;
import io.github.easy4j.redistpl.core.annotation.RedisPatternTopic;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisZSetCommands.*;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Redis 操作模板类，基于 {@link RedisTemplate} 的二次封装。
 * <p>
 * 提供了更丰富、更便捷的 Redis 操作方法，涵盖以下数据结构：
 * <ul>
 *   <li>String（字符串）：set/get/incr/decr 等</li>
 *   <li>Hash（哈希）：hSet/hGet/hmSet/hmGet 等</li>
 *   <li>List（列表）：lPush/rPush/lPop/rPop/lRange 等</li>
 *   <li>Set（集合）：sAdd/sRemove/sMembers/sDiff 等</li>
 *   <li>ZSet（有序集合）：zAdd/zRange/zRank 等</li>
 *   <li>Geo（地理位置）：geoAdd/geoDistance/geoRadius 等</li>
 *   <li>Lock（分布式锁）：基于 Lua 脚本的 setNx + pexpire 实现</li>
 *   <li>Pub/Sub（发布订阅）：基于注解的频道/模式订阅</li>
 * </ul>
 * <p>
 * 相比原生 {@link RedisOperationTemplate}，本模板额外提供了：
 * <ul>
 *   <li>类型安全的 get 方法（getString/getDouble/getLong/getInteger）</li>
 *   <li>批量操作的 mGet 系列方法</li>
 *   <li>基于 Lua 脚本的库存扣减（incr/decr/div）操作</li>
 *   <li>分布式锁的加锁/解锁方法</li>
 *   <li>大 Key 的 Scan 扫描删除方法</li>
 * </ul>
 *
 * @author wandl
 * @see RedisLua
 * @see RedisKey
 */
@SuppressWarnings({"unchecked","rawtypes"})
@Slf4j
public class RedisOperationTemplate extends AbstractOperations<String, Object> {

	/** 加锁成功标识 */
	private static final Long LOCK_SUCCESS = 1L;
	/** 锁已过期标识 */
    private static final Long LOCK_EXPIRED = -1L;

	/** 加锁 Lua 脚本：setnx + pexpire 原子操作 */
    private static final RedisScript<Long> LOCK_LUA_SCRIPT = RedisScript.of(RedisLua.LOCK_LUA_SCRIPT, Long.class );
	/** 解锁 Lua 脚本：先比较 value 再删除，保证只有持锁者能解锁 */
    private static final RedisScript<Long> UNLOCK_LUA_SCRIPT = RedisScript.of(RedisLua.UNLOCK_LUA_SCRIPT, Long.class );

	/** String 类型库存增加 Lua 脚本 */
    public static final RedisScript<Long> INCR_SCRIPT = RedisScript.of(RedisLua.INCR_SCRIPT, Long.class);
	/** String 类型库存扣减 Lua 脚本 */
    public static final RedisScript<Long> DECR_SCRIPT = RedisScript.of(RedisLua.DECR_SCRIPT, Long.class);
	/** String 类型库存除法 Lua 脚本 */
	public static final RedisScript<Long> DIV_SCRIPT = RedisScript.of(RedisLua.DIV_SCRIPT, Long.class);

	/** String 类型浮点数增加 Lua 脚本 */
    public static final RedisScript<Double> INCR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.INCR_BYFLOAT_SCRIPT, Double.class);
	/** String 类型浮点数减少 Lua 脚本 */
    public static final RedisScript<Double> DECR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.DECR_BYFLOAT_SCRIPT, Double.class);

	/** Hash 类型库存增加 Lua 脚本 */
    public static final RedisScript<Long> HINCR_SCRIPT = RedisScript.of(RedisLua.HINCR_SCRIPT, Long.class);
	/** Hash 类型库存扣减 Lua 脚本 */
    public static final RedisScript<Long> HDECR_SCRIPT = RedisScript.of(RedisLua.HDECR_SCRIPT, Long.class);
	/** Hash 类型库存除法 Lua 脚本 */
	public static final RedisScript<Long> HDIV_SCRIPT = RedisScript.of(RedisLua.HDIV_SCRIPT, Long.class);

	/** Hash 类型浮点数增加 Lua 脚本 */
    public static final RedisScript<Double> HINCR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.HINCR_BYFLOAT_SCRIPT, Double.class);
	/** Hash 类型浮点数减少 Lua 脚本 */
    public static final RedisScript<Double> HDECR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.HDECR_BYFLOAT_SCRIPT, Double.class);

	/** 对象转字符串转换函数 */
    public static final Function<Object, String> TO_STRING = member -> Objects.toString(member, null);

	/** 对象转 Double 转换函数，支持 Double 和数值型字符串 */
    public static final Function<Object, Double> TO_DOUBLE = member -> {
		if(Objects.isNull(member)) {
			return null;
		}
		return member instanceof Double ? (Double) member : new BigDecimal(member.toString()).doubleValue();
	};

	/** 对象转 Long 转换函数，支持 Long 和数值型字符串 */
	public static final Function<Object, Long> TO_LONG = member -> {
		if(Objects.isNull(member)) {
			return null;
		}
		return member instanceof Long ? (Long) member : new BigDecimal(member.toString()).longValue();
	};

	/** 对象转 Integer 转换函数，支持 Integer 和数值型字符串 */
	public static final Function<Object, Integer> TO_INTEGER = member -> {
		if(Objects.isNull(member)) {
			return null;
		}
		return member instanceof Integer ? (Integer) member : new BigDecimal(member.toString()).intValue();
	};

	/** 底层 RedisTemplate 实例 */
	private final RedisTemplate<String, Object> redisTemplate;

	/**
	 * 构造 RedisOperationTemplate 实例
	 *
	 * @param redisTemplate 底层 RedisTemplate 实例
	 */
	public RedisOperationTemplate(RedisTemplate<String, Object> redisTemplate) {
		super(redisTemplate);
		this.redisTemplate = redisTemplate;
	}

	/**
	 * 获取底层 RedisTemplate 实例
	 *
	 * @return RedisTemplate 实例
	 */
	public RedisTemplate<String, Object> getRedisTemplate() {
		return redisTemplate;
	}

	// =============================Serializer============================

	/**
	 * 将 Key 对象序列化为字节数组
	 *
	 * @param key Key 对象
	 * @return 序列化后的字节数组
	 */
	public byte[] getRawKey(Object key) {
		return rawKey(key);
	}

	/**
	 * 将字符串 Key 序列化为字节数组
	 *
	 * @param key 字符串 Key
	 * @return 序列化后的字节数组
	 */
	public byte[] getRawString(String key) {
		return rawString(key);
	}

	/**
	 * 将 Value 对象序列化为字节数组
	 *
	 * @param value Value 对象
	 * @return 序列化后的字节数组
	 */
	public byte[] getRawValue(Object value) {
		return rawValue(value);
	}

	/**
	 * 批量将 Value 对象序列化为字节数组
	 *
	 * @param values Value 对象集合
	 * @return 序列化后的字节数组
	 */
	public <V> byte[][] getRawValues(Collection<V> values) {
		return rawValues(values);
	}

	/**
	 * 将 Hash Key 对象序列化为字节数组
	 *
	 * @param hashKey Hash Key 对象
	 * @return 序列化后的字节数组
	 */
	public <HK> byte[] getRawHashKey(HK hashKey) {
		return rawHashKey(hashKey);
	}

	/**
	 * 批量将 Hash Key 对象序列化为字节数组
	 *
	 * @param hashKeys Hash Key 对象数组
	 * @return 序列化后的字节数组
	 */
	public <HK> byte[][] getRawHashKeys(HK... hashKeys) {
		return rawHashKeys(hashKeys);
	}

	/**
	 * 将 Hash Value 对象序列化为字节数组
	 *
	 * @param value Hash Value 对象
	 * @return 序列化后的字节数组
	 */
	public <HV> byte[] getRawHashValue(HV value) {
		return rawHashValue(value);
	}

	/**
	 * 将两个 Key 序列化为字节数组
	 *
	 * @param key      第一个 Key
	 * @param otherKey 第二个 Key
	 * @return 包含两个序列化结果的字节数组
	 */
	public byte[][] getRawKeys(String key, String otherKey) {
		return rawKeys(key, otherKey);
	}

	/**
	 * 批量将 Key 集合序列化为字节数组
	 *
	 * @param keys Key 集合
	 * @return 序列化后的字节数组
	 */
	public byte[][] getRawKeys(Collection<String> keys) {
		return rawKeys(keys);
	}

	/**
	 * 将一个 Key 和一个 Key 集合序列化为字节数组
	 *
	 * @param key  主 Key
	 * @param keys Key 集合
	 * @return 序列化后的字节数组
	 */
	public byte[][] getRawKeys(String key, Collection<String> keys) {
		return rawKeys(key, keys);
	}

	// =============================Deserialize============================

	/**
	 * 将字节数组集合反序列化为 Value 对象集合
	 *
	 * @param rawValues 字节数组集合
	 * @return 反序列化后的 Value 对象集合
	 */
	public Set<Object> getDeserializeValues(Set<byte[]> rawValues) {
		return deserializeValues(rawValues);
	}

	/**
	 * 将 Tuple 字节数组集合反序列化为 TypedTuple 对象集合
	 *
	 * @param rawValues Tuple 字节数组集合
	 * @return 反序列化后的 TypedTuple 对象集合
	 */
	public Set<TypedTuple<Object>> getDeserializeTupleValues(Set<Tuple> rawValues) {
		return deserializeTupleValues(rawValues);
	}

	/**
	 * 将 Tuple 字节数组列表反序列化为 TypedTuple 对象列表
	 *
	 * @param rawValues Tuple 字节数组列表
	 * @return 反序列化后的 TypedTuple 对象列表
	 */
	public List<TypedTuple<Object>> getDeserializeTupleValues(List<Tuple> rawValues) {
		return deserializeTupleValues(rawValues);
	}

	/**
	 * 将单个 Tuple 反序列化为 TypedTuple 对象
	 *
	 * @param tuple Tuple 对象
	 * @return 反序列化后的 TypedTuple 对象
	 */
	public TypedTuple<Object> getDeserializeTuple(Tuple tuple) {
		return deserializeTuple(tuple);
	}

	/**
	 * 将 TypedTuple 对象集合序列化为 Tuple 字节数组集合
	 *
	 * @param values TypedTuple 对象集合
	 * @return 序列化后的 Tuple 字节数组集合
	 */
	public Set<Tuple> getRawTupleValues(Set<TypedTuple<Object>> values) {
		return rawTupleValues(values);
	}

	/**
	 * 将字节数组列表反序列化为 Value 对象列表
	 *
	 * @param rawValues 字节数组列表
	 * @return 反序列化后的 Value 对象列表
	 */
	public List<Object> getDeserializeValues(List<byte[]> rawValues) {
		return deserializeValues(rawValues);
	}

	/**
	 * 将字节数组集合反序列化为 Hash Key 对象集合
	 *
	 * @param rawKeys 字节数组集合
	 * @return 反序列化后的 Hash Key 对象集合
	 */
	public <T> Set<T> getDeserializeHashKeys(Set<byte[]> rawKeys) {
		return deserializeHashKeys(rawKeys);
	}

	/**
	 * 将字节数组列表反序列化为 Hash Value 对象列表
	 *
	 * @param rawValues 字节数组列表
	 * @return 反序列化后的 Hash Value 对象列表
	 */
	public <T> List<T> getDeserializeHashValues(List<byte[]> rawValues) {
		return deserializeHashValues(rawValues);
	}

	/**
	 * 将字节数组 Map 反序列化为 Hash Key-Value Map
	 *
	 * @param entries 字节数组 Map
	 * @return 反序列化后的 Hash Key-Value Map
	 */
	public <HK, HV> Map<HK, HV> getDeserializeHashMap(@Nullable Map<byte[], byte[]> entries) {
		return deserializeHashMap(entries);
	}

	/**
	 * 将字节数组反序列化为 Key 字符串
	 *
	 * @param value 字节数组
	 * @return 反序列化后的 Key 字符串
	 */
	public String getDeserializeKey(byte[] value) {
		return deserializeKey(value);
	}

	/**
	 * 将字节数组集合反序列化为 Key 字符串集合
	 *
	 * @param keys 字节数组集合
	 * @return 反序列化后的 Key 字符串集合
	 */
	public Set<String> getDeserializeKeys(Set<byte[]> keys) {
		return deserializeKeys(keys);
	}

	/**
	 * 将字节数组反序列化为 Value 对象
	 *
	 * @param value 字节数组
	 * @return 反序列化后的 Value 对象
	 */
	public Object getDeserializeValue(byte[] value) {
		return deserializeValue(value);
	}

	/**
	 * 将字节数组反序列化为字符串
	 *
	 * @param value 字节数组
	 * @return 反序列化后的字符串
	 */
	public String getDeserializeString(byte[] value) {
		return deserializeString(value);
	}

	/**
	 * 将字节数组反序列化为 Hash Key 对象
	 *
	 * @param value 字节数组
	 * @return 反序列化后的 Hash Key 对象
	 */
	public <HK> HK getDeserializeHashKey(byte[] value) {
		return deserializeHashKey(value);
	}

	/**
	 * 将字节数组反序列化为 Hash Value 对象
	 *
	 * @param value 字节数组
	 * @return 反序列化后的 Hash Value 对象
	 */
	public <HV> HV getDeserializeHashValue(byte[] value) {
		return deserializeHashValue(value);
	}

	/**
	 * 将字节数组形式的地理位置结果反序列化为对象形式
	 *
	 * @param source 字节数组形式的地理位置结果
	 * @return 反序列化后的地理位置结果
	 */
	public GeoResults<GeoLocation<Object>> getDeserializeGeoResults(GeoResults<GeoLocation<byte[]>> source) {
		return deserializeGeoResults(source);
	}

	// =============================Keys============================

	/**
	 * 判断key是否存在
	 *
	 * @param key 键
	 * @return true 存在 false不存在
	 */
	public Boolean hasKey(String key) {
		try {
			return getOperations().hasKey(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 指定缓存失效时间
	 *
	 * @param key     键
	 * @param seconds 时间(秒)
	 * @return 过期是否设置成功
	 */
	public Boolean expire(String key, long seconds) {
		try {
			return getOperations().expire(key, seconds, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 指定缓存失效时间
	 *
	 * @param key     键
	 * @param timeout 时间
	 * @return 过期是否设置成功
	 */
	public Boolean expire(String key, Duration timeout) {
		try {
			return getOperations().expire(key, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 指定缓存失效时间
	 *
	 * @param key    键
	 * @param date 	 时间
	 * @return 过期是否设置成功
	 */
	public Boolean expireAt(String key, Date date) {
		try {
			return getOperations().expireAt(key, date);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回 key 的剩余的过期时间
	 *
	 * @param key 键 不能为null
	 * @return 时间(秒) 返回0代表为永久有效
	 */
	public Long getExpire(String key) {
		try {
			return getOperations().getExpire(key, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回 key 的剩余的过期时间
	 *
	 * @param key 键 不能为null
	 * @param unit 缓存过期时间单位
	 * @return 时间(秒) 返回0代表为永久有效
	 */
	public Long getExpire(String key, TimeUnit unit) {
		try {
			return getOperations().getExpire(key, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 模糊匹配缓存中的 Key
	 *
	 * @param pattern Key 匹配模式（支持通配符，如 {@code user:*}）
	 * @return 匹配的 Key 集合
	 */
	public Set<String> keys(String pattern) {
		try {
			if (Objects.isNull(pattern)) {
				return null;
			}
			Set<String> keys = this.scan(pattern);
			return keys;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除 key 的过期时间，key 将持久保持
	 *
	 * @param key 缓存key
	 * @return 持久化结果
	 */
	public Boolean persist(String key) {
		return redisTemplate.persist(key);
	}

	/**
	 * 从当前数据库中随机返回一个 key
	 *
	 * @return 随机key
	 */
	public String randomKey() {
		return redisTemplate.randomKey();
	}

	/**
	 * 修改 key 的名称
	 *
	 * @param oldKey 旧缓存key
	 * @param newKey 新缓存key
	 */
	public void rename(String oldKey, String newKey) {
		redisTemplate.rename(oldKey, newKey);
	}

	/**
	 * 仅当 newkey 不存在时，将 oldKey 改名为 newkey
	 *
	 * @param oldKey 旧缓存key
	 * @param newKey 新缓存key
	 * @return 是否修改成功
	 */
	public Boolean renameIfAbsent(String oldKey, String newKey) {
		return redisTemplate.renameIfAbsent(oldKey, newKey);
	}

	/**
	 * 返回 key 所储存的值的类型
	 *
	 * @param key 缓存key
	 * @return 缓存类型
	 */
	public DataType type(String key) {
		return redisTemplate.type(key);
	}

	// ============================String=============================

	/**
	 * 普通缓存放入
	 *
	 * @param key   键
	 * @param value 值
	 * @return true成功 false失败
	 */
	public boolean set(String key, Object value) {
		try {
			getOperations().opsForValue().set(key, value);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始
	 *
	 * @param key 缓存key
	 * @param value 缓存值
	 * @param offset 从指定位置开始覆写
	 */
	public void setRange(String key, Object value, long offset) {
		redisTemplate.opsForValue().set(key, value, offset);
	}

	/**
	 * 普通缓存放入并设置时间
	 *
	 * @param key     缓存key
	 * @param value   缓存值
	 * @param seconds 时间(秒) time要&gt;=0 如果time小于等于0 将设置无限期
	 * @return true成功 false 失败
	 */
	public boolean set(String key, Object value, long seconds) {
		try {
			if (seconds > 0) {
				getOperations().opsForValue().set(key, value, seconds, TimeUnit.SECONDS);
				return true;
			} else {
				return set(key, value);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 普通缓存放入并设置时间
	 *
	 * @param key     缓存key
	 * @param value   缓存值
	 * @param timeout 时间
	 * @return true成功 false 失败
	 */
	public boolean set(String key, Object value, Duration timeout) {
		if (Objects.isNull(timeout) || timeout.isNegative()) {
			return false;
		}
		try {
			getOperations().opsForValue().set(key, value, timeout);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 Key 不存在时设置缓存值（SETNX）
	 *
	 * @param key   键
	 * @param value 值
	 * @return true 设置成功（Key 不存在），false 设置失败（Key 已存在）
	 */
	public boolean setNx(String key, Object value) {
		try {
			return getOperations().opsForValue().setIfAbsent(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 1、仅可用于低并发功能，高并发严禁使用此方法
	 *
	 * @param key     并发锁
	 * @param value   锁key（务必能区别不同线程的请求）
	 * @param milliseconds 锁过期时间（单位：毫秒）
	 * @return 是否设置成功
	 */
	public boolean setNx(String key, Object value, long milliseconds) {
		try {
			return getOperations().opsForValue().setIfAbsent(key, value, Duration.ofMillis(milliseconds));
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 2、仅可用于低并发功能，高并发严禁使用此方法
	 *
	 * @param key     并发锁
	 * @param value   锁key（务必能区别不同线程的请求）
	 * @param timeout 锁过期时间
	 * @param unit    锁过期时间单位
	 * @return 是否设置成功
	 */
	public boolean setNx(String key, Object value, long timeout, TimeUnit unit) {
		try {
			return getOperations().opsForValue().setIfAbsent(key, value, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 1、仅可用于低并发功能，高并发严禁使用此方法
	 *
	 * @param key     并发锁
	 * @param value   锁key（务必能区别不同线程的请求）
	 * @param timeout 锁过期时间
	 * @return 是否设置成功
	 */
	public boolean setNx(String key, Object value, Duration timeout) {
		try {
			return getOperations().opsForValue().setIfAbsent(key, value, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 普通缓存获取
	 *
	 * @param key 键
	 * @return 值
	 */
	public Object get(String key) {
		try {
			return getOperations().opsForValue().get(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取缓存值并转为 String 类型
	 *
	 * @param key 键
	 * @return 字符串值，不存在时返回 null
	 */
	public String getString(String key) {
		return getFor(key, TO_STRING);
	}

	/**
	 * 获取缓存值并转为 String 类型，不存在时返回默认值
	 *
	 * @param key        键
	 * @param defaultVal 默认值
	 * @return 字符串值，不存在时返回默认值
	 */
	public String getString(String key, String defaultVal) {
		String rtVal = getString(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取缓存值并转为 Double 类型
	 *
	 * @param key 键
	 * @return Double 值，不存在时返回 null
	 */
	public Double getDouble(String key) {
		return getFor(key, TO_DOUBLE);
	}

	/**
	 * 获取缓存值并转为 Double 类型，不存在时返回默认值
	 *
	 * @param key        键
	 * @param defaultVal 默认值
	 * @return Double 值，不存在时返回默认值
	 */
	public Double getDouble(String key, double defaultVal) {
		Double rtVal = getDouble(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取缓存值并转为 Long 类型
	 *
	 * @param key 键
	 * @return Long 值，不存在时返回 null
	 */
	public Long getLong(String key) {
		return getFor(key, TO_LONG);
	}

	/**
	 * 获取缓存值并转为 Long 类型，不存在时返回默认值
	 *
	 * @param key        键
	 * @param defaultVal 默认值
	 * @return Long 值，不存在时返回默认值
	 */
	public Long getLong(String key, long defaultVal) {
		Long rtVal = getLong(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取缓存值并转为 Integer 类型
	 *
	 * @param key 键
	 * @return Integer 值，不存在时返回 null
	 */
	public Integer getInteger(String key) {
		return getFor(key, TO_INTEGER);
	}

	/**
	 * 获取缓存值并转为 Integer 类型，不存在时返回默认值
	 *
	 * @param key        键
	 * @param defaultVal 默认值
	 * @return Integer 值，不存在时返回默认值
	 */
	public Integer getInteger(String key, int defaultVal) {
		Integer rtVal = getInteger(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取缓存值并按指定类型转换
	 *
	 * @param key   键
	 * @param clazz 目标类型
	 * @param <T>   目标类型
	 * @return 转换后的值，不存在时返回 null
	 */
	public <T> T getFor(String key, Class<T> clazz) {
		return getFor(key, member -> clazz.cast(member));
	}

	/**
	 * 根据key获取值，并按Function函数进行转换
	 *
	 * @param key    键
	 * @param mapper 对象转换函数
	 * @param <T>   指定的类型
	 * @return xx
	 */
	public <T> T getFor(String key, Function<Object, T> mapper) {
		Object obj = this.get(key);
		if (Objects.nonNull(obj)) {
			return mapper.apply(obj);
		}
		return null;
	}

	/**
	 * 获取缓存值的子字符串（GETRANGE）
	 *
	 * @param key   键
	 * @param start 起始偏移量（含）
	 * @param end   结束偏移量（含），-1 表示到末尾
	 * @return 子字符串
	 */
	public String getRange(String key, long start, long end) {
		try {
			return redisTemplate.opsForValue().get(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 设置新值并返回旧值（GETSET），结果转为 String
	 *
	 * @param key   键
	 * @param value 新值
	 * @return 旧值（字符串形式）
	 */
	public String getStringAndSet(String key, Object value) {
		return getForAndSet(key, value, TO_STRING);
	}

	/**
	 * 设置新值并返回旧值（GETSET），结果转为 Double
	 *
	 * @param key   键
	 * @param value 新值
	 * @return 旧值（Double 形式）
	 */
	public Double getDoubleAndSet(String key, Object value) {
		return getForAndSet(key, value, TO_DOUBLE);
	}

	/**
	 * 设置新值并返回旧值（GETSET），结果转为 Long
	 *
	 * @param key   键
	 * @param value 新值
	 * @return 旧值（Long 形式）
	 */
	public Long getLongAndSet(String key, Object value) {
		return getForAndSet(key, value, TO_LONG);
	}

	/**
	 * 设置新值并返回旧值（GETSET），结果转为 Integer
	 *
	 * @param key   键
	 * @param value 新值
	 * @return 旧值（Integer 形式）
	 */
	public Integer getIntegerAndSet(String key, Object value) {
		return getForAndSet(key, value, TO_INTEGER);
	}

	/**
	 * 设置新值并返回旧值（GETSET），按指定方式转换结果
	 */
	public <T> T getForAndSet(String key, Object value, Function<Object, T> mapper) {
		Object obj = this.getAndSet(key, value);
		if (Objects.nonNull(obj)) {
			return mapper.apply(obj);
		}
		return null;
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 *
	 * @param key 缓存key
	 * @param value 新的值
	 * @return 设置前的值
	 */
	public Object getAndSet(String key, Object value) {
		try {
			return redisTemplate.opsForValue().getAndSet(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据key表达式获取缓存
	 *
	 * @param pattern 键表达式
	 * @return 值
	 */
	public List<Object> mGet(String pattern) {
		try {
			if (!StringUtils.hasText(pattern)) {
				return Lists.newArrayList();
			}
			Set<String> keys = this.keys(pattern);
			return getOperations().opsForValue().multiGet(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量获取缓存值并转为 Double 类型
	 */
	public List<Double> mGetDouble(Collection keys) {
		return mGetFor(keys, TO_DOUBLE);
	}

	/**
	 * 批量获取缓存值并转为 Long 类型
	 */
	public List<Long> mGetLong(Collection keys) {
		return mGetFor(keys, TO_LONG);
	}

	/**
	 * 批量获取缓存值并转为 Integer 类型
	 */
	public List<Integer> mGetInteger(Collection keys) {
		return mGetFor(keys, TO_INTEGER);
	}

	/**
	 * 批量获取缓存值并转为 String 类型
	 */
	public List<String> mGetString(Collection keys) {
		return mGetFor(keys, TO_STRING);
	}

	/**
	 * 批量获取缓存值并按指定方式转换
	 */
	public <T> List<T> mGetFor(Collection keys, Class<T> clazz) {
		return mGetFor(keys, member -> clazz.cast(member));
	}

	/**
	 * 批量获取缓存值并按指定方式转换
	 */
	public <T> List<T> mGetFor(Collection keys, Function<Object, T> mapper) {
		List<Object> members = this.mGet(keys);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 批量获取缓存值
	 *
	 * @param keys 键集合
	 * @return 值
	 */
	public List<Object> mGet(Collection keys) {
		try {
			if(CollectionUtils.isEmpty(keys)) {
				return Lists.newArrayList();
			}
			return getOperations().opsForValue().multiGet(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量获取缓存值
	 */
	public List<Object> mGet(Collection<Object> keys, String redisPrefix) {
		try {
			if(CollectionUtils.isEmpty(keys)) {
				return Lists.newArrayList();
			}
			Collection newKeys = keys.stream().map(key -> RedisKey.getKeyStr(redisPrefix, key.toString())).collect(Collectors.toList());
			return getOperations().opsForValue().multiGet(newKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key   键
	 * @param delta 要增加几(&gt;=0)
	 * @return 增加指定数值后的值
	 */
	public Long incr(String key, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getOperations().opsForValue().increment(key, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key     键
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的值
	 */
	public Long incr(String key, long delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForValue().increment(key, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增操作
	 */
	public Long incr(String key, long delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForValue().increment(key, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key   键
	 * @param delta 要增加几(&gt;=0)
	 * @return 增加指定数值后的值
	 */
	public Double incr(String key, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getOperations().opsForValue().increment(key, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key     键
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的值
	 */
	public Double incr(String key, double delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getOperations().opsForValue().increment(key, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增操作
	 */
	public Double incr(String key, double delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getOperations().opsForValue().increment(key, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key   键
	 * @param delta 要减少几(&gt;=0)
	 * @return 减少指定数值后的值
	 */
	public Long decr(String key, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getOperations().opsForValue().increment(key, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key     键
	 * @param delta   要减少几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 减少指定数值后的值
	 */
	public Long decr(String key, long delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForValue().increment(key, -delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减操作
	 */
	public Long decr(String key, long delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForValue().increment(key, -delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key   键
	 * @param delta 要减少几(&gt;=0)
	 * @return 减少指定数值后的值
	 */
	public Double decr(String key, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getOperations().opsForValue().increment(key, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key     键
	 * @param delta   要减少几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 减少指定数值后的值
	 */
	public Double decr(String key, double delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Double increment = getOperations().opsForValue().increment(key, -delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减操作
	 */
	public Double decr(String key, double delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Double increment = getOperations().opsForValue().increment(key, -delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 删除缓存
	 *
	 * @param keys 可以传一个值 或多个
	 */
	public void del(String... keys) {
		try {
			if (keys != null && keys.length > 0) {
				if (keys.length == 1) {
					getOperations().delete(keys[0]);
				} else {
					getOperations().delete(Stream.of(keys).collect(Collectors.toList()));
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 按模式匹配删除缓存 Key
	 */
	public Long delPattern(String pattern) {
		try {
			Set<String> keys = this.keys(pattern);
			if(CollectionUtils.isEmpty(keys)){
				return 0L;
			}
			return getOperations().delete(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * scan 实现
	 *
	 * @param pattern  表达式
	 * @param count 数量限制
	 * @return 扫描结果
	 */
	public Set<String> scan(String pattern, long count) {
		ScanOptions options = ScanOptions.scanOptions().count(count).match(pattern).build();
		return this.scan(options);
	}

	/**
	 * 扫描匹配的 Key
	 */
	public Set<String> scan(String pattern) {
		ScanOptions options = ScanOptions.scanOptions().match(pattern).build();
		return this.scan(options);
	}

	/**
	 * 扫描匹配的 Key
	 */
	public Set<String> scan(ScanOptions options) {
		return this.getOperations().execute((RedisConnection redisConnection) -> {
			try (Cursor<byte[]> cursor = redisConnection.scan(options)) {
				Set<String> keysTmp = new HashSet<>();
				while (cursor.hasNext()) {
					keysTmp.add(deserializeString(cursor.next()));
				}
				return keysTmp;
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RedisOperationException(e.getMessage());
			}
		});
	}

	// ===============================List=================================

	/**
	 * 获取list缓存的内容
	 *
	 * @param key   键
	 * @param start 开始
	 * @param end   结束 0 到 -1代表所有值
	 * @return list 集合
	 */
	public List<Object> lRange(String key, long start, long end) {
		try {
			return getOperations().opsForList().range(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 List 指定范围的元素并转为 String
	 */
	public List<String> lRangeString(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_STRING);
	}

	/**
	 * 获取 List 指定范围的元素并转为 Double
	 */
	public List<Double> lRangeDouble(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_DOUBLE);
	}

	/**
	 * 获取 List 指定范围的元素并转为 Long
	 */
	public List<Long> lRangeLong(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_LONG);
	}

	/**
	 * 获取 List 指定范围的元素并转为 Integer
	 */
	public List<Integer> lRangeInteger(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_INTEGER);
	}

	/**
	 * 获取指定类型的list缓存
	 *
	 * @param key   键
	 * @param start 开始下标
	 * @param end   结束下标, 0 到 -1代表所有值
	 * @param clazz 指定的类型
	 * @param <T>   指定的类型
	 * @return 类型转换后端集合
	 */
	public <T> List<T> lRangeFor(String key, long start, long end, Class<T> clazz) {
		return lRangeFor(key, start, end, member -> clazz.cast(member));
	}

	/**
	 * 获取list缓存的，并指定转换器
	 * @param key   键
	 * @param start 开始下标
	 * @param end   结束下标, 0 到 -1代表所有值
	 * @param mapper 对象转换函数
	 * @param <T>   指定的类型
	 * @return 类型转换后端集合
	 */
	public <T> List<T> lRangeFor(String key, long start, long end, Function<Object, T> mapper) {
		List<Object> members = this.lRange(key, start, end);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 通过索引 获取list中的值
	 *
	 * @param key   键
	 * @param index 索引 index&gt;=0时， 0 表头，1 第二个元素，依次类推；index&lt;0时，-1，表尾，-2倒数第二个元素，依次类推
	 * @return 所以所在位置元素
	 */
	public Object lIndex(String key, long index) {
		try {
			return getOperations().opsForList().index(key, index);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 通过索引获取 List 中的元素并转为 String
	 */
	public String lIndexString(String key, long index) {
		return lIndexFor(key, index, TO_STRING);
	}

	/**
	 * 通过索引获取 List 中的元素并转为 String，不存在时返回默认值
	 */
	public String glIndexString(String key, long index, String defaultVal) {
		String rtVal = lIndexString(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 通过索引获取 List 中的元素并转为 Double
	 */
	public Double lIndexDouble(String key, long index) {
		return lIndexFor(key, index, TO_DOUBLE);
	}

	/**
	 * 通过索引获取 List 中的元素并转为 Double
	 */
	public Double lIndexDouble(String key, long index, double defaultVal) {
		Double rtVal = lIndexDouble(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 通过索引获取 List 中的元素并转为 Long
	 */
	public Long lIndexLong(String key, long index) {
		return lIndexFor(key, index, TO_LONG);
	}

	/**
	 * 通过索引获取 List 中的元素并转为 Long
	 */
	public Long lIndexLong(String key, long index, long defaultVal) {
		Long rtVal = lIndexLong(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 通过索引获取 List 中的元素并转为 Integer
	 */
	public Integer lIndexInteger(String key, long index) {
		return lIndexFor(key, index, TO_INTEGER);
	}

	/**
	 * 通过索引获取 List 中的元素并转为 Integer
	 */
	public Integer lIndexInteger(String key, long index, int defaultVal) {
		Integer rtVal = lIndexInteger(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 通过索引获取 List 中的元素并按指定方式转换
	 */
	public <T> T lIndexFor(String key, long index, Function<Object, T> mapper) {
		Object member = lIndex(key, index);
		if (Objects.nonNull(member)) {
			return mapper.apply(member);
		}
		return null;
	}

	/**
	 * 向 List 左侧添加不重复元素
	 */
	public <V> Long lLeftPushDistinct(String key, V value) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = rawValue(value);
				redisConnection.lRem(rawKey, 0, rawValue);
				redisConnection.lPush(rawKey, rawValue);
				return null;
			}, this.valueSerializer());
			return (Long) result.get(1);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 List 左侧（头部）添加元素
	 */
	public <V> Long lLeftPush(String key, V value) {
		return this.lLeftPush(key, value, 0);
	}

	/**
	 * 向 List 左侧（头部）添加元素
	 */
	public <V> Long lLeftPush(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lLeftPushAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = getOperations().opsForList().leftPush(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 List 左侧（头部）添加元素
	 */
	public <V> Long lLeftPush(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lLeftPushAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = getOperations().opsForList().leftPush(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量向 List 左侧添加元素
	 */
	public <V> Long lLeftPushAll(String key, Collection<V> values) {
		try {
			Long rt = getOperations().opsForList().leftPushAll(key, values.toArray());
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量向 List 左侧添加元素
	 */
	public <V> Long lLeftPushAll(String key, Collection<V> values, long seconds) {
		try {
			Long rt = getOperations().opsForList().leftPushAll(key, values.toArray());
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量向 List 左侧添加元素
	 */
	public <V> Long lLeftPushAll(String key, Collection<V> values, Duration timeout) {
		try {
			Long rt = getOperations().opsForList().leftPushAll(key, values.toArray());
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时向左侧添加元素
	 */
	public <V> Long lLeftPushx(String key, V value) {
		return this.lLeftPushx(key, value, 0);
	}

	/**
	 * 仅当 List 存在时向左侧添加元素
	 */
	public <V> Long lLeftPushx(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lLeftPushxAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = getOperations().opsForList().leftPushIfPresent(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时向左侧添加元素
	 */
	public <V> Long lLeftPushx(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lLeftPushxAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = getOperations().opsForList().leftPushIfPresent(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时批量向左侧添加元素
	 */
	public <V> Long lLeftPushxAll(String key, Collection<V> values, long seconds) {
		try {
			long rt = values.stream().map(value -> getOperations().opsForList().leftPushIfPresent(key, value)).count();
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时批量向左侧添加元素
	 */
	public <V> Long lLeftPushxAll(String key, Collection<V> values, Duration timeout) {
		try {
			long rt = values.stream().map(value -> getOperations().opsForList().leftPushIfPresent(key, value)).count();
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 左侧（头部）弹出元素
	 */
	public Object lLeftPop(String key) {
		try {
			return getOperations().opsForList().leftPop(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 左侧弹出元素并从列表中移除
	 */
	public <V> Object lLeftPopAndLrem(String key) {
		try {
			return getOperations().execute((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = redisConnection.lPop(rawKey);
				redisConnection.lRem(rawKey, 0, rawValue);
				return deserializeValue(rawValue);
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 左侧（头部）弹出元素
	 */
	public Object lLeftPop(String key, long timeout, TimeUnit unit) {
		try {
			return getOperations().opsForList().leftPop(key, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 左侧（头部）弹出元素
	 */
	public Object lLeftPop(String key, Duration timeout) {
		try {
			return getOperations().opsForList().leftPop(key, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从list左侧取count个元素并移除已经去除的元素
	 *
	 * @param key 缓存key
	 * @param count 去除元素的个数
	 * @return 被移除元素列表
	 */
	public List<Object> lLeftPop(String key, Integer count) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				redisConnection.lRange(rawKey, 0, count - 1);
				redisConnection.lTrim(rawKey, count, -1);
				return null;
			}, this.valueSerializer());
			return (List<Object>) result.get(0);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 左侧（头部）弹出元素
	 */
	public <T> List<T> lLeftPop(String key, Integer count, Class<T> clazz) {
		try {
			List<Object> range = this.lLeftPop(key, count);
			List<T> result = range.stream().map(member -> clazz.cast(member)).collect(Collectors.toList());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 List 右侧添加不重复元素
	 */
	public <V> Long lRightPushDistinct(String key, V value) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = rawValue(value);
				redisConnection.lRem(rawKey, 0, rawValue);
				redisConnection.rPush(rawKey, rawValue);
				return null;
			}, this.valueSerializer());
			return (Long) result.get(1);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key   键
	 * @param value 值
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPush(String key, V value) {
		return this.lRightPush(key, value, 0);
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key     键
	 * @param value   值
	 * @param seconds 时间(秒)
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPush(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lRightPushAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = getOperations().opsForList().rightPush(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 List 右侧（尾部）添加元素
	 */
	public <V> Long lRightPush(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lRightPushAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = getOperations().opsForList().rightPush(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量向 List 右侧添加元素
	 */
	public <V> Long lRightPushAll(String key, Collection<V> values) {
		try {
			return getOperations().opsForList().rightPushAll(key, values.toArray());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量向 List 右侧添加元素
	 */
	public <V> Long lRightPushAll(String key, Collection<V> values, long seconds) {
		try {
			Long rt = getOperations().opsForList().rightPushAll(key, values.toArray());
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量向 List 右侧添加元素
	 */
	public <V> Long lRightPushAll(String key, Collection<V> values, Duration timeout) {
		try {
			Long rt = getOperations().opsForList().rightPushAll(key, values.toArray());
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key   键
	 * @param value 值
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPushx(String key, V value) {
		return this.lRightPushx(key, value, 0);
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key     键
	 * @param value   值
	 * @param seconds 时间(秒)
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPushx(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lRightPushxAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = getOperations().opsForList().rightPushIfPresent(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时向右侧添加元素
	 */
	public <V> Long lRightPushx(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lRightPushxAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = getOperations().opsForList().rightPushIfPresent(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时批量向右侧添加元素
	 */
	public <V> Long lRightPushxAll(String key, Collection<V> values, long seconds) {
		try {
			long rt = values.stream().map(value -> getOperations().opsForList().rightPushIfPresent(key, value)).count();
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 List 存在时批量向右侧添加元素
	 */
	public <V> Long lRightPushxAll(String key, Collection<V> values, Duration timeout) {
		try {
			long rt = values.stream().map(value -> getOperations().opsForList().rightPushIfPresent(key, value)).count();
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 右侧（尾部）弹出元素
	 */
	public Object lRightPop(String key) {
		try {
			return getOperations().opsForList().rightPop(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 右侧弹出元素并从列表中移除
	 */
	public <V> Object lRightPopAndLrem(String key) {
		try {
			return getOperations().execute((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = redisConnection.rPop(rawKey);
				redisConnection.lRem(rawKey, 0, rawValue);
				return deserializeValue(rawValue);
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 右侧（尾部）弹出元素
	 */
	public Object lRightPop(String key, long timeout, TimeUnit unit) {
		try {
			return getOperations().opsForList().rightPop(key, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从 List 右侧（尾部）弹出元素
	 */
	public Object lRightPop(String key, Duration timeout) {
		try {
			return getOperations().opsForList().rightPop(key, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从list右侧取count个元素并移除已经去除的元素
	 *	1、Redis Ltrim 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
	 *  2、下标 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * @param key 缓存key
	 * @param count 移除元素个数
	 * @return 右侧移除的元素集合
	 */
	public List<Object> lRightPop(String key, Integer count) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				redisConnection.lRange(rawKey, -(count - 1), -1);
				redisConnection.lTrim(rawKey, 0, -(count - 1));
				return null;
			}, this.valueSerializer());
			return (List<Object>) result.get(0);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从源 List 右侧弹出元素并添加到目标 List 左侧（RPOPLPUSH）
	 */
	public Object lRightPopAndLeftPush(String sourceKey, String destinationKey) {
		try {
			return getOperations().opsForList().rightPopAndLeftPush(sourceKey, destinationKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从源 List 右侧弹出元素并添加到目标 List 左侧（RPOPLPUSH）
	 */
	public Object lRightPopAndLeftPush(String sourceKey, String destinationKey, long timeout, TimeUnit unit) {
		try {
			return getOperations().opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从源 List 右侧弹出元素并添加到目标 List 左侧（RPOPLPUSH）
	 */
	public Object lRightPopAndLeftPush(String sourceKey, String destinationKey, Duration timeout) {
		try {
			return getOperations().opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据索引修改list中的某条数据
	 *
	 * @param key   键
	 * @param index 索引
	 * @param value 值
	 * @return 元素是否设置成功
	 */
	public boolean lSet(String key, long index, Object value) {
		try {
			getOperations().opsForList().set(key, index, value);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取list缓存的长度
	 *
	 * @param key 键
	 * @return list集合的元素个数
	 */
	public long lSize(String key) {
		try {
			return getOperations().opsForList().size(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 修剪 List，仅保留指定区间内的元素
	 */
	public boolean lTrim(String key, long start, long end) {
		try {
			getOperations().opsForList().trim(key, start, end);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除N个值为value
	 *
	 * @param key   键
	 * @param count 移除多少个
	 * @param value 值
	 * @return 移除的个数
	 */
	public Long lRem(String key, long count, Object value) {
		try {
			return getOperations().opsForList().remove(key, count, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ================================Hash=================================

	/**
	 * hash递减
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要减少记(小于0)
	 * @return 减少指定数值后的结果
	 */
	public Long hDecr(String key, String hashKey, int delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getOperations().opsForHash().increment(key, hashKey, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递减
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要减少记(&gt;=0)
	 * @return 减少指定数值后的结果
	 */
	public Long hDecr(String key, String hashKey, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getOperations().opsForHash().increment(key, hashKey, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递减
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要减少记(&gt;=0)
	 * @return 减少指定数值后的结果
	 */
	public Double hDecr(String key, String hashKey, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getOperations().opsForHash().increment(key, hashKey, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 删除hash表中的值
	 *
	 * @param key      键 不能为null
	 * @param hashKeys 项 可以使多个 不能为null
	 */
	public void hDel(String key, Object... hashKeys) {
		try {
			getOperations().opsForHash().delete(key, hashKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Hash删除: hscan + hdel
	 *
	 * @param bigHashKey hash key
	 */
	public void hDel(String bigHashKey) {
		try {
			this.hScan(bigHashKey, (entry) -> {
				this.hDel(bigHashKey, entry.getKey());
			});
			this.del(bigHashKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取hashKey对应的指定键值
	 *
	 * @param key     键
	 * @param hashKey hash键
	 * @return 对应的键值
	 */
	public Object hGet(String key, String hashKey) {
		try {
			return getOperations().opsForHash().get(key, hashKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Hash 字段值
	 */
	public <V> V hGet(String key, String hashKey, V defaultVal) {
		try {
			Object rtVal = getOperations().opsForHash().get(key, hashKey);
			return Objects.nonNull(rtVal) ? (V) rtVal : defaultVal;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Hash 字段值并转为 String
	 */
	public String hGetString(String key, String hashKey) {
		return hGetFor(key, hashKey, TO_STRING);
	}

	/**
	 * 获取 Hash 字段值并转为 String
	 */
	public String hGetString(String key, String hashKey, String defaultVal) {
		String rtVal = hGetString(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取 Hash 字段值并转为 Double
	 */
	public Double hGetDouble(String key, String hashKey) {
		return hGetFor(key, hashKey, TO_DOUBLE);
	}

	/**
	 * 获取 Hash 字段值并转为 Double
	 */
	public Double hGetDouble(String key, String hashKey, double defaultVal) {
		Double rtVal = hGetDouble(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取 Hash 字段值并转为 Long
	 */
	public Long hGetLong(String key, String hashKey) {
		return hGetFor(key, hashKey, TO_LONG);
	}

	/**
	 * 获取 Hash 字段值并转为 Long
	 */
	public Long hGetLong(String key, String hashKey, long defaultVal) {
		Long rtVal = hGetLong(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取 Hash 字段值并转为 Integer
	 */
	public Integer hGetInteger(String key, String hashKey) {
		return hGetFor(key, hashKey, TO_INTEGER);
	}

	/**
	 * 获取 Hash 字段值并转为 Integer
	 */
	public Integer hGetInteger(String key, String hashKey, int defaultVal) {
		Integer rtVal = hGetInteger(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取 Hash 字段值并按指定方式转换
	 */
	public <T> T hGetFor(String key, String hashKey, Class<T> clazz) {
		return hGetFor(key, hashKey, member -> clazz.cast(member));
	}

	/**
	 * 获取 Hash 字段值并按指定方式转换
	 */
	public <T> T hGetFor(String key, String hashKey, Function<Object, T> mapper) {
		Object rt = this.hGet(key, hashKey);
		return Objects.nonNull(rt) ? mapper.apply(rt) : null;
	}

	/**
	 * 获取 Hash 字段值并转为 String
	 */
	public List<String> hGetString(Collection<Object> keys, String hashKey) {
		return hGetFor(keys, hashKey, TO_STRING);
	}

	/**
	 * 获取 Hash 字段值并转为 Double
	 */
	public List<Double> hGetDouble(Collection<Object> keys, String hashKey) {
		return hGetFor(keys, hashKey, TO_DOUBLE);
	}

	/**
	 * 获取 Hash 字段值并转为 Long
	 */
	public List<Long> hGetLong(Collection<Object> keys, String hashKey) {
		return hGetFor(keys, hashKey, TO_LONG);
	}

	/**
	 * 获取 Hash 字段值并转为 Integer
	 */
	public List<Integer> hGetInteger(Collection<Object> keys, String hashKey) {
		return hGetFor(keys, hashKey, TO_INTEGER);
	}

	/**
	 * 获取 Hash 字段值并按指定方式转换
	 */
	public <T> List<T> hGetFor(Collection<Object> keys, String hashKey, Class<T> clazz) {
		return hGetFor(keys, hashKey, member -> clazz.cast(member));
	}

	/**
	 * 获取 Hash 字段值并按指定方式转换
	 */
	public <T> List<T> hGetFor(Collection<Object> keys, String hashKey, Function<Object, T> mapper) {
		List<Object> members = this.hGet(keys, hashKey);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 获取 Hash 字段值
	 */
	public List<Object> hGet(Collection<Object> keys, String hashKey) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					connection.hGet(rawKey(key), rawHashKey(hashKey));
				});
				return null;
			}, this.valueSerializer());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Hash 字段值
	 */
	public List<Object> hGet(Collection<Object> keys, String redisPrefix, String hashKey) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(RedisKey.getKeyStr(redisPrefix, String.valueOf(key)));
					connection.hGet(rawKey, rawHashKey(hashKey));
				});
				return null;
			}, this.valueSerializer());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 判断hash表中是否有该项的值
	 *
	 * @param key     键 不能为null
	 * @param hashKey 项 不能为null
	 * @return true 存在 false不存在
	 */
	public boolean hHasKey(String key, String hashKey) {
		try {
			return getOperations().opsForHash().hasKey(key, hashKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取hashKey对应的所有键值
	 *
	 * @param key 键
	 * @return 对应的多个键值
	 */
	public Map<String, Object> hmGet(String key) {
		try {
			HashOperations<String, String, Object> opsForHash = getOperations().opsForHash();
			return opsForHash.entries(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Hash 中所有字段和值
	 */
	public List<Map<String, Object>> hmGet(Collection<String> keys) {
		if (CollectionUtils.isEmpty(keys)) {
			return Lists.newArrayList();
		}
		return keys.parallelStream().map(key -> {
			return this.hmGet(key);
		}).collect(Collectors.toList());
	}

	/**
	 * 获取 Hash 中所有字段和值
	 */
	public List<Map<String, Object>> hmGet(Collection<String> keys, String redisPrefix) {
		if (CollectionUtils.isEmpty(keys)) {
			return Lists.newArrayList();
		}
		return keys.parallelStream().map(key -> {
			return this.hmGet(RedisKey.getKeyStr(redisPrefix, key));
		}).collect(Collectors.toList());
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值
	 */
	public List<Object> hMultiGet(String key, Collection<Object> hashKeys) {
		try {
			return getOperations().opsForHash().multiGet(key, hashKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量获取多个 Hash 字段值
	 */
	public Map<String, Object> hmMultiGet(String key, Collection<Object> hashKeys) {
		try {
			List<Object> result = getOperations().opsForHash().multiGet(key, hashKeys);
			Map<String, Object> ans = new HashMap<>(hashKeys.size());
			int index = 0;
			for (Object hashKey : hashKeys) {
				ans.put(hashKey.toString(), result.get(index));
				index++;
			}
			return ans;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量获取多个 Hash 字段值
	 */
	public List<Map<String, Object>> hmMultiGet(Collection<String> keys, Collection<Object> hashKeys) {
		if (CollectionUtils.isEmpty(keys) || CollectionUtils.isEmpty(hashKeys)) {
			return Lists.newArrayList();
		}
		return keys.parallelStream().map(key -> {
			return this.hmMultiGet(key, hashKeys);
		}).collect(Collectors.toList());
	}

	/**
	 * 批量获取多个 Hash 字段值
	 */
	public Map<String, Map<String, Object>> hmMultiGet(Collection<String> keys, String identityHashKey,
			Collection<Object> hashKeys) {
		if (CollectionUtils.isEmpty(keys) || CollectionUtils.isEmpty(hashKeys)) {
			return Maps.newHashMap();
		}
		return keys.parallelStream().map(key -> {
			return this.hmMultiGet(key, hashKeys);
		}).collect(Collectors.toMap(kv -> MapUtils.getString(kv, identityHashKey), Function.identity()));
	}

	/**
	 * 批量获取多个 Hash 的所有字段和值
	 */
	public List<Map<String, Object>> hmMultiGetAll(Collection<Object> keys) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(String.valueOf(key));
					connection.hGetAll(rawKey);
				});
				return null;
			}, this.valueSerializer());
			return result.stream().map(mapper -> (Map<String, Object>) mapper).collect(Collectors.toList());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值
	 */
	public List<Object> hMultiGet(Collection<Object> keys, String hashKey) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection connection) -> {
				byte[] rawHashKey = rawHashKey(hashKey);
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(String.valueOf(key));
					connection.hGet(rawKey, rawHashKey);
				});
				return null;
			}, this.valueSerializer());
			return result.stream().collect(Collectors.toList());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值并转为 String
	 */
	public List<String> hMultiGetString(Collection<Object> keys, String hashKey) {
		return hMultiGetFor(keys, hashKey, TO_STRING);
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值并转为 Double
	 */
	public List<Double> hMultiGetDouble(Collection<Object> keys, String hashKey) {
		return hMultiGetFor(keys, hashKey, TO_DOUBLE);
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值并转为 Long
	 */
	public List<Long> hMultiGetLong(Collection<Object> keys, String hashKey) {
		return hMultiGetFor(keys, hashKey, TO_LONG);
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值并转为 Integer
	 */
	public List<Integer> hMultiGetInteger(Collection<Object> keys, String hashKey) {
		return hMultiGetFor(keys, hashKey, TO_INTEGER);
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值并按指定方式转换
	 */
	public <T> List<T> hMultiGetFor(Collection<Object> keys, String hashKey, Class<T> clazz) {
		return hMultiGetFor(keys, hashKey, member -> clazz.cast(member));
	}

	/**
	 * 批量获取多个 Key 的同一 Hash 字段值并按指定方式转换
	 */
	public <T> List<T> hMultiGetFor(Collection<Object> keys, String hashKey, Function<Object, T> mapper) {
		List<Object> members = this.hMultiGet(keys, hashKey);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 批量获取多个 Hash 的所有字段和值
	 */
	public List<Map<String, Object>> hmMultiGetAll(Collection<Object> keys, String redisPrefix) {
		try {
			List<Object> result = getOperations().executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(RedisKey.getKeyStr(redisPrefix, String.valueOf(key)));
					connection.hGetAll(rawKey);
				});
				return null;
			}, this.valueSerializer());
			return result.stream().map(mapper -> (Map<String, Object>) mapper).collect(Collectors.toList());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 为多个 Key 设置相同的 Hash 字段值
	 */
	public boolean hmMultiSet(String key, Collection<Object> hashKeys, Object value) {
		if (CollectionUtils.isEmpty(hashKeys) || !StringUtils.hasText(key)) {
			return false;
		}
		try {
			getOperations().executePipelined((RedisConnection connection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawHashValue = rawHashValue(value);
				for (Object hashKey : hashKeys) {
					connection.hSet(rawKey, rawHashKey(hashKey), rawHashValue);
				}
				return null;
			});
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * HashSet
	 *
	 * @param key 键
	 * @param map 对应多个键值
	 * @return true 成功 false 失败
	 */
	public boolean hmSet(String key, Map<String, Object> map) {
		try {
			getOperations().opsForHash().putAll(key, map);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * HashSet 并设置时间
	 *
	 * @param key     键
	 * @param map     对应多个键值
	 * @param seconds 时间(秒)
	 * @return true成功 false失败
	 */
	public boolean hmSet(String key, Map<String, Object> map, long seconds) {
		try {
			getOperations().opsForHash().putAll(key, map);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 批量设置 Hash 字段值
	 */
	public boolean hmSet(String key, Map<String, Object> map, Duration timeout) {
		try {
			getOperations().opsForHash().putAll(key, map);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 扫描 Hash 中的字段和值
	 */
	public void hScan(String bigHashKey, Consumer<Entry<Object,Object>> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).build();
		this.hScan(bigHashKey, options).forEachRemaining(consumer);
	}

	/**
	 * 扫描 Hash 中的字段和值
	 */
	public void hScan(String bigHashKey, String pattern, Consumer<Entry<Object,Object>> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build();
		this.hScan(bigHashKey, options).forEachRemaining(consumer);
	}

	/**
	 * 扫描 Hash 中的字段和值
	 */
	public void hScan(String bigHashKey, ScanOptions options, Consumer<Entry<Object,Object>> consumer) {
		this.hScan(bigHashKey, options).forEachRemaining(consumer);
	}

	/**
	 * 扫描 Hash 中的字段和值
	 */
	public Cursor<Entry<Object, Object>> hScan(String bigHashKey, ScanOptions options) {
		return  getOperations().opsForHash().scan(bigHashKey, options);
	}

	/**
	 * 向一张hash表中放入数据,如果不存在将创建
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param value   值
	 * @return true 成功 false失败
	 */
	public boolean hSet(String key, String hashKey, Object value) {
		try {
			getOperations().opsForHash().put(key, hashKey, value);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向一张hash表中放入数据,如果不存在将创建
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param value   值
	 * @param seconds    时间(秒) 注意:如果已存在的hash表有时间,这里将会替换原有的时间
	 * @return true 成功 false失败
	 */
	public boolean hSet(String key, String hashKey, Object value, long seconds) {
		try {
			getOperations().opsForHash().put(key, hashKey, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 设置 Hash 字段值
	 */
	public boolean hSet(String key, String hashKey, Object value, Duration timeout) {
		try {
			getOperations().opsForHash().put(key, hashKey, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 仅当 Hash 字段不存在时设置值
	 */
	public boolean hSetNX(String key, String hashKey, Object value) {
		try {
			return getOperations().opsForHash().putIfAbsent(key, hashKey, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取所有哈希表中的字段
	 *
	 * @param key 缓存key
	 * @return  哈希缓存的所有key
	 */
	public Set<Object> hKeys(String key) {
		try {
			return getOperations().opsForHash().keys(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash的大小
	 *
	 * @param key 缓存key
	 * @return  hash的大小
	 */
	public Long hSize(String key) {
		try {
			return getOperations().opsForHash().size(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取哈希表中所有值
	 *
	 * @param key 缓存key
	 * @return 哈希表中所有值
	 */
	public List<Object> hValues(String key) {
		try {
			return getOperations().opsForHash().values(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @return 增加指定数值后的结果
	 */
	public Long hIncr(String key, String hashKey, int delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getOperations().opsForHash().increment(key, hashKey, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的结果
	 */
	public Long hIncr(String key, String hashKey, int delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForHash().increment(key, hashKey, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Hash 字段值递增
	 */
	public Long hIncr(String key, String hashKey, int delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForHash().increment(key, hashKey, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @return 增加指定数值后的新数值
	 */
	public Long hIncr(String key, String hashKey, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getOperations().opsForHash().increment(key, hashKey, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的新数值
	 */
	public Long hIncr(String key, String hashKey, long delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForHash().increment(key, hashKey, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @param timeout 过期时长（秒）
	 * @return 增加指定数值后的新数值
	 */
	public Long hIncr(String key, String hashKey, long delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getOperations().opsForHash().increment(key, hashKey, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @return 增加指定数值后的新数值
	 */
	public Double hIncr(String key, String hashKey, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getOperations().opsForHash().increment(key, hashKey, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Hash 字段值递增
	 */
	public Double hIncr(String key, String hashKey, double delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getOperations().opsForHash().increment(key, hashKey, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Hash 字段值递增
	 */
	public Double hIncr(String key, String hashKey, double delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getOperations().opsForHash().increment(key, hashKey, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ============================Set=============================

	/**
	 * 将数据放入set缓存
	 *
	 * @param key    键
	 * @param values 值 可以是多个
	 * @return 成功个数
	 */
	public Long sAdd(String key, Object... values) {
		try {
			return getOperations().opsForSet().add(key, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 Set 中添加元素并设置过期时间
	 */
	public Long sAddAndExpire(String key, long seconds, Object... values) {
		try {
			Long rt = getOperations().opsForSet().add(key, values);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 Set 中添加元素并设置过期时间
	 */
	public Long sAddAndExpire(String key, Duration timeout, Object... values) {
		try {
			Long rt = getOperations().opsForSet().add(key, values);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Set删除: sscan + srem
	 *
	 * @param bigSetKey 键
	 * @return 批量删除结果
	 */
	public Boolean sDel(String bigSetKey) {
		try {
			this.sScan(bigSetKey, (value) -> {
				getOperations().opsForSet().remove(bigSetKey, deserializeValue(value));
			});
			return getOperations().delete(bigSetKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据key获取Set中的所有值
	 *
	 * @param key 键
	 * @return Set 集合的所有元素
	 */
	public Set<Object> sGet(String key) {
		try {
			return getOperations().opsForSet().members(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 中所有元素并转为 String
	 */
	public Set<String> sGetString(String key) {
		return sGetFor(key, TO_STRING);
	}

	/**
	 * 获取 Set 中所有元素并转为 Double
	 */
	public Set<Double> sGetDouble(String key) {
		return sGetFor(key, TO_DOUBLE);
	}

	/**
	 * 获取 Set 中所有元素并转为 Long
	 */
	public Set<Long> sGetLong(String key) {
		return sGetFor(key, TO_LONG);
	}

	/**
	 * 获取 Set 中所有元素并转为 Integer
	 */
	public Set<Integer> sGetInteger(String key) {
		return sGetFor(key, TO_INTEGER);
	}

	/**
	 * 获取 Set 中所有元素并按指定方式转换
	 */
	public <T> Set<T> sGetFor(String key, Class<T> clazz) {
		return sGetFor(key, member -> clazz.cast(member));
	}

	/**
	 * 获取 Set 中所有元素并按指定方式转换
	 */
	public <T> Set<T> sGetFor(String key, Function<Object, T> mapper) {
		Set<Object> members = this.sGet(key);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 获取两个key的不同value
	 *
	 * @param key      键
	 * @param otherKey 键
	 * @return 返回key中和otherKey的不同数据
	 */
	public Set<Object> sDiff(String key, String otherKey) {
		try {
			return getOperations().opsForSet().difference(key, otherKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取两个key的不同数据，存储到destKey中
	 *
	 * @param key      键
	 * @param otherKey 键
	 * @param destKey  键
	 * @return 返回成功数据
	 */
	public Long sDiffAndStore(String key, String otherKey, String destKey) {
		try {
			return getOperations().opsForSet().differenceAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取key和keys的不同数据，存储到destKey中
	 *
	 * @param key     键
	 * @param keys    键集合
	 * @param destKey 键
	 * @return 返回成功数据
	 */
	public Long sDiffAndStore(String key, Collection<String> keys, String destKey) {
		try {
			return getOperations().opsForSet().differenceAndStore(key, keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取多个keys的不同数据，存储到destKey中
	 *
	 * @param keys    键集合
	 * @param destKey 键
	 * @return 返回成功数据
	 */
	public Long sDiffAndStore(Collection<String> keys, String destKey) {
		try {
			return getOperations().opsForSet().differenceAndStore(keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据value从一个set中查询,是否存在
	 *
	 * @param key   键
	 * @param value 值
	 * @return true 存在 false不存在
	 */
	public boolean sHasKey(String key, Object value) {
		try {
			return getOperations().opsForSet().isMember(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的交集
	 */
	public Set<Object> sIntersect(String key, String otherKey) {
		try {
			return getOperations().opsForSet().intersect(key, otherKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的交集
	 */
	public Set<Object> sIntersect(String key, Collection<String> otherKeys) {
		try {
			return getOperations().opsForSet().intersect(key, otherKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的交集
	 */
	public Set<Object> sIntersect(Collection<String> otherKeys) {
		try {
			return getOperations().opsForSet().intersect(otherKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的交集并存储到目标 Key
	 */
	public Long sIntersectAndStore(String key, String otherKey, String destKey) {
		try {
			return getOperations().opsForSet().intersectAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的交集并存储到目标 Key
	 */
	public Long sIntersectAndStore(String key, Collection<String> otherKeys, String destKey) {
		try {
			return getOperations().opsForSet().intersectAndStore(key, otherKeys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的交集并存储到目标 Key
	 */
	public Long sIntersectAndStore(Collection<String> otherKeys, String destKey) {
		try {
			return getOperations().opsForSet().intersectAndStore(otherKeys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 随机获取指定数量的元素,同一个元素可能会选中两次
	 *
	 * @param key 缓存key
	 * @param count 随机获取元素数量
	 * @return 随机获取的元素集合
	 */
	public List<String> sRandomString(String key, long count) {
		return sRandomFor(key, count, TO_STRING);
	}

	/**
	 * 从 Set 中随机获取元素并转为 Double
	 */
	public List<Double> sRandomDouble(String key, long count) {
		return sRandomFor(key, count, TO_DOUBLE);
	}

	/**
	 * 从 Set 中随机获取元素并转为 Long
	 */
	public List<Long> sRandomLong(String key, long count) {
		return sRandomFor(key, count, TO_LONG);
	}

	/**
	 * 从 Set 中随机获取元素并转为 Integer
	 */
	public List<Integer> sRandomInteger(String key, long count) {
		return sRandomFor(key, count, TO_INTEGER);
	}

	/**
	 * 从 Set 中随机获取元素并按指定方式转换
	 */
	public <T> List<T> sRandomFor(String key, long count, Class<T> clazz) {
		return sRandomFor(key, count, member -> clazz.cast(member));
	}

	/**
	 * 从 Set 中随机获取元素并按指定方式转换
	 */
	public <T> List<T> sRandomFor(String key, long count, Function<Object, T> mapper) {
		List<Object> members = this.sRandom(key, count);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 从 Set 中随机获取指定数量的元素（可能重复）
	 */
	public List<Object> sRandom(String key, long count) {
		try {
			return getOperations().opsForSet().randomMembers(key, count);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 随机获取指定数量的元素,去重(同一个元素只能选择一次)
	 *
	 * @param key 缓存key
	 * @param count 随机获取元素数量
	 * @return 随机获取的元素集合（不重复）
	 */
	public Set<String> sRandomDistinctString(String key, long count) {
		return sRandomDistinctFor(key, count, TO_STRING);
	}

	/**
	 * 从 Set 中随机获取不重复元素并转为 Double
	 */
	public Set<Double> sRandomDistinctDouble(String key, long count) {
		return sRandomDistinctFor(key, count, TO_DOUBLE);
	}

	/**
	 * 从 Set 中随机获取不重复元素并转为 Long
	 */
	public Set<Long> sRandomDistinctLong(String key, long count) {
		return sRandomDistinctFor(key, count, TO_LONG);
	}

	/**
	 * 从 Set 中随机获取不重复元素并转为 Integer
	 */
	public Set<Integer> sRandomDistinctInteger(String key, long count) {
		return sRandomDistinctFor(key, count, TO_INTEGER);
	}

	/**
	 * 从 Set 中随机获取不重复元素并按指定方式转换
	 */
	public <T> Set<T> sRandomDistinctFor(String key, long count, Class<T> clazz) {
		return sRandomDistinctFor(key, count, member -> clazz.cast(member));
	}

	/**
	 * 从 Set 中随机获取不重复元素并按指定方式转换
	 */
	public <T> Set<T> sRandomDistinctFor(String key, long count, Function<Object, T> mapper) {
		Set<Object> members = this.sRandomDistinct(key, count);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 从 Set 中随机获取指定数量的不重复元素
	 */
	public Set<Object> sRandomDistinct(String key, long count) {
		try {
			return getOperations().opsForSet().distinctRandomMembers(key, count);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除值为value的
	 *
	 * @param key    键
	 * @param values 值 可以是多个
	 * @return 移除的个数
	 */
	public Long sRemove(String key, Object... values) {
		try {
			Long count = getOperations().opsForSet().remove(key, values);
			return count;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 扫描 Set 中的元素
	 */
	public void sScan(String bigSetKey, Consumer<byte[]> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).build();
		this.sScan(bigSetKey, options, consumer);
	}

	/**
	 * 扫描 Set 中的元素
	 */
	public void sScan(String bigSetKey, String pattern, Consumer<byte[]> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build();
		this.sScan(bigSetKey, options, consumer);
	}

	/**
	 * 扫描 Set 中的元素
	 */
	public void sScan(String bigSetKey, ScanOptions options, Consumer<byte[]> consumer) {
		this.getOperations().execute((RedisConnection redisConnection) -> {
			try (Cursor<byte[]> cursor = redisConnection.sScan(rawKey(bigSetKey), options)) {
				cursor.forEachRemaining(consumer);
				return null;
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RedisOperationException(e.getMessage());
			}
		});
	}

	/**
	 * 将set数据放入缓存
	 *
	 * @param key     键
	 * @param seconds 过期时长(秒)
	 * @param values  值 可以是多个
	 * @return 成功个数
	 */
	public Long sSetAndTime(String key, long seconds, Object... values) {
		try {
			Long count = getOperations().opsForSet().add(key, values);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return count;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取set缓存的长度
	 *
	 * @param key 键
	 * @return set缓存的长度
	 */
	public Long sSize(String key) {
		try {
			return getOperations().opsForSet().size(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的并集
	 */
	public Set<Object> sUnion(String key, String otherKey) {
		try {
			return getOperations().opsForSet().union(key, otherKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的并集
	 */
	public Set<Object> sUnion(String key, Collection<String> keys) {
		try {
			return getOperations().opsForSet().union(key, keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 合并所有指定keys的数据
	 *
	 * @param keys 键集合
	 * @return 返回成功数据
	 */
	public Set<Object> sUnion(Collection<String> keys) {
		try {
			return getOperations().opsForSet().union(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的并集并存储到目标 Key
	 */
	public Long sUnionAndStore(String key, String otherKey, String destKey) {
		try {
			return getOperations().opsForSet().unionAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 Set 的并集并存储到目标 Key
	 */
	public Long sUnionAndStore(String key, Collection<String> keys, String destKey) {
		try {
			return getOperations().opsForSet().unionAndStore(key, keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 合并所有指定keys的数据，存储到destKey中
	 *
	 * @param keys    键集合
	 * @param destKey 键
	 * @return 返回成功数据
	 */
	public Long sUnionAndStore(Collection<String> keys, String destKey) {
		try {
			return getOperations().opsForSet().unionAndStore(keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================ZSet=================================

	/**
	 * 向 ZSet 中添加元素
	 */
	public Boolean zAdd(String key, Object value, double score) {
		try {
			return getOperations().opsForZSet().add(key, value, score);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 ZSet 中添加元素
	 */
	public Boolean zAdd(String key, Object value, double score, long seconds) {
		try {
			Boolean result = getOperations().opsForZSet().add(key, value, score);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 ZSet 中添加元素
	 */
	public Boolean zAdd(String key, Object value, double score, Duration timeout) {
		try {
			Boolean result = getOperations().opsForZSet().add(key, value, score);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 ZSet 中添加元素
	 */
	public Long zAdd(String key, Set<TypedTuple<Object>> tuples) {
		try {
			return getOperations().opsForZSet().add(key, tuples);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 ZSet 中添加元素
	 */
	public Long zAdd(String key, Set<TypedTuple<Object>> tuples, long seconds) {
		try {
			Long result = getOperations().opsForZSet().add(key, tuples);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向 ZSet 中添加元素
	 */
	public Long zAdd(String key, Set<TypedTuple<Object>> tuples, Duration timeout) {
		try {
			Long result = getOperations().opsForZSet().add(key, tuples);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的元素数量
	 */
	public Long zCard(String key) {
		try {
			return getOperations().opsForZSet().zCard(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 判断 ZSet 中是否存在指定元素
	 */
	public Boolean zHas(String key, Object value) {
		try {
			return getOperations().opsForZSet().score(key, value) != null;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定分数范围内的元素数量
	 */
	public Long zCount(String key, double min, double max) {
		try {
			return getOperations().opsForZSet().count(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Set删除: sscan + srem
	 *
	 * @param bigZsetKey 键
	 * @return 是否删除成功
	 */
	public Boolean zDel(String bigZsetKey) {
		try {
			this.zScan(bigZsetKey, (tuple) -> {
				this.zRem(bigZsetKey, deserializeTuple(tuple).getValue());
			});
			return getOperations().delete(bigZsetKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 增加 ZSet 中元素的分数
	 */
	public Double zIncr(String key, Object value, double delta) {
		try {
			return getOperations().opsForZSet().incrementScore(key, value, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 增加 ZSet 中元素的分数
	 */
	public Double zIncr(String key, Object value, double delta, long seconds) {
		try {
			Double result = getOperations().opsForZSet().incrementScore(key, value, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 增加 ZSet 中元素的分数
	 */
	public Double zIncr(String key, Object value, double delta, Duration timeout) {
		try {
			Double result = getOperations().opsForZSet().incrementScore(key, value, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// zset指定元素增加值，并监听指定区域的顺序变化，如果指定区域元素发送变化，则返回true
	/**
	 * 增加 ZSet 中元素的分数并检查结果范围
	 */
	public Boolean zIncrAndWatch(String key, Object value, double delta, long start, long end) {
		try {
			byte[] rawKey = rawKey(key);
			byte[] rawValue = rawValue(value);
			return template.execute(redisConnection -> {
				// 1、增加score之前查询指定区域的元素对象
				Set<TypedTuple<Object>> zset1 = deserializeTupleValues(redisConnection.zRevRangeWithScores(rawKey, start, end));
				// 2、增加score
				redisConnection.zIncrBy(rawKey, delta, rawValue);
				// 3、增加score之后查询指定区域的元素对象
				Set<TypedTuple<Object>> zset2 = deserializeTupleValues(redisConnection.zRevRangeWithScores(rawKey, start, end));
				// 4、如果同一key两次取值有一个为空，表示元素发生了新增或移除，那两个元素一定有变化了
				if(CollectionUtils.isEmpty(zset1) && !CollectionUtils.isEmpty(zset2) || !CollectionUtils.isEmpty(zset1) && CollectionUtils.isEmpty(zset2)) {
					return Boolean.TRUE;
				}
				// 5、如果两个元素都不为空，但是长度不相同，表示元素一定有变化了
				if(zset1.size() != zset2.size()) {
					return Boolean.TRUE;
				}
				// 6、 两个set都不为空，且长度相同，则对key进行提取，并比较keyList与keyList2,一旦遇到相同位置处的值不一样，表示顺序发生了变化
				List<String> keyList1 = Objects.isNull(zset1) ? Lists.newArrayList() : zset1.stream().map(item -> item.getValue().toString()).collect(Collectors.toList());
				List<String> keyList2 = Objects.isNull(zset2) ? Lists.newArrayList() : zset2.stream().map(item -> item.getValue().toString()).collect(Collectors.toList());
				for (int i = 0; i < keyList1.size(); i++) {
					if(!Objects.equals(keyList1.get(i), keyList2.get(i))) {
						return Boolean.TRUE;
					}
				}
				return Boolean.FALSE;
			}, true);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的交集并存储到目标 Key
	 */
	public Long zIntersectAndStore(String key, String otherKey, String destKey) {
		try {
			return getOperations().opsForZSet().intersectAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的交集并存储到目标 Key
	 */
	public Long zIntersectAndStore(String key, Collection<String> otherKeys, String destKey) {
		try {
			return getOperations().opsForZSet().intersectAndStore(key, otherKeys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的交集并存储到目标 Key
	 */
	public Long zIntersectAndStore(String key, Collection<String> otherKeys, String destKey, Aggregate aggregate) {
		try {
			return getOperations().opsForZSet().intersectAndStore(key, otherKeys, destKey, aggregate);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的交集并存储到目标 Key
	 */
	public Long zIntersectAndStore(String key, Collection<String> otherKeys, String destKey, Aggregate aggregate,
			Weights weights) {
		try {
			return getOperations().opsForZSet().intersectAndStore(key, otherKeys, destKey, aggregate, weights);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除zset中的元素
	 *
	 * @param key 缓存key
	 * @param values 要移除的value数组
	 * @return 移除的元素个数
	 */
	public Long zRem(String key, Object... values) {
		try {
			return getOperations().opsForZSet().remove(key, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除分数区间内的元素
	 *
	 * @param key redis key
	 * @param min 最小score
	 * @param max 最大score
	 * @return 移除的元素个数
	 */
	public Long zRemByScore(String key, double min, double max) {
		try {
			return getOperations().opsForZSet().removeRangeByScore(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 String
	 */
	public Set<String> zRangeString(String key, long start, long end) {
		return zRangeFor(key, start, end, TO_STRING);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 Double
	 */
	public Set<Double> zRangeDouble(String key, long  start, long end) {
		return zRangeFor(key, start, end, TO_DOUBLE);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 Long
	 */
	public Set<Long> zRangeLong(String key, long  start, long end) {
		return zRangeFor(key, start, end, TO_LONG);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 Integer
	 */
	public Set<Integer> zRangeInteger(String key, long  start, long end) {
		return zRangeFor(key, start, end, TO_INTEGER);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并按指定方式转换
	 */
	public <T> Set<T> zRangeFor(String key, long start, long end, Class<T> clazz) {
		return zRangeFor(key, start, end, member -> clazz.cast(member));
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并按指定方式转换
	 */
	public <T> Set<T> zRangeFor(String key, long  start, long end, Function<Object, T> mapper) {
		Set<Object> members = this.zRange(key, start, end);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper)
					.collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素（升序）
	 */
	public Set<Object> zRange(String key, long start, long end) {
		try {
			return getOperations().opsForZSet().range(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 String
	 */
	public Set<String> zRangeStringByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_STRING);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 Double
	 */
	public Set<Double> zRangeDoubleByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_DOUBLE);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 Long
	 */
	public Set<Long> zRangeLongByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_LONG);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 Integer
	 */
	public Set<Integer> zRangeIntegerByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_INTEGER);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并按指定方式转换
	 */
	public <T> Set<T> zRangeByScoreFor(String key, double min, double max, Class<T> clazz) {
		return zRangeByScoreFor(key, min, max, member -> clazz.cast(member));
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并按指定方式转换
	 */
	public <T> Set<T> zRangeByScoreFor(String key, double min, double max, Function<Object, T> mapper) {
		Set<Object> members = this.zRangeByScore(key, min, max);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper)
					.collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素（升序）
	 */
	public Set<Object> zRangeByScore(String key, double min, double max) {
		try {
			return getOperations().opsForZSet().rangeByScore(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素及分数（升序）
	 */
	public Set<TypedTuple<Object>> zRangeWithScores(String key, long start, long end) {
		try {
			return getOperations().opsForZSet().rangeWithScores(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素及分数（升序）
	 */
	public Set<TypedTuple<Object>> zRangeByScoreWithScores(String key, double min, double max) {
		try {
			return getOperations().opsForZSet().rangeByScoreWithScores(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 按字典序获取 ZSet 中指定范围的元素
	 */
	public Set<Object> zRangeByLex(String key, Range range) {
		try {
			return getOperations().opsForZSet().rangeByLex(key, range);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 按字典序获取 ZSet 中指定范围的元素
	 */
	public Set<Object> zRangeByLex(String key, Range range, Limit limit) {
		try {
			return getOperations().opsForZSet().rangeByLex(key, range, limit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素（降序）
	 */
	public Set<Object> zRevrange(String key, long start, long end) {
		try {
			return getOperations().opsForZSet().reverseRange(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 String（降序）
	 */
	public Set<String> zRevrangeString(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_STRING);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 Double（降序）
	 */
	public Set<Double> zRevrangeDouble(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_DOUBLE);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 Long（降序）
	 */
	public Set<Long> zRevrangeLong(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_LONG);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并转为 Integer（降序）
	 */
	public Set<Integer> zRevrangeInteger(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_INTEGER);
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并按指定方式转换（降序）
	 */
	public <T> Set<T> zRevrangeFor(String key, long start, long end, Class<T> clazz) {
		return zRevrangeFor(key, start, end, member -> clazz.cast(member));
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素并按指定方式转换（降序）
	 */
	public <T> Set<T> zRevrangeFor(String key, long  start, long end, Function<Object, T> mapper) {
		Set<Object> members = this.zRevrange(key, start, end);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 获取 ZSet 中指定索引范围的元素及分数（降序）
	 */
	public Set<TypedTuple<Object>> zRevrangeWithScores(String key, long start, long end) {
		try {
			return getOperations().opsForZSet().reverseRangeWithScores(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素（降序）
	 */
	public Set<Object> zRevrangeByScore(String key, double min, double max) {
		try {
			return getOperations().opsForZSet().reverseRangeByScore(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 String（降序）
	 */
	public Set<String> zRevrangeStringByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_STRING);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 Double（降序）
	 */
	public Set<Double> zRevrangeDoubleByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_DOUBLE);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 Long（降序）
	 */
	public Set<Long> zRevrangeLongByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_LONG);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并转为 Integer（降序）
	 */
	public Set<Integer> zRevrangeIntegerByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_INTEGER);
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并按指定方式转换（降序）
	 */
	public <T> Set<T> zRevrangeForByScore(String key, double min, double max, Class<T> clazz) {
		return zRevrangeForByScore(key, min, max, member -> clazz.cast(member));
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素并按指定方式转换（降序）
	 */
	public <T> Set<T> zRevrangeForByScore(String key, double min, double max, Function<Object, T> mapper) {
		Set<Object> members = this.zRevrangeByScore(key, min, max);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 获取 ZSet 中指定分数范围的元素及分数（降序）
	 */
	public Set<TypedTuple<Object>> zRevrangeByScoreWithScores(String key, double min, double max) {
		try {
			return getOperations().opsForZSet().reverseRangeByScoreWithScores(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 中元素的排名（降序，从 0 开始）
	 */
	public Long zRevRank(String key, Object value) {
		try {
			return getOperations().opsForZSet().reverseRank(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 扫描 ZSet 中的元素
	 */
	public void zScan(String bigZsetKey, Consumer<Tuple> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).build();
		this.zScan(bigZsetKey, options, consumer);
	}

	/**
	 * 扫描 ZSet 中的元素
	 */
	public void zScan(String bigZsetKey, String pattern, Consumer<Tuple> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build();
		this.zScan(bigZsetKey, options, consumer);
	}

	/**
	 * 扫描 ZSet 中的元素
	 */
	public void zScan(String bigZsetKey, ScanOptions options, Consumer<Tuple> consumer) {
		this.getOperations().execute((RedisConnection redisConnection) -> {
			try (Cursor<Tuple> cursor = redisConnection.zScan(rawKey(bigZsetKey), options)) {
				cursor.forEachRemaining(consumer);
				return null;
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RedisOperationException(e.getMessage());
			}
		});
	}

	/**
	 * 获取 ZSet 中元素的分数
	 */
	public Double zScore(String key, Object value, double defaultVal) {
		Double rtVal = zScore(key, value);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取 ZSet 中元素的分数
	 */
	public Double zScore(String key, Object value) {
		try {
			return getOperations().opsForZSet().score(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的并集并存储到目标 Key
	 */
	public Long zUnionAndStore(String key, String otherKey, String destKey) {
		try {
			return getOperations().opsForZSet().unionAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的并集并存储到目标 Key
	 */
	public Long zUnionAndStore(String key, Collection<String> keys, String destKey) {
		try {
			return getOperations().opsForZSet().unionAndStore(key, keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的并集并存储到目标 Key
	 */
	public Long zUnionAndStore(String key, Collection<String> keys, String destKey, Aggregate aggregate) {
		try {
			return getOperations().opsForZSet().unionAndStore(key, keys, destKey, aggregate);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 ZSet 的并集并存储到目标 Key
	 */
	public Long zUnionAndStore(String key, Collection<String> keys, String destKey, Aggregate aggregate, Weights weights) {
		try {
			return getOperations().opsForZSet().unionAndStore(key, keys, destKey, aggregate, weights);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================HyperLogLog=================================

	/**
	 * 向 HyperLogLog 中添加元素
	 */
	public Long pfAdd(String key, Object... values) {
		try {
			return getOperations().opsForHyperLogLog().add(key, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 删除 HyperLogLog
	 */
	public Boolean pfDel(String key) {
		try {
			getOperations().opsForHyperLogLog().delete(key);
			return Boolean.TRUE;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取 HyperLogLog 的基数估算值
	 */
	public Long pfCount(String... keys) {
		try {
			return getOperations().opsForHyperLogLog().size(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 合并多个 HyperLogLog 到目标 Key
	 */
	public Long pfMerge(String destination, String... sourceKeys) {
		try {
			return getOperations().opsForHyperLogLog().union(destination, sourceKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================BitMap=================================

	/**
	 * 设置ASCII码, 字符串'a'的ASCII码是97, 转为二进制是'01100001', 此方法是将二进制第offset位值变为value
	 *
	 * @param key 缓存key
	 * @param offset 偏移量
	 * @param value 值,true为1, false为0
	 * @return 是否设置成功
	 */
	public Boolean setBit(String key, long offset, boolean value) {
		try {
			return getOperations().opsForValue().setBit(key, offset, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)
	 *
	 * @param key 缓存key
	 * @param offset 偏移量
	 * @return 是否有值
	 */
	public Boolean getBit(String key, long offset) {
		try {
			return getOperations().opsForValue().getBit(key, offset);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 追加到末尾
	 *
	 * @param key  缓存key
	 * @param value 字符串
	 * @return 追加结果
	 */
	public Integer append(String key, String value) {
		try {
			return getOperations().opsForValue().append(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================Message=================================

	/**
	 * 发送消息
	 *
	 * @param channel 消息channel
	 * @param message 消息内容
	 */
	public void sendMessage(String channel, String message) {
		try {
			getOperations().convertAndSend(channel, message);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================Lock=================================

	/**
	 * 1、对指定key来进行加锁逻辑（此锁是分布式阻塞锁）
	 * https://www.jianshu.com/p/6dbc44defd94
	 * @param lockKey  锁 key
	 * @param seconds  最大阻塞时间(秒)，超过时间将不再等待拿锁
	 * @return 获取锁成功/失败
	 */
	public boolean tryBlockLock(String lockKey, int seconds) {
        try {
			return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
			    // 1、获取时间毫秒值
			    long expireAt = redisConnection.time() + seconds * 1000 + 1;
			    // 2、第一次请求, 锁标识不存在的情况，直接拿到锁
			    Boolean acquire = redisConnection.setNX(rawKey(lockKey), String.valueOf(expireAt).getBytes());
			    if (acquire) {
			        return true;
			    } else {
			    	// 3、非第一次请求，阻塞等待拿到锁
			    	redisConnection.bRPop(seconds, rawKey(lockKey + ":list"));
			    }
			    return false;
			});
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
		}
       	return false;
    }

	/**
	 * 2、删除指定key来进行完成解锁逻辑
	 * @param lockKey  锁key
	 * @param requestId  锁值
	 * @return 释放锁成功/失败
	 */
    public boolean unBlockLock(String lockKey, String requestId) {
    	try {
    		return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
    			redisConnection.del(rawKey(lockKey));
    			byte[] rawKey = rawKey(lockKey + ":list");
    			byte[] rawValue = rawValue(requestId);
    			redisConnection.rPush(rawKey, rawValue);
    		    return true;
    		}, true);
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 尝试获取分布式锁
	 */
	public boolean tryLock(String lockKey, Duration timeout) {
		return tryLock( lockKey, timeout.toMillis());
	}

	/**
	 * 1、对指定key来进行加锁逻辑（此锁是全局性的）
	 * @param lockKey  锁key
	 * @param expireMillis 锁有效期
	 * @return 是否加锁成功
	 */
	public boolean tryLock(String lockKey, long expireMillis) {
        try {
			return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
				byte[] serLockKey = rawString(lockKey);
			    // 1、获取时间毫秒值
			    long expireAt = redisConnection.time() + expireMillis + 1;
			    // 2、获取锁
			    Boolean acquire = redisConnection.setNX(serLockKey, String.valueOf(expireAt).getBytes());
			    if (acquire) {
			        return true;
			    } else {
			        byte[] bytes = redisConnection.get(serLockKey);
			        // 3、非空判断
			        if (Objects.nonNull(bytes) && bytes.length > 0) {
			            long expireTime = Long.parseLong(new String(bytes));
			            // 4、如果锁已经过期
			            if (expireTime < redisConnection.time()) {
			                // 5、重新加锁，防止死锁
			                byte[] set = redisConnection.getSet(serLockKey, String.valueOf(redisConnection.time() + expireMillis + 1).getBytes());
			                return Long.parseLong(new String(set)) < redisConnection.time();
			            }
			        }
			    }
			    return false;
			});
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
		}
       	return false;
    }

	/**
	 * 2、删除指定key来进行完成解锁逻辑
	 * @param lockKey  锁key
	 * @return 是否解锁成功
	 */
    public boolean unlock(String lockKey) {
    	try {
	        return getOperations().delete(lockKey);
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
			throw new RedisOperationException(e.getMessage());
		}
	}

    /**
     * 尝试获取分布式锁
     */
    public boolean tryLock(String lockKey, String requestId, Duration timeout, int retryTimes, long retryInterval) {
    	return tryLock(lockKey, requestId, timeout.toMillis(), retryTimes, retryInterval);
    }

    /**
	 * 1、lua脚本加锁
	 * @param lockKey       锁的 key
	 * @param requestId     锁的 value
	 * @param expire        key 的过期时间，单位 ms
	 * @param retryTimes    重试次数，即加锁失败之后的重试次数
	 * @param retryInterval 重试时间间隔，单位 ms
	 * @return 加锁 true 成功
	 */
	public boolean tryLock(String lockKey, String requestId, long expire, int retryTimes, long retryInterval) {
       try {
			return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
				// 1、执行lua脚本
				Long result =  this.executeLuaScript(LOCK_LUA_SCRIPT, Collections.singletonList(lockKey), requestId, expire);
				if(LOCK_SUCCESS.equals(result)) {
				    log.info("locked... redisK = {}", lockKey);
				    return true;
				} else {
					// 2、重试获取锁
			        int count = 0;
			        while(count < retryTimes) {
			            try {
			                Thread.sleep(retryInterval);
			                result = this.executeLuaScript(LOCK_LUA_SCRIPT, Collections.singletonList(lockKey), requestId, expire);
			                if(LOCK_SUCCESS.equals(result)) {
			                	log.info("locked... redisK = {}", lockKey);
			                    return true;
			                }
			                log.warn("{} times try to acquire lock", count + 1);
			                count++;
			            } catch (Exception e) {
			            	log.error("acquire redis occurred an exception", e);
			            }
			        }
			        log.info("fail to acquire lock {}", lockKey);
			        return false;
				}
			});
		} catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
		}
       	return false;
	}

	/**
	 * 2、lua脚本释放KEY
	 * @param lockKey 释放本请求对应的锁的key
	 * @param requestId   释放本请求对应的锁的value
	 * @return 释放锁 true 成功
	 */
    public boolean unlock(String lockKey, String requestId) {
        log.info("unlock... redisK = {}", lockKey);
        try {
            // 使用lua脚本删除redis中匹配value的key
            Long result = this.executeLuaScript(UNLOCK_LUA_SCRIPT, Collections.singletonList(lockKey), requestId);
            //如果这里抛异常，后续锁无法释放
            if (LOCK_SUCCESS.equals(result)) {
            	log.info("release lock success. redisK = {}", lockKey);
                return true;
            } else if (LOCK_EXPIRED.equals(result)) {
            	log.warn("release lock exception, key has expired or released");
            } else {
                //其他情况，一般是删除KEY失败，返回0
            	log.error("release lock failed");
            }
        } catch (Exception e) {
        	log.error("release lock occurred an exception", e);
			throw new RedisOperationException(e.getMessage());
        }
        return false;
    }

	// ===============================Pipeline=================================

	/**
	 * 执行 Redis Pipeline 批量操作
	 */
	public List<Object> executePipelined(RedisCallback<?> action) {
		try {
			return getOperations().executePipelined(action);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行 Redis Pipeline 批量操作
	 */
	public List<Object> executePipelined(RedisCallback<?> action, RedisSerializer<?> resultSerializer) {
		try {
			return getOperations().executePipelined(action, resultSerializer);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================RedisScript=================================

	/**
     * 库存增加
     * @param key   库存key
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Long luaIncr(String key, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(INCR_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存增加
     * @param key   库存key
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Double luaIncr(String key, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(INCR_BYFLOAT_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Long luaDecr(String key, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(DECR_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 库存除以指定数值
	 * @param key   库存key
	 * @param delta 被除数
	 * @return
	 *      -3:代表传进来的被除数值是非正数（非法值）
	 *      -2:库存未初始化
	 *      -1:库存不足
	 *      大于等于0: 除法运算后的结果
	 */
	public Long luaDiv(String key, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(DIV_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Double luaDecr(String key, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(DECR_BYFLOAT_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}

	}

	/**
     * 库存增加
     * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Long luaHincr(String key, String hashKey, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return getOperations().execute(HINCR_SCRIPT, this.hashValueSerializer(), this.hashValueSerializer(),
					Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存增加
     * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Double luaHincr(String key, String hashKey, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return getOperations().execute(HINCR_BYFLOAT_SCRIPT, this.hashValueSerializer(),
					this.hashValueSerializer(), Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Long luaHdecr(String key, String hashKey, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return getOperations().execute(HDECR_SCRIPT, this.hashValueSerializer(), this.hashValueSerializer(),
					Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Double luaHdecr(String key, String hashKey, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return getOperations().execute(HDECR_BYFLOAT_SCRIPT, this.hashValueSerializer(),
					this.hashValueSerializer(), Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 库存除以指定值
	 * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 被除数
	 * @return
	 *      -3:代表传进来的被除数值是非正数（非法值）
	 *      -2:库存未初始化
	 *      -1:库存不足
	 *      大于等于0: 除法运算后的结果
	 */
	public Long luaHdiv(String key, String hashKey, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return getOperations().execute(HDIV_SCRIPT, this.hashValueSerializer(),
					this.hashValueSerializer(), Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行lua脚本
	 *
	 * @param luaScript  脚本内容
	 * @param returnType 返回值类型
	 * @param keys       redis键列表
	 * @param values     值列表
	 * @param <R> 返回类型
	 * @return lua脚步执行结果
	 */
	public <R> R executeLuaScript(String luaScript, Class<R> returnType, List<String> keys, Object... values) {
		try {
			RedisScript redisScript = RedisScript.of(luaScript, returnType);
			return (R) getOperations().execute(redisScript, keys, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行lua脚本
	 *
	 * @param luaScript 脚本内容
	 * @param keys      redis键列表
	 * @param values    值列表
	 * @param <R> 返回类型
	 * @return lua脚步执行结果
	 */
	public <R> R executeLuaScript(RedisScript<R> luaScript, List<String> keys, Object... values) {
		try {
			return (R) getOperations().execute(luaScript, keys, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行lua脚本
	 *
	 * @param luaScript  脚本内容
	 * @param returnType 返回值类型
	 * @param keys       redis键列表
	 * @param values     值列表
	 * @param <R> 返回类型
	 * @return lua脚步执行结果
	 */
	public <R> R executeLuaScript(Resource luaScript, Class<R> returnType, List<String> keys, Object... values) {
		try {
			RedisScript redisScript = RedisScript.of(luaScript, returnType);
			return (R) getOperations().execute(redisScript, keys, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================RedisCommand=================================

	/**
	 * 获取redis服务器时间 保证集群环境下时间一致
	 * @return Redis服务器时间戳
	 */
	public Long timeNow() {
		try {
			return getOperations().execute((RedisCallback<Long>) redisConnection -> {
				return redisConnection.time();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取redis服务器时间 保证集群环境下时间一致
	 * @param expiration 过期时间搓
	 * @return Redis服务器时间戳
	 */
	public Long period(long expiration) {
		try {
			return getOperations().execute((RedisCallback<Long>) redisConnection -> {
				return expiration - redisConnection.time();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取当前数据库的 Key 数量
	 */
	public Long dbSize() {
		try {
			return getOperations().execute((RedisCallback<Long>) redisConnection -> {
				return redisConnection.dbSize();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取最后一次成功执行 SAVE 命令的时间戳
	 */
	public Long lastSave() {
		try {
			return getOperations().execute((RedisCallback<Long>) redisConnection -> {
				return redisConnection.lastSave();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 异步重写 AOF 文件
	 */
	public void bgReWriteAof() {
		try {
			getOperations().execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.bgReWriteAof();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 异步保存数据到磁盘（BGSAVE）
	 */
	public void bgSave() {
		try {
			getOperations().execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.bgSave();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 同步保存数据到磁盘（SAVE）
	 */
	public void save() {
		try {
			getOperations().execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.save();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 清空当前数据库
	 */
	public void flushDb() {
		try {
			getOperations().execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.flushDb();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 清空所有数据库
	 */
	public void flushAll() {
		try {
			getOperations().execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.flushAll();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

}
