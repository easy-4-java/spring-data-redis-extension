package io.github.easy4j.redistpl.core;



/**
 * Container of Lua scripts used by the {@code spring-data-redis-extension}
 * templates to perform atomic Redis operations.
 *
 * <p>All scripts use {@code KEYS[]} for variable key inputs and {@code ARGV[]}
 * for variable value inputs. The scripts encode inventory-style primitives
 * (increment / decrement / division) and distributed-lock primitives
 * (acquire / release). Each script returns a long that callers interpret
 * according to the conventions documented in the script-level Javadoc.</p>
 *
 * <p>Reference: <a href="https://www.233tw.com/lua/7033">https://www.233tw.com/lua/7033</a>.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 3.0.0
 */
public class RedisLua {

	/**
	 * Lua script implementing an atomic {@code SETNX + PEXPIRE} distributed
	 * lock acquisition. Returns {@code 1} on success, {@code -1} otherwise.
	 */
	public static final String LOCK_LUA_SCRIPT = "if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then return redis.call('pexpire', KEYS[1], ARGV[2]) else return -1 end";

	/**
	 * Lua script implementing the comparison-and-delete pattern used to release
	 * a distributed lock. Returns {@code 1} on success, {@code -1} otherwise.
	 */
	public static final String UNLOCK_LUA_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return -1 end";

    /**
     * Increments a string-typed inventory counter atomically.
     *
     * <p>Return codes:</p>
     * <ul>
     *   <li>{@code -4}: the supplied increment is negative (invalid input)</li>
     *   <li>{@code -3}: the counter is not initialised</li>
     *   <li>{@code >=0}: the remaining inventory after the increment</li>
     * </ul>
     */
    public static final String INCR_SCRIPT =
		"if (redis.call('EXISTS', KEYS[1]) == 1) then"
	   + "    local num = tonumber(ARGV[1]);"
	   + "    if (num < 0) then "
	   + "    	  return -4;"
	   + "    end;"
	   + "    return redis.call('INCRBY', KEYS[1], num);"
	   + "end;"
	   + "return -3;";

    /**
     * Increments a string-typed inventory counter using a floating-point delta.
     * Return codes follow the same convention as {@link #INCR_SCRIPT}.
     */
    public static final String INCR_BYFLOAT_SCRIPT =
		"if (redis.call('EXISTS', KEYS[1]) == 1) then"
	   + "    local num = tonumber(ARGV[1]);"
	   + "    if (num < 0) then "
	   + "    	  return -4;"
	   + "    end;"
	   + "    return redis.call('INCRBYFLOAT', KEYS[1], num);"
	   + "end;"
	   + "return -3;";

    /**
     * Decrements a string-typed inventory counter atomically.
     *
     * <p>Return codes:</p>
     * <ul>
     *   <li>{@code -4}: the supplied delta is non-positive (invalid input)</li>
     *   <li>{@code -3}: the counter is not initialised</li>
     *   <li>{@code -2}: not enough inventory available</li>
     *   <li>{@code -1}: inventory is already zero</li>
     *   <li>{@code >=0}: the remaining inventory after the decrement</li>
     * </ul>
     */
    public static final String DECR_SCRIPT =
    	  "if (redis.call('EXISTS', KEYS[1]) == 1) then"
	    + "    local stock = tonumber(redis.call('GET', KEYS[1]));"
	    + "    local num = tonumber(ARGV[1]);"
	    + "    if (num <= 0) then"
	    + "        return -4;"
	    + "    end;"
	    + "    if (stock <= 0) then"
	    + "        return -1;"
	    + "    end;"
	    + "    if (stock >= num) then"
	    + "        return redis.call('INCRBY', KEYS[1], 0 - num);"
	    + "    end;"
	    + "    return -2;"
	    + "end;"
	    + "return -3;";

	/**
	 * Divides a string-typed inventory counter atomically.
	 *
	 * <p>Return codes:</p>
	 * <ul>
	 *   <li>{@code -3}: the supplied divisor is non-positive (invalid input)</li>
	 *   <li>{@code -2}: the counter is not initialised</li>
	 *   <li>{@code -1}: inventory is non-positive</li>
	 *   <li>{@code >=0}: the integer quotient of {@code stock / divisor}</li>
	 * </ul>
	 */
	public static final String DIV_SCRIPT =
		"if (redis.call('EXISTS', KEYS[1]) == 1) then"
		+ "    local total = tonumber(redis.call('GET', KEYS[1]));"
		+ "    local divided = tonumber(ARGV[1]);"
		+ "    if (divided <= 0) then"
		+ "        return -3;"
		+ "    end;"
		+ "    if (total <= 0) then"
		+ "        return -1;"
		+ "    end;"
		+ "    return tonumber(total/divided);"
		+ "end;"
		+ "return -2;";

    /**
     * Decrements a string-typed inventory counter using a floating-point delta.
     * Return codes follow the same convention as {@link #DECR_SCRIPT}.
     */
    public static final String DECR_BYFLOAT_SCRIPT =
    	  "if (redis.call('EXISTS', KEYS[1]) == 1) then"
	    + "    local stock = tonumber(redis.call('GET', KEYS[1]));"
	    + "    local num = tonumber(ARGV[1]);"
	    + "    if (num <= 0) then"
	    + "        return -4;"
	    + "    end;"
	    + "    if (stock <= 0) then"
	    + "        return -1;"
	    + "    end;"
	    + "    if (stock >= num) then"
	    + "        return redis.call('INCRBYFLOAT', KEYS[1], 0 - num);"
	    + "    end;"
	    + "    return -2;"
	    + "end;"
	    + "return -3;";

    /**
     * Increments a hash-field-typed inventory counter atomically. The script
     * uses {@code KEYS[1]} for the hash key and {@code KEYS[2]} for the hash
     * field. Return codes mirror {@link #INCR_SCRIPT}.
     */
    public static final String HINCR_SCRIPT =
		  "if (redis.call('HEXISTS', KEYS[1], KEYS[2]) == 1) then"
	    + "    local num = tonumber(ARGV[1]);"
	    + "    if (num < 0) then "
	    + "    	  return -4;"
	    + "    end;"
	    + "    return redis.call('HINCRBY', KEYS[1], KEYS[2], num);"
	    + "end;"
	    + "return -3;";

    /**
     * Decrements a hash-field-typed inventory counter atomically. Return
     * codes mirror {@link #DECR_SCRIPT}.
     */
    public static final String HDECR_SCRIPT =
		  "if (redis.call('HEXISTS', KEYS[1], KEYS[2]) == 1) then"
	    + "    local stock = tonumber(redis.call('HGET', KEYS[1], KEYS[2]));"
	    + "    local num = tonumber(ARGV[1]);"
	    + "    if (num <= 0) then"
	    + "        return -4;"
	    + "    end;"
	    + "    if (stock <= 0) then"
	    + "        return -1;"
	    + "    end;"
	    + "    if (stock >= num) then"
	    + "        return redis.call('HINCRBY', KEYS[1], KEYS[2], 0 - num);"
	    + "    end;"
	    + "    return -2;"
	    + "end;"
	    + "return -3;";

	/**
	 * Divides a hash-field-typed inventory counter atomically.
	 *
	 * <p>Return codes:</p>
	 * <ul>
	 *   <li>{@code -3}: the supplied divisor is non-positive (invalid input)</li>
	 *   <li>{@code -2}: the hash field is not initialised</li>
	 *   <li>{@code -1}: inventory is non-positive</li>
	 *   <li>{@code >=0}: the integer quotient of {@code total / divisor}</li>
	 * </ul>
	 */
	public static final String HDIV_SCRIPT =
			"if (redis.call('HEXISTS', KEYS[1], KEYS[2]) == 1) then"
					+ "    local total = tonumber(redis.call('HGET', KEYS[1], KEYS[2]));"
					+ "    local divided = tonumber(ARGV[1]);"
					+ "    if (divided <= 0) then"
					+ "        return -3;"
					+ "    end;"
					+ "    if (total <= 0) then"
					+ "        return -1;"
					+ "    end;"
					+ "    return tonumber(total/divided);"
					+ "end;"
					+ "return -2;";

    /**
     * Increments a hash-field-typed inventory counter using a floating-point
     * delta. Return codes mirror {@link #HINCR_SCRIPT}.
     */
    public static final String HINCR_BYFLOAT_SCRIPT =
  		  "if (redis.call('HEXISTS', KEYS[1], KEYS[2]) == 1) then"
  	    + "    local num = tonumber(ARGV[1]);"
  	    + "    if (num < 0) then "
  	    + "    	  return -4;"
  	    + "    end;"
  	    + "    return redis.call('HINCRBYFLOAT', KEYS[1], KEYS[2], num);"
  	    + "end;"
  	    + "return -3;";

    /**
     * Decrements a hash-field-typed inventory counter using a floating-point
     * delta. Return codes mirror {@link #HDECR_SCRIPT}.
     */
    public static final String HDECR_BYFLOAT_SCRIPT =
  		  "if (redis.call('HEXISTS', KEYS[1], KEYS[2]) == 1) then"
  	    + "    local stock = tonumber(redis.call('HGET', KEYS[1], KEYS[2]));"
  	    + "    local num = tonumber(ARGV[1]);"
  	    + "    if (num <= 0) then"
  	    + "        return -4;"
  	    + "    end;"
  	    + "    if (stock <= 0) then"
  	    + "        return -1;"
  	    + "    end;"
  	    + "    if (stock >= num) then"
  	    + "        return redis.call('HINCRBYFLOAT', KEYS[1], KEYS[2], 0 - num);"
  	    + "    end;"
  	    + "    return -2;"
  	    + "end;"
  	    + "return -3;";

}