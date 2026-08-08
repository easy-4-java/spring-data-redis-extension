package io.github.easy4j.redistpl.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RedisKey}.
 */
class RedisKeyTest {

    @Test
    void shouldReturnDescriptionForGeoLocationKey() {
        assertNotNull(RedisKey.GEO_LOCATION_KEY.getDesc());
        assertFalse(RedisKey.GEO_LOCATION_KEY.getDesc().isEmpty());
    }

    @Test
    void shouldGenerateKeyWithoutArgument() {
        String key = RedisKey.GEO_LOCATION_KEY.getKey();
        assertNotNull(key);
        assertTrue(key.startsWith(RedisKey.REDIS_PREFIX));
        assertTrue(key.contains(RedisKeyConstant.GEO_LOCATION_KEY));
    }

    @Test
    void shouldGenerateKeyWithArgument() {
        String key = RedisKey.IP_REGION_INFO.getKey("192.168.1.1");
        assertNotNull(key);
        assertTrue(key.startsWith(RedisKey.REDIS_PREFIX));
        assertTrue(key.contains("192.168.1.1"));
    }

    @Test
    void shouldGenerateKeyStrWithMultipleArgs() {
        String key = RedisKey.getKeyStr("module", "sub", "id");
        assertEquals("rds:module:sub:id", key);
    }

    @Test
    void shouldSkipNullArgsInGetKeyStr() {
        String key = RedisKey.getKeyStr("module", null, "id");
        assertEquals("rds:module:id", key);
    }

    @Test
    void shouldSkipBlankArgsInGetKeyStr() {
        String key = RedisKey.getKeyStr("module", "", "id");
        assertEquals("rds:module:id", key);
    }

    @Test
    void shouldGenerateKeyStrWithPrefixOnly() {
        String key = RedisKey.getKeyStr();
        assertEquals(RedisKey.REDIS_PREFIX, key);
    }

    @Test
    void shouldGenerateThreadKeyStr() {
        String key = RedisKey.getThreadKeyStr("prefix", "arg1", "arg2");
        assertNotNull(key);
        assertTrue(key.startsWith("prefix:"));
        assertTrue(key.contains("arg1"));
        assertTrue(key.contains("arg2"));
    }

    @Test
    void shouldSkipNullInThreadKeyStr() {
        String key = RedisKey.getThreadKeyStr("prefix", null, "arg2");
        assertNotNull(key);
        assertTrue(key.contains("arg2"));
    }

    @Test
    void shouldHaveCorrectDelimiter() {
        assertEquals(":", RedisKey.DELIMITER);
    }

    @Test
    void shouldHaveCorrectRedisPrefix() {
        assertEquals("rds", RedisKey.REDIS_PREFIX);
    }

    @Test
    void shouldGenerateAllEnumKeys() {
        for (RedisKey redisKey : RedisKey.values()) {
            assertNotNull(redisKey.getKey());
            assertNotNull(redisKey.getDesc());
        }
    }

    @Test
    void shouldGenerateIpLocationBaiduKey() {
        String key = RedisKey.IP_LOCATION_BAIDU_INFO.getKey("10.0.0.1");
        assertTrue(key.contains("baidu"));
        assertTrue(key.contains("10.0.0.1"));
    }

    @Test
    void shouldGenerateIpLocationPconlineKey() {
        String key = RedisKey.IP_LOCATION_PCONLINE_INFO.getKey("10.0.0.1");
        assertTrue(key.contains("pconline"));
        assertTrue(key.contains("10.0.0.1"));
    }

    @Test
    void shouldGenerateIpLocationKey() {
        String key = RedisKey.IP_LOCATION_INFO.getKey("10.0.0.1");
        assertTrue(key.contains("ip:location"));
        assertTrue(key.contains("10.0.0.1"));
    }
}
