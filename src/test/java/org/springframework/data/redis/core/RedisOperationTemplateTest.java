package org.springframework.data.redis.core;

import io.github.easy4j.redistpl.core.RedisOperationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link RedisOperationTemplate}.
 */
class RedisOperationTemplateTest {

    private RedisTemplate<String, Object> redisTemplate;
    private ValueOperations<String, Object> valueOps;
    private HashOperations<String, Object, Object> hashOps;
    private ListOperations<String, Object> listOps;
    private SetOperations<String, Object> setOps;
    private ZSetOperations<String, Object> zSetOps;
    private RedisOperationTemplate template;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        hashOps = mock(HashOperations.class);
        listOps = mock(ListOperations.class);
        setOps = mock(SetOperations.class);
        zSetOps = mock(ZSetOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(redisTemplate.opsForList()).thenReturn(listOps);
        when(redisTemplate.opsForSet()).thenReturn(setOps);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOps);

        template = new RedisOperationTemplate(redisTemplate);
    }

    // ==================== Constructor ====================

    @Test
    void shouldReturnRedisTemplate() {
        assertSame(redisTemplate, template.getRedisTemplate());
    }

    // ==================== Key Operations ====================

    @Test
    void shouldCheckHasKey() {
        when(redisTemplate.hasKey("key")).thenReturn(true);
        assertTrue(template.hasKey("key"));
    }

    @Test
    void shouldExpireKey() {
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertTrue(template.expire("key", 60));
    }

    @Test
    void shouldExpireKeyWithDuration() {
        when(redisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(true);
        assertTrue(template.expire("key", Duration.ofSeconds(60)));
    }

    @Test
    void shouldExpireAt() {
        Date date = new Date();
        when(redisTemplate.expireAt("key", date)).thenReturn(true);
        assertTrue(template.expireAt("key", date));
    }

    @Test
    void shouldGetExpire() {
        when(redisTemplate.getExpire("key", TimeUnit.SECONDS)).thenReturn(60L);
        assertEquals(60L, template.getExpire("key"));
    }

    @Test
    void shouldGetExpireWithUnit() {
        when(redisTemplate.getExpire("key", TimeUnit.MILLISECONDS)).thenReturn(60000L);
        assertEquals(60000L, template.getExpire("key", TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldPersist() {
        when(redisTemplate.persist("key")).thenReturn(true);
        assertTrue(template.persist("key"));
    }

    @Test
    void shouldRandomKey() {
        when(redisTemplate.randomKey()).thenReturn("randomKey");
        assertEquals("randomKey", template.randomKey());
    }

    @Test
    void shouldRename() {
        template.rename("oldKey", "newKey");
        verify(redisTemplate).rename("oldKey", "newKey");
    }

    @Test
    void shouldRenameIfAbsent() {
        when(redisTemplate.renameIfAbsent("oldKey", "newKey")).thenReturn(true);
        assertTrue(template.renameIfAbsent("oldKey", "newKey"));
    }

    @Test
    void shouldGetType() {
        when(redisTemplate.type("key")).thenReturn(DataType.STRING);
        assertEquals(DataType.STRING, template.type("key"));
    }

    // ==================== String Operations ====================

    @Test
    void shouldSet() {
        assertTrue(template.set("key", "value"));
        verify(valueOps).set("key", "value");
    }

    @Test
    void shouldSetWithSeconds() {
        assertTrue(template.set("key", "value", 60));
        verify(valueOps).set("key", "value", 60, TimeUnit.SECONDS);
    }

    @Test
    void shouldSetWithoutTtlWhenSecondsZero() {
        assertTrue(template.set("key", "value", 0));
        verify(valueOps).set("key", "value");
    }

    @Test
    void shouldSetWithDuration() {
        assertTrue(template.set("key", "value", Duration.ofSeconds(60)));
        verify(valueOps).set("key", "value", Duration.ofSeconds(60));
    }

    @Test
    void shouldReturnFalseWhenDurationIsNull() {
        assertFalse(template.set("key", "value", (Duration) null));
    }

    @Test
    void shouldReturnFalseWhenDurationIsNegative() {
        assertFalse(template.set("key", "value", Duration.ofSeconds(-1)));
    }

    @Test
    void shouldSetRange() {
        template.setRange("key", "value", 5);
        verify(valueOps).set("key", "value", 5);
    }

    @Test
    void shouldSetNx() {
        when(valueOps.setIfAbsent("key", "value")).thenReturn(true);
        assertTrue(template.setNx("key", "value"));
    }

    @Test
    void shouldSetNxWithMilliseconds() {
        when(valueOps.setIfAbsent(eq("key"), eq("value"), any(Duration.class))).thenReturn(true);
        assertTrue(template.setNx("key", "value", 60000L));
    }

    @Test
    void shouldSetNxWithTimeoutAndUnit() {
        when(valueOps.setIfAbsent("key", "value", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertTrue(template.setNx("key", "value", 60, TimeUnit.SECONDS));
    }

    @Test
    void shouldSetNxWithDuration() {
        when(valueOps.setIfAbsent(eq("key"), eq("value"), any(Duration.class))).thenReturn(true);
        assertTrue(template.setNx("key", "value", Duration.ofSeconds(60)));
    }

    @Test
    void shouldGet() {
        when(valueOps.get("key")).thenReturn("value");
        assertEquals("value", template.get("key"));
    }

    @Test
    void shouldGetString() {
        when(valueOps.get("key")).thenReturn("value");
        assertEquals("value", template.getString("key"));
    }

    @Test
    void shouldGetStringWithDefault() {
        when(valueOps.get("key")).thenReturn(null);
        assertEquals("default", template.getString("key", "default"));
    }

    @Test
    void shouldGetDouble() {
        when(valueOps.get("key")).thenReturn(3.14);
        assertEquals(3.14, template.getDouble("key"));
    }

    @Test
    void shouldGetDoubleWithDefault() {
        when(valueOps.get("key")).thenReturn(null);
        assertEquals(1.0, template.getDouble("key", 1.0));
    }

    @Test
    void shouldGetLong() {
        when(valueOps.get("key")).thenReturn(42L);
        assertEquals(42L, template.getLong("key"));
    }

    @Test
    void shouldGetLongWithDefault() {
        when(valueOps.get("key")).thenReturn(null);
        assertEquals(0L, template.getLong("key", 0L));
    }

    @Test
    void shouldGetInteger() {
        when(valueOps.get("key")).thenReturn(42);
        assertEquals(42, template.getInteger("key"));
    }

    @Test
    void shouldGetIntegerWithDefault() {
        when(valueOps.get("key")).thenReturn(null);
        assertEquals(0, template.getInteger("key", 0));
    }

    @Test
    void shouldGetForWithClass() {
        when(valueOps.get("key")).thenReturn("value");
        String result = template.getFor("key", String.class);
        assertEquals("value", result);
    }

    @Test
    void shouldGetForWithMapper() {
        when(valueOps.get("key")).thenReturn("VALUE");
        String result = template.getFor("key", v -> ((String) v).toLowerCase());
        assertEquals("value", result);
    }

    @Test
    void shouldGetForReturnNullWhenValueIsNull() {
        when(valueOps.get("key")).thenReturn(null);
        assertNull(template.getFor("key", String.class));
    }

    @Test
    void shouldGetRange() {
        when(valueOps.get("key", 0, 5)).thenReturn("hello");
        assertEquals("hello", template.getRange("key", 0, 5));
    }

    @Test
    void shouldGetAndSet() {
        when(valueOps.getAndSet("key", "newValue")).thenReturn("oldValue");
        assertEquals("oldValue", template.getAndSet("key", "newValue"));
    }

    @Test
    void shouldGetStringAndSet() {
        when(valueOps.getAndSet("key", "newValue")).thenReturn("oldValue");
        assertEquals("oldValue", template.getStringAndSet("key", "newValue"));
    }

    @Test
    void shouldGetDoubleAndSet() {
        when(valueOps.getAndSet("key", 42)).thenReturn(3.14);
        assertEquals(3.14, template.getDoubleAndSet("key", 42));
    }

    @Test
    void shouldGetLongAndSet() {
        when(valueOps.getAndSet("key", 42)).thenReturn(100L);
        assertEquals(100L, template.getLongAndSet("key", 42));
    }

    @Test
    void shouldGetIntegerAndSet() {
        when(valueOps.getAndSet("key", 42)).thenReturn(100);
        assertEquals(100, template.getIntegerAndSet("key", 42));
    }

    @Test
    void shouldIncrLong() {
        when(valueOps.increment("key", 5)).thenReturn(10L);
        assertEquals(10L, template.incr("key", 5));
    }

    @Test
    void shouldThrowWhenIncrWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.incr("key", -1));
    }

    @Test
    void shouldIncrLongWithSeconds() {
        when(valueOps.increment("key", 5)).thenReturn(10L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(10L, template.incr("key", 5, 60));
    }

    @Test
    void shouldIncrLongWithDuration() {
        when(valueOps.increment("key", 5)).thenReturn(10L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(10L, template.incr("key", 5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldIncrDouble() {
        when(valueOps.increment("key", 1.5)).thenReturn(3.0);
        assertEquals(3.0, template.incr("key", 1.5));
    }

    @Test
    void shouldThrowWhenIncrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.incr("key", -1.0));
    }

    @Test
    void shouldIncrDoubleWithSeconds() {
        when(valueOps.increment("key", 1.5)).thenReturn(3.0);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(3.0, template.incr("key", 1.5, 60));
    }

    @Test
    void shouldIncrDoubleWithDuration() {
        when(valueOps.increment("key", 1.5)).thenReturn(3.0);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(3.0, template.incr("key", 1.5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldDecrLong() {
        when(valueOps.increment("key", -5)).thenReturn(5L);
        assertEquals(5L, template.decr("key", 5));
    }

    @Test
    void shouldThrowWhenDecrWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.decr("key", -1));
    }

    @Test
    void shouldDecrLongWithSeconds() {
        when(valueOps.increment("key", -5)).thenReturn(5L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(5L, template.decr("key", 5, 60));
    }

    @Test
    void shouldDecrLongWithDuration() {
        when(valueOps.increment("key", -5)).thenReturn(5L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(5L, template.decr("key", 5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldDecrDouble() {
        when(valueOps.increment("key", -1.5)).thenReturn(1.5);
        assertEquals(1.5, template.decr("key", 1.5));
    }

    @Test
    void shouldThrowWhenDecrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.decr("key", -1.0));
    }

    @Test
    void shouldDecrDoubleWithSeconds() {
        when(valueOps.increment("key", -1.5)).thenReturn(1.5);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(1.5, template.decr("key", 1.5, 60));
    }

    @Test
    void shouldDecrDoubleWithDuration() {
        when(valueOps.increment("key", -1.5)).thenReturn(1.5);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(1.5, template.decr("key", 1.5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldDeleteSingleKey() {
        template.del("key");
        verify(redisTemplate).delete("key");
    }

    @Test
    void shouldDeleteMultipleKeys() {
        template.del("key1", "key2");
        verify(redisTemplate).delete(anyList());
    }

    @Test
    void shouldNotDeleteWhenKeysEmpty() {
        template.del();
        verify(redisTemplate, never()).delete(anyString());
    }

    @Test
    void shouldNotDeleteWhenKeysNull() {
        template.del((String[]) null);
        verify(redisTemplate, never()).delete(anyString());
    }

    @Test
    void shouldAppend() {
        when(valueOps.append("key", "suffix")).thenReturn(10);
        assertEquals(10, template.append("key", "suffix"));
    }

    // ==================== List Operations ====================

    @Test
    void shouldLRange() {
        List<Object> expected = Arrays.asList("a", "b", "c");
        when(listOps.range("key", 0, -1)).thenReturn(expected);
        assertEquals(expected, template.lRange("key", 0, -1));
    }

    @Test
    void shouldLRangeString() {
        when(listOps.range("key", 0, -1)).thenReturn(Arrays.asList("a", "b"));
        List<String> result = template.lRangeString("key", 0, -1);
        assertEquals(Arrays.asList("a", "b"), result);
    }

    @Test
    void shouldLRangeDouble() {
        when(listOps.range("key", 0, -1)).thenReturn(Arrays.asList(1.1, 2.2));
        List<Double> result = template.lRangeDouble("key", 0, -1);
        assertEquals(Arrays.asList(1.1, 2.2), result);
    }

    @Test
    void shouldLRangeLong() {
        when(listOps.range("key", 0, -1)).thenReturn(Arrays.asList(1L, 2L));
        List<Long> result = template.lRangeLong("key", 0, -1);
        assertEquals(Arrays.asList(1L, 2L), result);
    }

    @Test
    void shouldLRangeInteger() {
        when(listOps.range("key", 0, -1)).thenReturn(Arrays.asList(1, 2));
        List<Integer> result = template.lRangeInteger("key", 0, -1);
        assertEquals(Arrays.asList(1, 2), result);
    }

    @Test
    void shouldLRangeForWithClass() {
        when(listOps.range("key", 0, -1)).thenReturn(Arrays.asList("a", "b"));
        List<String> result = template.lRangeFor("key", 0, -1, String.class);
        assertEquals(Arrays.asList("a", "b"), result);
    }

    @Test
    void shouldLRangeForWithMapper() {
        when(listOps.range("key", 0, -1)).thenReturn(Arrays.asList("a", "b"));
        List<String> result = template.lRangeFor("key", 0, -1, v -> ((String) v).toUpperCase());
        assertEquals(Arrays.asList("A", "B"), result);
    }

    @Test
    void shouldLRangeForReturnNullWhenResultIsNull() {
        when(listOps.range("key", 0, -1)).thenReturn(null);
        assertNull(template.lRangeFor("key", 0, -1, String.class));
    }

    @Test
    void shouldLIndex() {
        when(listOps.index("key", 0)).thenReturn("value");
        assertEquals("value", template.lIndex("key", 0));
    }

    @Test
    void shouldLIndexString() {
        when(listOps.index("key", 0)).thenReturn("value");
        assertEquals("value", template.lIndexString("key", 0));
    }

    @Test
    void shouldLIndexStringWithDefault() {
        when(listOps.index("key", 0)).thenReturn(null);
        assertEquals("default", template.glIndexString("key", 0, "default"));
    }

    @Test
    void shouldLIndexDouble() {
        when(listOps.index("key", 0)).thenReturn(3.14);
        assertEquals(3.14, template.lIndexDouble("key", 0));
    }

    @Test
    void shouldLIndexDoubleWithDefault() {
        when(listOps.index("key", 0)).thenReturn(null);
        assertEquals(1.0, template.lIndexDouble("key", 0, 1.0));
    }

    @Test
    void shouldLIndexLong() {
        when(listOps.index("key", 0)).thenReturn(42L);
        assertEquals(42L, template.lIndexLong("key", 0));
    }

    @Test
    void shouldLIndexLongWithDefault() {
        when(listOps.index("key", 0)).thenReturn(null);
        assertEquals(0L, template.lIndexLong("key", 0, 0L));
    }

    @Test
    void shouldLIndexInteger() {
        when(listOps.index("key", 0)).thenReturn(42);
        assertEquals(42, template.lIndexInteger("key", 0));
    }

    @Test
    void shouldLIndexIntegerWithDefault() {
        when(listOps.index("key", 0)).thenReturn(null);
        assertEquals(0, template.lIndexInteger("key", 0, 0));
    }

    @Test
    void shouldLIndexFor() {
        when(listOps.index("key", 0)).thenReturn("value");
        String result = template.lIndexFor("key", 0, v -> ((String) v).toUpperCase());
        assertEquals("VALUE", result);
    }

    @Test
    void shouldLIndexForReturnNullWhenMemberIsNull() {
        when(listOps.index("key", 0)).thenReturn(null);
        assertNull(template.lIndexFor("key", 0, v -> v));
    }

    @Test
    void shouldLLeftPush() {
        when(listOps.leftPush("key", "value")).thenReturn(1L);
        assertEquals(1L, template.lLeftPush("key", "value"));
    }

    @Test
    void shouldLLeftPushWithSeconds() {
        when(listOps.leftPush("key", "value")).thenReturn(1L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(1L, template.lLeftPush("key", "value", 60));
    }

    @Test
    void shouldLLeftPushWithDuration() {
        when(listOps.leftPush("key", "value")).thenReturn(1L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(1L, template.lLeftPush("key", "value", Duration.ofSeconds(60)));
    }

    @Test
    void shouldLLeftPushAll() {
        when(listOps.leftPushAll(eq("key"), any(Object[].class))).thenReturn(2L);
        assertEquals(2L, template.lLeftPushAll("key", Arrays.asList("a", "b")));
    }

    @Test
    void shouldLLeftPushAllWithSeconds() {
        when(listOps.leftPushAll(eq("key"), any(Object[].class))).thenReturn(2L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(2L, template.lLeftPushAll("key", Arrays.asList("a", "b"), 60));
    }

    @Test
    void shouldLLeftPushAllWithDuration() {
        when(listOps.leftPushAll(eq("key"), any(Object[].class))).thenReturn(2L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(2L, template.lLeftPushAll("key", Arrays.asList("a", "b"), Duration.ofSeconds(60)));
    }

    @Test
    void shouldLLeftPushx() {
        when(listOps.leftPushIfPresent("key", "value")).thenReturn(1L);
        assertEquals(1L, template.lLeftPushx("key", "value"));
    }

    @Test
    void shouldLLeftPushxWithSeconds() {
        when(listOps.leftPushIfPresent("key", "value")).thenReturn(1L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(1L, template.lLeftPushx("key", "value", 60));
    }

    @Test
    void shouldLLeftPushxWithDuration() {
        when(listOps.leftPushIfPresent("key", "value")).thenReturn(1L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(1L, template.lLeftPushx("key", "value", Duration.ofSeconds(60)));
    }

    @Test
    void shouldLLeftPop() {
        when(listOps.leftPop("key")).thenReturn("value");
        assertEquals("value", template.lLeftPop("key"));
    }

    @Test
    void shouldLLeftPopWithTimeout() {
        when(listOps.leftPop("key", 60, TimeUnit.SECONDS)).thenReturn("value");
        assertEquals("value", template.lLeftPop("key", 60, TimeUnit.SECONDS));
    }

    @Test
    void shouldLLeftPopWithDuration() {
        when(listOps.leftPop(eq("key"), any(Duration.class))).thenReturn("value");
        assertEquals("value", template.lLeftPop("key", Duration.ofSeconds(60)));
    }

    @Test
    void shouldLRightPush() {
        when(listOps.rightPush("key", "value")).thenReturn(1L);
        assertEquals(1L, template.lRightPush("key", "value"));
    }

    @Test
    void shouldLRightPushWithSeconds() {
        when(listOps.rightPush("key", "value")).thenReturn(1L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(1L, template.lRightPush("key", "value", 60));
    }

    @Test
    void shouldLRightPushWithDuration() {
        when(listOps.rightPush("key", "value")).thenReturn(1L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(1L, template.lRightPush("key", "value", Duration.ofSeconds(60)));
    }

    @Test
    void shouldLRightPushAll() {
        when(listOps.rightPushAll(eq("key"), any(Object[].class))).thenReturn(2L);
        assertEquals(2L, template.lRightPushAll("key", Arrays.asList("a", "b")));
    }

    @Test
    void shouldLRightPushAllWithSeconds() {
        when(listOps.rightPushAll(eq("key"), any(Object[].class))).thenReturn(2L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(2L, template.lRightPushAll("key", Arrays.asList("a", "b"), 60));
    }

    @Test
    void shouldLRightPushAllWithDuration() {
        when(listOps.rightPushAll(eq("key"), any(Object[].class))).thenReturn(2L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(2L, template.lRightPushAll("key", Arrays.asList("a", "b"), Duration.ofSeconds(60)));
    }

    @Test
    void shouldLRightPop() {
        when(listOps.rightPop("key")).thenReturn("value");
        assertEquals("value", template.lRightPop("key"));
    }

    @Test
    void shouldLRightPopWithTimeout() {
        when(listOps.rightPop("key", 60, TimeUnit.SECONDS)).thenReturn("value");
        assertEquals("value", template.lRightPop("key", 60, TimeUnit.SECONDS));
    }

    @Test
    void shouldLRightPopWithDuration() {
        when(listOps.rightPop(eq("key"), any(Duration.class))).thenReturn("value");
        assertEquals("value", template.lRightPop("key", Duration.ofSeconds(60)));
    }

    @Test
    void shouldLRightPopAndLeftPush() {
        when(listOps.rightPopAndLeftPush("src", "dest")).thenReturn("value");
        assertEquals("value", template.lRightPopAndLeftPush("src", "dest"));
    }

    @Test
    void shouldLRightPopAndLeftPushWithTimeout() {
        when(listOps.rightPopAndLeftPush("src", "dest", 60, TimeUnit.SECONDS)).thenReturn("value");
        assertEquals("value", template.lRightPopAndLeftPush("src", "dest", 60, TimeUnit.SECONDS));
    }

    @Test
    void shouldLRightPopAndLeftPushWithDuration() {
        when(listOps.rightPopAndLeftPush(eq("src"), eq("dest"), any(Duration.class))).thenReturn("value");
        assertEquals("value", template.lRightPopAndLeftPush("src", "dest", Duration.ofSeconds(60)));
    }

    @Test
    void shouldLSet() {
        assertTrue(template.lSet("key", 0, "value"));
        verify(listOps).set("key", 0, "value");
    }

    @Test
    void shouldLSize() {
        when(listOps.size("key")).thenReturn(5L);
        assertEquals(5L, template.lSize("key"));
    }

    @Test
    void shouldLTrim() {
        assertTrue(template.lTrim("key", 0, 5));
        verify(listOps).trim("key", 0, 5);
    }

    @Test
    void shouldLRem() {
        when(listOps.remove("key", 1, "value")).thenReturn(1L);
        assertEquals(1L, template.lRem("key", 1, "value"));
    }

    // ==================== Hash Operations ====================

    @Test
    void shouldHSet() {
        doNothing().when(hashOps).put("key", "field", "value");
        assertTrue(template.hSet("key", "field", "value"));
    }

    @Test
    void shouldHSetWithSeconds() {
        doNothing().when(hashOps).put("key", "field", "value");
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertTrue(template.hSet("key", "field", "value", 60));
    }

    @Test
    void shouldHSetWithDuration() {
        doNothing().when(hashOps).put("key", "field", "value");
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertTrue(template.hSet("key", "field", "value", Duration.ofSeconds(60)));
    }

    @Test
    void shouldHSetNX() {
        when(hashOps.putIfAbsent("key", "field", "value")).thenReturn(true);
        assertTrue(template.hSetNX("key", "field", "value"));
    }

    @Test
    void shouldHGet() {
        when(hashOps.get("key", "field")).thenReturn("value");
        assertEquals("value", template.hGet("key", "field"));
    }

    @Test
    void shouldHGetWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(null);
        assertEquals("default", template.hGet("key", "field", "default"));
    }

    @Test
    void shouldHGetString() {
        when(hashOps.get("key", "field")).thenReturn("value");
        assertEquals("value", template.hGetString("key", "field"));
    }

    @Test
    void shouldHGetStringWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(null);
        assertEquals("default", template.hGetString("key", "field", "default"));
    }

    @Test
    void shouldHGetDouble() {
        when(hashOps.get("key", "field")).thenReturn(3.14);
        assertEquals(3.14, template.hGetDouble("key", "field"));
    }

    @Test
    void shouldHGetDoubleWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(null);
        assertEquals(1.0, template.hGetDouble("key", "field", 1.0));
    }

    @Test
    void shouldHGetLong() {
        when(hashOps.get("key", "field")).thenReturn(42L);
        assertEquals(42L, template.hGetLong("key", "field"));
    }

    @Test
    void shouldHGetLongWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(null);
        assertEquals(0L, template.hGetLong("key", "field", 0L));
    }

    @Test
    void shouldHGetInteger() {
        when(hashOps.get("key", "field")).thenReturn(42);
        assertEquals(42, template.hGetInteger("key", "field"));
    }

    @Test
    void shouldHGetIntegerWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(null);
        assertEquals(0, template.hGetInteger("key", "field", 0));
    }

    @Test
    void shouldHGetForWithClass() {
        when(hashOps.get("key", "field")).thenReturn("value");
        String result = template.hGetFor("key", "field", String.class);
        assertEquals("value", result);
    }

    @Test
    void shouldHGetForWithMapper() {
        when(hashOps.get("key", "field")).thenReturn("VALUE");
        String result = template.hGetFor("key", "field", v -> ((String) v).toLowerCase());
        assertEquals("value", result);
    }

    @Test
    void shouldHGetForReturnNullWhenValueIsNull() {
        when(hashOps.get("key", "field")).thenReturn(null);
        assertNull(template.hGetFor("key", "field", String.class));
    }

    @Test
    void shouldHHasKey() {
        when(hashOps.hasKey("key", "field")).thenReturn(true);
        assertTrue(template.hHasKey("key", "field"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldHmGet() {
        Map<Object, Object> expected = new HashMap<>();
        expected.put("field1", "value1");
        expected.put("field2", "value2");
        when(hashOps.entries("key")).thenReturn(expected);
        assertEquals(expected, template.hmGet("key"));
    }

    @Test
    void shouldHmSet() {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "value1");
        doNothing().when(hashOps).putAll("key", map);
        assertTrue(template.hmSet("key", map));
    }

    @Test
    void shouldHmSetWithSeconds() {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "value1");
        doNothing().when(hashOps).putAll("key", map);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertTrue(template.hmSet("key", map, 60));
    }

    @Test
    void shouldHmSetWithDuration() {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "value1");
        doNothing().when(hashOps).putAll("key", map);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertTrue(template.hmSet("key", map, Duration.ofSeconds(60)));
    }

    @Test
    void shouldHKeys() {
        Set<Object> expected = new HashSet<>(Arrays.asList("field1", "field2"));
        when(hashOps.keys("key")).thenReturn(expected);
        assertEquals(expected, template.hKeys("key"));
    }

    @Test
    void shouldHSize() {
        when(hashOps.size("key")).thenReturn(2L);
        assertEquals(2L, template.hSize("key"));
    }

    @Test
    void shouldHValues() {
        List<Object> expected = Arrays.asList("value1", "value2");
        when(hashOps.values("key")).thenReturn(expected);
        assertEquals(expected, template.hValues("key"));
    }

    @Test
    void shouldHIncrInt() {
        when(hashOps.increment("key", "field", 5)).thenReturn(10L);
        assertEquals(10L, template.hIncr("key", "field", 5));
    }

    @Test
    void shouldThrowWhenHIncrIntWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", -1));
    }

    @Test
    void shouldHIncrIntWithSeconds() {
        when(hashOps.increment("key", "field", 5)).thenReturn(10L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(10L, template.hIncr("key", "field", 5, 60));
    }

    @Test
    void shouldHIncrIntWithDuration() {
        when(hashOps.increment("key", "field", 5)).thenReturn(10L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(10L, template.hIncr("key", "field", 5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldHIncrLong() {
        when(hashOps.increment("key", "field", 5L)).thenReturn(10L);
        assertEquals(10L, template.hIncr("key", "field", 5L));
    }

    @Test
    void shouldThrowWhenHIncrLongWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", -1L));
    }

    @Test
    void shouldHIncrLongWithSeconds() {
        when(hashOps.increment("key", "field", 5L)).thenReturn(10L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(10L, template.hIncr("key", "field", 5L, 60));
    }

    @Test
    void shouldHIncrLongWithDuration() {
        when(hashOps.increment("key", "field", 5L)).thenReturn(10L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(10L, template.hIncr("key", "field", 5L, Duration.ofSeconds(60)));
    }

    @Test
    void shouldHIncrDouble() {
        when(hashOps.increment("key", "field", 1.5)).thenReturn(3.0);
        assertEquals(3.0, template.hIncr("key", "field", 1.5));
    }

    @Test
    void shouldThrowWhenHIncrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", -1.0));
    }

    @Test
    void shouldHIncrDoubleWithSeconds() {
        when(hashOps.increment("key", "field", 1.5)).thenReturn(3.0);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(3.0, template.hIncr("key", "field", 1.5, 60));
    }

    @Test
    void shouldHIncrDoubleWithDuration() {
        when(hashOps.increment("key", "field", 1.5)).thenReturn(3.0);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(3.0, template.hIncr("key", "field", 1.5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldHDecrInt() {
        when(hashOps.increment("key", "field", -5)).thenReturn(5L);
        assertEquals(5L, template.hDecr("key", "field", 5));
    }

    @Test
    void shouldThrowWhenHDecrIntWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", -1));
    }

    @Test
    void shouldHDecrLong() {
        when(hashOps.increment("key", "field", -5L)).thenReturn(5L);
        assertEquals(5L, template.hDecr("key", "field", 5L));
    }

    @Test
    void shouldThrowWhenHDecrLongWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", -1L));
    }

    @Test
    void shouldHDecrDouble() {
        when(hashOps.increment("key", "field", -1.5)).thenReturn(1.5);
        assertEquals(1.5, template.hDecr("key", "field", 1.5));
    }

    @Test
    void shouldThrowWhenHDecrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", -1.0));
    }

    @Test
    void shouldHDel() {
        template.hDel("key", "field1", "field2");
        verify(hashOps).delete("key", "field1", "field2");
    }

    // ==================== Set Operations ====================

    @Test
    void shouldSAdd() {
        when(setOps.add("key", "v1", "v2")).thenReturn(2L);
        assertEquals(2L, template.sAdd("key", "v1", "v2"));
    }

    @Test
    void shouldSAddAndExpire() {
        when(setOps.add("key", "v1")).thenReturn(1L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(1L, template.sAddAndExpire("key", 60, "v1"));
    }

    @Test
    void shouldSAddAndExpireWithDuration() {
        when(setOps.add("key", "v1")).thenReturn(1L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(1L, template.sAddAndExpire("key", Duration.ofSeconds(60), "v1"));
    }

    @Test
    void shouldSGet() {
        Set<Object> expected = new HashSet<>(Arrays.asList("v1", "v2"));
        when(setOps.members("key")).thenReturn(expected);
        assertEquals(expected, template.sGet("key"));
    }

    @Test
    void shouldSGetString() {
        when(setOps.members("key")).thenReturn(new HashSet<>(Arrays.asList("a", "b")));
        Set<String> result = template.sGetString("key");
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
    }

    @Test
    void shouldSGetDouble() {
        when(setOps.members("key")).thenReturn(new HashSet<>(Arrays.asList(1.1, 2.2)));
        Set<Double> result = template.sGetDouble("key");
        assertTrue(result.contains(1.1));
        assertTrue(result.contains(2.2));
    }

    @Test
    void shouldSGetLong() {
        when(setOps.members("key")).thenReturn(new HashSet<>(Arrays.asList(1L, 2L)));
        Set<Long> result = template.sGetLong("key");
        assertTrue(result.contains(1L));
        assertTrue(result.contains(2L));
    }

    @Test
    void shouldSGetInteger() {
        when(setOps.members("key")).thenReturn(new HashSet<>(Arrays.asList(1, 2)));
        Set<Integer> result = template.sGetInteger("key");
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
    }

    @Test
    void shouldSGetForWithClass() {
        when(setOps.members("key")).thenReturn(new HashSet<>(Arrays.asList("a", "b")));
        Set<String> result = template.sGetFor("key", String.class);
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
    }

    @Test
    void shouldSGetForWithMapper() {
        when(setOps.members("key")).thenReturn(new HashSet<>(Arrays.asList("a", "b")));
        Set<String> result = template.sGetFor("key", v -> ((String) v).toUpperCase());
        assertTrue(result.contains("A"));
        assertTrue(result.contains("B"));
    }

    @Test
    void shouldSGetForReturnNullWhenMembersIsNull() {
        when(setOps.members("key")).thenReturn(null);
        assertNull(template.sGetFor("key", String.class));
    }

    @Test
    void shouldSHasKey() {
        when(setOps.isMember("key", "value")).thenReturn(true);
        assertTrue(template.sHasKey("key", "value"));
    }

    @Test
    void shouldSRemove() {
        when(setOps.remove("key", "v1", "v2")).thenReturn(2L);
        assertEquals(2L, template.sRemove("key", "v1", "v2"));
    }

    @Test
    void shouldSSize() {
        when(setOps.size("key")).thenReturn(5L);
        assertEquals(5L, template.sSize("key"));
    }

    @Test
    void shouldSDiff() {
        Set<Object> expected = new HashSet<>(Collections.singletonList("v1"));
        when(setOps.difference("key1", "key2")).thenReturn(expected);
        assertEquals(expected, template.sDiff("key1", "key2"));
    }

    @Test
    void shouldSDiffAndStore() {
        when(setOps.differenceAndStore("key1", "key2", "dest")).thenReturn(1L);
        assertEquals(1L, template.sDiffAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldSDiffAndStoreWithKeys() {
        when(setOps.differenceAndStore(eq("key1"), anyCollection(), eq("dest"))).thenReturn(1L);
        assertEquals(1L, template.sDiffAndStore("key1", Arrays.asList("key2", "key3"), "dest"));
    }

    @Test
    void shouldSDiffAndStoreWithCollection() {
        when(setOps.differenceAndStore(anyCollection(), eq("dest"))).thenReturn(1L);
        assertEquals(1L, template.sDiffAndStore(Arrays.asList("key1", "key2"), "dest"));
    }

    @Test
    void shouldSIntersect() {
        Set<Object> expected = new HashSet<>(Collections.singletonList("v1"));
        when(setOps.intersect("key1", "key2")).thenReturn(expected);
        assertEquals(expected, template.sIntersect("key1", "key2"));
    }

    @Test
    void shouldSIntersectWithCollection() {
        Set<Object> expected = new HashSet<>(Collections.singletonList("v1"));
        when(setOps.intersect(eq("key1"), anyCollection())).thenReturn(expected);
        assertEquals(expected, template.sIntersect("key1", Arrays.asList("key2", "key3")));
    }

    @Test
    void shouldSIntersectWithKeys() {
        Set<Object> expected = new HashSet<>(Collections.singletonList("v1"));
        when(setOps.intersect(anyCollection())).thenReturn(expected);
        assertEquals(expected, template.sIntersect(Arrays.asList("key1", "key2")));
    }

    @Test
    void shouldSIntersectAndStore() {
        when(setOps.intersectAndStore("key1", "key2", "dest")).thenReturn(1L);
        assertEquals(1L, template.sIntersectAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldSUnion() {
        Set<Object> expected = new HashSet<>(Arrays.asList("v1", "v2"));
        when(setOps.union("key1", "key2")).thenReturn(expected);
        assertEquals(expected, template.sUnion("key1", "key2"));
    }

    @Test
    void shouldSUnionAndStore() {
        when(setOps.unionAndStore("key1", "key2", "dest")).thenReturn(2L);
        assertEquals(2L, template.sUnionAndStore("key1", "key2", "dest"));
    }

    // ==================== ZSet Operations ====================

    @Test
    void shouldZAdd() {
        when(zSetOps.add("key", "value", 1.0)).thenReturn(true);
        assertTrue(template.zAdd("key", "value", 1.0));
    }

    @Test
    void shouldZAddWithSeconds() {
        when(zSetOps.add("key", "value", 1.0)).thenReturn(true);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertTrue(template.zAdd("key", "value", 1.0, 60));
    }

    @Test
    void shouldZAddWithDuration() {
        when(zSetOps.add("key", "value", 1.0)).thenReturn(true);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertTrue(template.zAdd("key", "value", 1.0, Duration.ofSeconds(60)));
    }

    @Test
    void shouldZCard() {
        when(zSetOps.zCard("key")).thenReturn(5L);
        assertEquals(5L, template.zCard("key"));
    }

    @Test
    void shouldZHas() {
        when(zSetOps.score("key", "value")).thenReturn(1.0);
        assertTrue(template.zHas("key", "value"));
    }

    @Test
    void shouldZHasReturnFalseWhenNotExists() {
        when(zSetOps.score("key", "value")).thenReturn(null);
        assertFalse(template.zHas("key", "value"));
    }

    @Test
    void shouldZCount() {
        when(zSetOps.count("key", 1.0, 10.0)).thenReturn(5L);
        assertEquals(5L, template.zCount("key", 1.0, 10.0));
    }

    @Test
    void shouldZIncr() {
        when(zSetOps.incrementScore("key", "value", 1.5)).thenReturn(3.0);
        assertEquals(3.0, template.zIncr("key", "value", 1.5));
    }

    @Test
    void shouldZIncrWithSeconds() {
        when(zSetOps.incrementScore("key", "value", 1.5)).thenReturn(3.0);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(3.0, template.zIncr("key", "value", 1.5, 60));
    }

    @Test
    void shouldZIncrWithDuration() {
        when(zSetOps.incrementScore("key", "value", 1.5)).thenReturn(3.0);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(3.0, template.zIncr("key", "value", 1.5, Duration.ofSeconds(60)));
    }

    @Test
    void shouldZRange() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList("v1", "v2"));
        when(zSetOps.range("key", 0, -1)).thenReturn(expected);
        assertEquals(expected, template.zRange("key", 0, -1));
    }

    @Test
    void shouldZRangeByScore() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList("v1", "v2"));
        when(zSetOps.rangeByScore("key", 1.0, 10.0)).thenReturn(expected);
        assertEquals(expected, template.zRangeByScore("key", 1.0, 10.0));
    }

    @Test
    void shouldZRangeWithScores() {
        Set<TypedTuple<Object>> expected = new LinkedHashSet<>();
        when(zSetOps.rangeWithScores("key", 0, -1)).thenReturn(expected);
        assertEquals(expected, template.zRangeWithScores("key", 0, -1));
    }

    @Test
    void shouldZRangeByScoreWithScores() {
        Set<TypedTuple<Object>> expected = new LinkedHashSet<>();
        when(zSetOps.rangeByScoreWithScores("key", 1.0, 10.0)).thenReturn(expected);
        assertEquals(expected, template.zRangeByScoreWithScores("key", 1.0, 10.0));
    }

    @Test
    void shouldZRangeByLex() {
        Set<Object> expected = new LinkedHashSet<>();
        when(zSetOps.rangeByLex(eq("key"), any(RedisZSetCommands.Range.class))).thenReturn(expected);
        assertEquals(expected, template.zRangeByLex("key", RedisZSetCommands.Range.unbounded()));
    }

    @Test
    void shouldZRevrange() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList("v2", "v1"));
        when(zSetOps.reverseRange("key", 0, -1)).thenReturn(expected);
        assertEquals(expected, template.zRevrange("key", 0, -1));
    }

    @Test
    void shouldZRevrangeByScore() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList("v2", "v1"));
        when(zSetOps.reverseRangeByScore("key", 1.0, 10.0)).thenReturn(expected);
        assertEquals(expected, template.zRevrangeByScore("key", 1.0, 10.0));
    }

    @Test
    void shouldZRevrangeWithScores() {
        Set<TypedTuple<Object>> expected = new LinkedHashSet<>();
        when(zSetOps.reverseRangeWithScores("key", 0, -1)).thenReturn(expected);
        assertEquals(expected, template.zRevrangeWithScores("key", 0, -1));
    }

    @Test
    void shouldZRevrangeByScoreWithScores() {
        Set<TypedTuple<Object>> expected = new LinkedHashSet<>();
        when(zSetOps.reverseRangeByScoreWithScores("key", 1.0, 10.0)).thenReturn(expected);
        assertEquals(expected, template.zRevrangeByScoreWithScores("key", 1.0, 10.0));
    }

    @Test
    void shouldZRem() {
        when(zSetOps.remove("key", "v1", "v2")).thenReturn(2L);
        assertEquals(2L, template.zRem("key", "v1", "v2"));
    }

    @Test
    void shouldZRemByScore() {
        when(zSetOps.removeRangeByScore("key", 1.0, 10.0)).thenReturn(5L);
        assertEquals(5L, template.zRemByScore("key", 1.0, 10.0));
    }

    @Test
    void shouldZScore() {
        when(zSetOps.score("key", "value")).thenReturn(1.5);
        assertEquals(1.5, template.zScore("key", "value"));
    }

    @Test
    void shouldZScoreWithDefault() {
        when(zSetOps.score("key", "value")).thenReturn(null);
        assertEquals(0.0, template.zScore("key", "value", 0.0));
    }

    @Test
    void shouldZRevRank() {
        when(zSetOps.reverseRank("key", "value")).thenReturn(2L);
        assertEquals(2L, template.zRevRank("key", "value"));
    }

    @Test
    void shouldZIntersectAndStore() {
        when(zSetOps.intersectAndStore("key1", "key2", "dest")).thenReturn(1L);
        assertEquals(1L, template.zIntersectAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldZIntersectAndStoreWithCollection() {
        when(zSetOps.intersectAndStore(eq("key1"), anyCollection(), eq("dest"))).thenReturn(1L);
        assertEquals(1L, template.zIntersectAndStore("key1", Arrays.asList("key2", "key3"), "dest"));
    }

    @Test
    void shouldZUnionAndStore() {
        when(zSetOps.unionAndStore("key1", "key2", "dest")).thenReturn(2L);
        assertEquals(2L, template.zUnionAndStore("key1", "key2", "dest"));
    }

    // ==================== Serializer Methods ====================

    @Test
    void shouldGetRedisTemplate() {
        assertNotNull(template.getRedisTemplate());
        assertSame(redisTemplate, template.getRedisTemplate());
    }

    // ==================== Error Handling ====================

    @Test
    void shouldThrowRedisOperationExceptionWhenSetFails() {
        doThrow(new RuntimeException("Redis error")).when(valueOps).set(anyString(), any());
        assertThrows(RedisOperationException.class, () -> template.set("key", "value"));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenGetFails() {
        when(valueOps.get(anyString())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.get("key"));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenHasKeyFails() {
        when(redisTemplate.hasKey(anyString())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.hasKey("key"));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenExpireFails() {
        when(redisTemplate.expire(anyString(), anyLong(), any(TimeUnit.class))).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.expire("key", 60));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenHSetFails() {
        doThrow(new RuntimeException("Redis error")).when(hashOps).put(anyString(), any(), any());
        assertThrows(RedisOperationException.class, () -> template.hSet("key", "field", "value"));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenLRangeFails() {
        when(listOps.range(anyString(), anyLong(), anyLong())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.lRange("key", 0, -1));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenSAddFails() {
        when(setOps.add(anyString(), any())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.sAdd("key", "value"));
    }

    @Test
    void shouldThrowRedisOperationExceptionWhenZAddFails() {
        when(zSetOps.add(anyString(), any(), anyDouble())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.zAdd("key", "value", 1.0));
    }

    // ==================== HyperLogLog Operations ====================

    @Test
    void shouldPfAdd() {
        HyperLogLogOperations<String, Object> hllOps = mock(HyperLogLogOperations.class);
        when(redisTemplate.opsForHyperLogLog()).thenReturn(hllOps);
        when(hllOps.add("key", "v1", "v2")).thenReturn(1L);
        assertEquals(1L, template.pfAdd("key", "v1", "v2"));
    }

    @Test
    void shouldPfDel() {
        HyperLogLogOperations<String, Object> hllOps = mock(HyperLogLogOperations.class);
        when(redisTemplate.opsForHyperLogLog()).thenReturn(hllOps);
        doNothing().when(hllOps).delete("key");
        assertTrue(template.pfDel("key"));
    }

    @Test
    void shouldPfCount() {
        HyperLogLogOperations<String, Object> hllOps = mock(HyperLogLogOperations.class);
        when(redisTemplate.opsForHyperLogLog()).thenReturn(hllOps);
        when(hllOps.size("key1", "key2")).thenReturn(100L);
        assertEquals(100L, template.pfCount("key1", "key2"));
    }

    @Test
    void shouldPfMerge() {
        HyperLogLogOperations<String, Object> hllOps = mock(HyperLogLogOperations.class);
        when(redisTemplate.opsForHyperLogLog()).thenReturn(hllOps);
        when(hllOps.union("dest", "src1", "src2")).thenReturn(1L);
        assertEquals(1L, template.pfMerge("dest", "src1", "src2"));
    }

    // ==================== BitMap Operations ====================

    @Test
    void shouldSetBit() {
        when(valueOps.setBit("key", 0, true)).thenReturn(true);
        assertTrue(template.setBit("key", 0, true));
    }

    @Test
    void shouldGetBit() {
        when(valueOps.getBit("key", 0)).thenReturn(true);
        assertTrue(template.getBit("key", 0));
    }

    // ==================== More Hash Operations ====================

    @Test
    @SuppressWarnings("unchecked")
    void shouldHMultiGet() {
        List<Object> expected = Arrays.asList("v1", "v2");
        when(hashOps.multiGet("key", Arrays.asList("f1", "f2"))).thenReturn(expected);
        assertEquals(expected, template.hMultiGet("key", Arrays.asList("f1", "f2")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHmMultiGet() {
        List<Object> values = Arrays.asList("v1", "v2");
        when(hashOps.multiGet("key", Arrays.asList("f1", "f2"))).thenReturn(values);
        Map<String, Object> result = template.hmMultiGet("key", Arrays.asList("f1", "f2"));
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void shouldHmMultiSet() {
        assertTrue(template.hmMultiSet("key", Arrays.asList("f1", "f2"), "value"));
    }

    @Test
    void shouldHmMultiSetReturnFalseWhenEmpty() {
        assertFalse(template.hmMultiSet("key", Collections.emptyList(), "value"));
    }

    @Test
    void shouldHmMultiSetReturnFalseWhenKeyBlank() {
        assertFalse(template.hmMultiSet("", Arrays.asList("f1"), "value"));
    }

    // ==================== More Set Operations ====================

    @Test
    void shouldSUnionWithKeys() {
        Set<Object> expected = new HashSet<>(Arrays.asList("v1", "v2"));
        when(setOps.union(anyCollection())).thenReturn(expected);
        assertEquals(expected, template.sUnion(Arrays.asList("key1", "key2")));
    }

    @Test
    void shouldSRandom() {
        when(setOps.randomMembers("key", 3)).thenReturn(Arrays.asList("v1", "v2", "v3"));
        List<Object> result = template.sRandom("key", 3);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    void shouldSRandomDistinct() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList("v1", "v2"));
        when(setOps.distinctRandomMembers("key", 2)).thenReturn(expected);
        assertEquals(expected, template.sRandomDistinct("key", 2));
    }

    @Test
    void shouldSRandomString() {
        when(setOps.randomMembers("key", 2)).thenReturn(Arrays.asList("v1", "v2"));
        List<String> result = template.sRandomString("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomDouble() {
        when(setOps.randomMembers("key", 2)).thenReturn(Arrays.asList(1.1, 2.2));
        List<Double> result = template.sRandomDouble("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomLong() {
        when(setOps.randomMembers("key", 2)).thenReturn(Arrays.asList(1L, 2L));
        List<Long> result = template.sRandomLong("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomInteger() {
        when(setOps.randomMembers("key", 2)).thenReturn(Arrays.asList(1, 2));
        List<Integer> result = template.sRandomInteger("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomDistinctString() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList("v1", "v2"));
        when(setOps.distinctRandomMembers("key", 2)).thenReturn(expected);
        Set<String> result = template.sRandomDistinctString("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomDistinctDouble() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList(1.1, 2.2));
        when(setOps.distinctRandomMembers("key", 2)).thenReturn(expected);
        Set<Double> result = template.sRandomDistinctDouble("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomDistinctLong() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList(1L, 2L));
        when(setOps.distinctRandomMembers("key", 2)).thenReturn(expected);
        Set<Long> result = template.sRandomDistinctLong("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSRandomDistinctInteger() {
        Set<Object> expected = new LinkedHashSet<>(Arrays.asList(1, 2));
        when(setOps.distinctRandomMembers("key", 2)).thenReturn(expected);
        Set<Integer> result = template.sRandomDistinctInteger("key", 2);
        assertNotNull(result);
    }

    @Test
    void shouldSDel() {
        when(setOps.remove(eq("key"), any())).thenReturn(1L);
        when(redisTemplate.delete("key")).thenReturn(true);
        assertTrue(template.sDel("key"));
    }

    @Test
    void shouldSSetAndTime() {
        when(setOps.add("key", "v1")).thenReturn(1L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(1L, template.sSetAndTime("key", 60, "v1"));
    }

    // ==================== More ZSet Operations ====================

    @Test
    void shouldZAddWithTuples() {
        Set<TypedTuple<Object>> tuples = new LinkedHashSet<>();
        when(zSetOps.add(eq("key"), anySet())).thenReturn(2L);
        assertEquals(2L, template.zAdd("key", tuples));
    }

    @Test
    void shouldZAddWithTuplesAndSeconds() {
        Set<TypedTuple<Object>> tuples = new LinkedHashSet<>();
        when(zSetOps.add(eq("key"), anySet())).thenReturn(2L);
        when(redisTemplate.expire("key", 60, TimeUnit.SECONDS)).thenReturn(true);
        assertEquals(2L, template.zAdd("key", tuples, 60));
    }

    @Test
    void shouldZAddWithTuplesAndDuration() {
        Set<TypedTuple<Object>> tuples = new LinkedHashSet<>();
        when(zSetOps.add(eq("key"), anySet())).thenReturn(2L);
        when(redisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(true);
        assertEquals(2L, template.zAdd("key", tuples, Duration.ofSeconds(60)));
    }

    @Test
    void shouldZIntersectAndStoreWithAggregate() {
        when(zSetOps.intersectAndStore(eq("key1"), anyCollection(), eq("dest"), any(RedisZSetCommands.Aggregate.class))).thenReturn(1L);
        assertEquals(1L, template.zIntersectAndStore("key1", Arrays.asList("key2"), "dest", RedisZSetCommands.Aggregate.SUM));
    }

    @Test
    void shouldZUnionAndStoreWithCollection() {
        when(zSetOps.unionAndStore(eq("key1"), anyCollection(), eq("dest"))).thenReturn(2L);
        assertEquals(2L, template.zUnionAndStore("key1", Arrays.asList("key2", "key3"), "dest"));
    }

    @Test
    void shouldZUnionAndStoreWithAggregate() {
        when(zSetOps.unionAndStore(eq("key1"), anyCollection(), eq("dest"), any(RedisZSetCommands.Aggregate.class))).thenReturn(2L);
        assertEquals(2L, template.zUnionAndStore("key1", Arrays.asList("key2"), "dest", RedisZSetCommands.Aggregate.SUM));
    }

    @Test
    void shouldZDel() {
        when(redisTemplate.delete("key")).thenReturn(true);
        assertTrue(template.zDel("key"));
    }

    @Test
    void shouldZRangeString() {
        when(zSetOps.range("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList("v1", "v2")));
        Set<String> result = template.zRangeString("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeDouble() {
        when(zSetOps.range("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList(1.1, 2.2)));
        Set<Double> result = template.zRangeDouble("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeLong() {
        when(zSetOps.range("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList(1L, 2L)));
        Set<Long> result = template.zRangeLong("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeInteger() {
        when(zSetOps.range("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList(1, 2)));
        Set<Integer> result = template.zRangeInteger("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeByScoreString() {
        when(zSetOps.rangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList("v1", "v2")));
        Set<String> result = template.zRangeStringByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeByScoreDouble() {
        when(zSetOps.rangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList(1.1, 2.2)));
        Set<Double> result = template.zRangeDoubleByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeByScoreLong() {
        when(zSetOps.rangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList(1L, 2L)));
        Set<Long> result = template.zRangeLongByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRangeByScoreInteger() {
        when(zSetOps.rangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList(1, 2)));
        Set<Integer> result = template.zRangeIntegerByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeString() {
        when(zSetOps.reverseRange("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList("v2", "v1")));
        Set<String> result = template.zRevrangeString("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeDouble() {
        when(zSetOps.reverseRange("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList(2.2, 1.1)));
        Set<Double> result = template.zRevrangeDouble("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeLong() {
        when(zSetOps.reverseRange("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList(2L, 1L)));
        Set<Long> result = template.zRevrangeLong("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeInteger() {
        when(zSetOps.reverseRange("key", 0, -1)).thenReturn(new LinkedHashSet<>(Arrays.asList(2, 1)));
        Set<Integer> result = template.zRevrangeInteger("key", 0, -1);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeByScoreString() {
        when(zSetOps.reverseRangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList("v2", "v1")));
        Set<String> result = template.zRevrangeStringByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeByScoreDouble() {
        when(zSetOps.reverseRangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList(2.2, 1.1)));
        Set<Double> result = template.zRevrangeDoubleByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeByScoreLong() {
        when(zSetOps.reverseRangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList(2L, 1L)));
        Set<Long> result = template.zRevrangeLongByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    @Test
    void shouldZRevrangeByScoreInteger() {
        when(zSetOps.reverseRangeByScore("key", 1.0, 10.0)).thenReturn(new LinkedHashSet<>(Arrays.asList(2, 1)));
        Set<Integer> result = template.zRevrangeIntegerByScore("key", 1.0, 10.0);
        assertNotNull(result);
    }

    // ==================== More String Operations ====================

    @Test
    void shouldMGet() {
        List<Object> expected = Arrays.asList("v1", "v2");
        when(redisTemplate.keys(anyString())).thenReturn(new LinkedHashSet<>(Arrays.asList("key1", "key2")));
        when(valueOps.multiGet(anyCollection())).thenReturn(expected);
        List<Object> result = template.mGet("key*");
        assertNotNull(result);
    }

    @Test
    void shouldMGetReturnEmptyWhenPatternBlank() {
        List<Object> result = template.mGet("");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMGetCollection() {
        List<Object> expected = Arrays.asList("v1", "v2");
        when(valueOps.multiGet(anyCollection())).thenReturn(expected);
        List<Object> result = template.mGet(Arrays.asList("key1", "key2"));
        assertNotNull(result);
    }

    @Test
    void shouldMGetCollectionReturnEmptyWhenEmpty() {
        List<Object> result = template.mGet(Collections.emptyList());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldGetRangeOperations() {
        assertNotNull(template.getRedisTemplate());
    }

    // ==================== Comprehensive Error Handling ====================

    @Test
    void shouldThrowWhenExpireAtFails() {
        when(redisTemplate.expireAt(anyString(), any(Date.class))).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.expireAt("key", new Date()));
    }

    @Test
    void shouldThrowWhenGetExpireFails() {
        when(redisTemplate.getExpire(anyString(), any(TimeUnit.class))).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.getExpire("key"));
    }

    @Test
    void shouldThrowWhenDelFails() {
        doThrow(new RuntimeException("error")).when(redisTemplate).delete(anyString());
        assertThrows(RedisOperationException.class, () -> template.del("key"));
    }

    @Test
    void shouldThrowWhenIncrFails() {
        when(valueOps.increment(anyString(), anyLong())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.incr("key", 1));
    }

    @Test
    void shouldThrowWhenDecrFails() {
        when(valueOps.increment(anyString(), anyLong())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.decr("key", 1));
    }

    @Test
    void shouldThrowWhenLSetFails() {
        doThrow(new RuntimeException("error")).when(listOps).set(anyString(), anyLong(), any());
        assertThrows(RedisOperationException.class, () -> template.lSet("key", 0, "value"));
    }

    @Test
    void shouldThrowWhenLSizeFails() {
        when(listOps.size(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lSize("key"));
    }

    @Test
    void shouldThrowWhenLTrimFails() {
        doThrow(new RuntimeException("error")).when(listOps).trim(anyString(), anyLong(), anyLong());
        assertThrows(RedisOperationException.class, () -> template.lTrim("key", 0, 5));
    }

    @Test
    void shouldThrowWhenLRemFails() {
        when(listOps.remove(anyString(), anyLong(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lRem("key", 1, "value"));
    }

    @Test
    void shouldThrowWhenLIndexFails() {
        when(listOps.index(anyString(), anyLong())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lIndex("key", 0));
    }

    @Test
    void shouldThrowWhenLLeftPushFails() {
        when(listOps.leftPush(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lLeftPush("key", "value"));
    }

    @Test
    void shouldThrowWhenLRightPushFails() {
        when(listOps.rightPush(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lRightPush("key", "value"));
    }

    @Test
    void shouldThrowWhenLLeftPopFails() {
        when(listOps.leftPop(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lLeftPop("key"));
    }

    @Test
    void shouldThrowWhenLRightPopFails() {
        when(listOps.rightPop(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.lRightPop("key"));
    }

    @Test
    void shouldThrowWhenHGetFails() {
        when(hashOps.get(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hGet("key", "field"));
    }

    @Test
    void shouldThrowWhenHHasKeyFails() {
        when(hashOps.hasKey(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hHasKey("key", "field"));
    }

    @Test
    void shouldThrowWhenHmGetFails() {
        when(hashOps.entries(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hmGet("key"));
    }

    @Test
    void shouldThrowWhenHKeysFails() {
        when(hashOps.keys(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hKeys("key"));
    }

    @Test
    void shouldThrowWhenHSizeFails() {
        when(hashOps.size(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hSize("key"));
    }

    @Test
    void shouldThrowWhenHValuesFails() {
        when(hashOps.values(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hValues("key"));
    }

    @Test
    void shouldThrowWhenHIncrFails() {
        when(hashOps.increment(anyString(), any(), anyInt())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", 1));
    }

    @Test
    void shouldThrowWhenHDecrFails() {
        when(hashOps.increment(anyString(), any(), anyInt())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", 1));
    }

    @Test
    void shouldThrowWhenHDelFails() {
        doThrow(new RuntimeException("error")).when(hashOps).delete(anyString(), any());
        assertThrows(RedisOperationException.class, () -> template.hDel("key", "field"));
    }

    @Test
    void shouldThrowWhenSGetFails() {
        when(setOps.members(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sGet("key"));
    }

    @Test
    void shouldThrowWhenSHasKeyFails() {
        when(setOps.isMember(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sHasKey("key", "value"));
    }

    @Test
    void shouldThrowWhenSRemoveFails() {
        when(setOps.remove(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sRemove("key", "value"));
    }

    @Test
    void shouldThrowWhenSSizeFails() {
        when(setOps.size(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sSize("key"));
    }

    @Test
    void shouldThrowWhenSDiffFails() {
        when(setOps.difference(anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sDiff("key1", "key2"));
    }

    @Test
    void shouldThrowWhenSIntersectFails() {
        when(setOps.intersect(anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sIntersect("key1", "key2"));
    }

    @Test
    void shouldThrowWhenSUnionFails() {
        when(setOps.union(anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sUnion("key1", "key2"));
    }

    @Test
    void shouldThrowWhenZCardFails() {
        when(zSetOps.zCard(anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zCard("key"));
    }

    @Test
    void shouldThrowWhenZScoreFails() {
        when(zSetOps.score(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zScore("key", "value"));
    }

    @Test
    void shouldThrowWhenZRangeFails() {
        when(zSetOps.range(anyString(), anyLong(), anyLong())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRange("key", 0, -1));
    }

    @Test
    void shouldThrowWhenZRangeByScoreFails() {
        when(zSetOps.rangeByScore(anyString(), anyDouble(), anyDouble())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRangeByScore("key", 1.0, 10.0));
    }

    @Test
    void shouldThrowWhenZRevrangeFails() {
        when(zSetOps.reverseRange(anyString(), anyLong(), anyLong())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRevrange("key", 0, -1));
    }

    @Test
    void shouldThrowWhenZRevrangeByScoreFails() {
        when(zSetOps.reverseRangeByScore(anyString(), anyDouble(), anyDouble())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRevrangeByScore("key", 1.0, 10.0));
    }

    @Test
    void shouldThrowWhenZRemFails() {
        when(zSetOps.remove(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRem("key", "value"));
    }

    @Test
    void shouldThrowWhenZRemByScoreFails() {
        when(zSetOps.removeRangeByScore(anyString(), anyDouble(), anyDouble())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRemByScore("key", 1.0, 10.0));
    }

    @Test
    void shouldThrowWhenZCountFails() {
        when(zSetOps.count(anyString(), anyDouble(), anyDouble())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zCount("key", 1.0, 10.0));
    }

    @Test
    void shouldThrowWhenZIncrFails() {
        when(zSetOps.incrementScore(anyString(), any(), anyDouble())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zIncr("key", "value", 1.0));
    }

    @Test
    void shouldThrowWhenZRevRankFails() {
        when(zSetOps.reverseRank(anyString(), any())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zRevRank("key", "value"));
    }

    @Test
    void shouldThrowWhenHmSetFails() {
        doThrow(new RuntimeException("error")).when(hashOps).putAll(anyString(), anyMap());
        assertThrows(RedisOperationException.class, () -> template.hmSet("key", new HashMap<>()));
    }

    @Test
    void shouldThrowWhenSDiffAndStoreFails() {
        when(setOps.differenceAndStore(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sDiffAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldThrowWhenSIntersectAndStoreFails() {
        when(setOps.intersectAndStore(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sIntersectAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldThrowWhenSUnionAndStoreFails() {
        when(setOps.unionAndStore(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.sUnionAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldThrowWhenZIntersectAndStoreFails() {
        when(zSetOps.intersectAndStore(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zIntersectAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldThrowWhenZUnionAndStoreFails() {
        when(zSetOps.unionAndStore(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.zUnionAndStore("key1", "key2", "dest"));
    }

    @Test
    void shouldThrowWhenKeysFails() {
        when(redisTemplate.scan(any(ScanOptions.class))).thenThrow(new RuntimeException("error"));
        assertThrows(RedisOperationException.class, () -> template.keys("pattern"));
    }
}
