package org.springframework.data.redis.core;

import io.github.easy4j.redistpl.core.RedisOperationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveServerCommands;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ReactiveRedisOperationTemplate}.
 */
class ReactiveRedisOperationTemplateTest {

    @SuppressWarnings("unchecked")
    private ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    @SuppressWarnings("unchecked")
    private ReactiveValueOperations<String, Object> valueOps;
    @SuppressWarnings("unchecked")
    private ReactiveHashOperations<String, Object, Object> hashOps;
    @SuppressWarnings("unchecked")
    private ReactiveListOperations<String, Object> listOps;
    @SuppressWarnings("unchecked")
    private ReactiveSetOperations<String, Object> setOps;
    @SuppressWarnings("unchecked")
    private ReactiveZSetOperations<String, Object> zSetOps;
    private ReactiveRedisOperationTemplate template;
    private RedisSerializationContext<String, Object> serializationContext;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        valueOps = mock(ReactiveValueOperations.class);
        hashOps = mock(ReactiveHashOperations.class);
        listOps = mock(ReactiveListOperations.class);
        setOps = mock(ReactiveSetOperations.class);
        zSetOps = mock(ReactiveZSetOperations.class);

        when(reactiveRedisTemplate.opsForValue()).thenReturn(valueOps);
        when(reactiveRedisTemplate.opsForHash()).thenReturn(hashOps);
        when(reactiveRedisTemplate.opsForList()).thenReturn(listOps);
        when(reactiveRedisTemplate.opsForSet()).thenReturn(setOps);
        when(reactiveRedisTemplate.opsForZSet()).thenReturn(zSetOps);

        template = new ReactiveRedisOperationTemplate(reactiveRedisTemplate);
    }

    // ==================== Key Operations ====================

    @Test
    void shouldExpireWithSeconds() {
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Boolean result = template.expire("key", 60).block();
        assertTrue(result);
    }

    @Test
    void shouldExpireWithDuration() {
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Boolean result = template.expire("key", Duration.ofSeconds(60)).block();
        assertTrue(result);
    }

    @Test
    void shouldReturnFalseWhenExpireDurationIsNull() {
        Boolean result = template.expire("key", (Duration) null).block();
        assertFalse(result);
    }

    @Test
    void shouldExpireAt() {
        Instant instant = Instant.now().plusSeconds(60);
        when(reactiveRedisTemplate.expireAt("key", instant)).thenReturn(Mono.just(true));
        Boolean result = template.expireAt("key", instant).block();
        assertTrue(result);
    }

    @Test
    void shouldReturnFalseWhenExpireAtIsNull() {
        Boolean result = template.expireAt("key", null).block();
        assertFalse(result);
    }

    @Test
    void shouldGetExpire() {
        when(reactiveRedisTemplate.getExpire("key")).thenReturn(Mono.just(Duration.ofSeconds(60)));
        Duration result = template.getExpire("key").block();
        assertNotNull(result);
    }

    @Test
    void shouldHasKey() {
        when(reactiveRedisTemplate.hasKey("key")).thenReturn(Mono.just(true));
        Boolean result = template.hasKey("key").block();
        assertTrue(result);
    }

    @Test
    void shouldGetKey() {
        when(reactiveRedisTemplate.keys("pattern")).thenReturn(Flux.just("key1", "key2"));
        List<String> result = template.getKey("pattern").collectList().block();
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void shouldReturnEmptyFluxWhenGetKeyPatternIsNull() {
        List<String> result = template.getKey(null).collectList().block();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldGetVagueKey() {
        when(reactiveRedisTemplate.keys("*pattern*")).thenReturn(Flux.just("key1"));
        List<String> result = template.getVagueKey("pattern").collectList().block();
        assertNotNull(result);
    }

    @Test
    void shouldGetValueKeyByPrefix() {
        when(reactiveRedisTemplate.keys("prefix*")).thenReturn(Flux.just("prefix:key1"));
        List<String> result = template.getValueKeyByPrefix("prefix").collectList().block();
        assertNotNull(result);
    }

    // ==================== String Operations ====================

    @Test
    void shouldSet() {
        when(valueOps.set("key", "value")).thenReturn(Mono.just(true));
        Boolean result = template.set("key", "value").block();
        assertTrue(result);
    }

    @Test
    void shouldSetWithSeconds() {
        when(valueOps.set("key", "value", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Boolean result = template.set("key", "value", 60).block();
        assertTrue(result);
    }

    @Test
    void shouldSetWithoutTtlWhenSecondsZero() {
        when(valueOps.set("key", "value")).thenReturn(Mono.just(true));
        Boolean result = template.set("key", "value", 0).block();
        assertTrue(result);
    }

    @Test
    void shouldSetWithDuration() {
        when(valueOps.set("key", "value", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Boolean result = template.set("key", "value", Duration.ofSeconds(60)).block();
        assertTrue(result);
    }

    @Test
    void shouldReturnFalseWhenSetDurationIsNull() {
        Boolean result = template.set("key", "value", (Duration) null).block();
        assertFalse(result);
    }

    @Test
    void shouldReturnFalseWhenSetDurationIsNegative() {
        Boolean result = template.set("key", "value", Duration.ofSeconds(-1)).block();
        assertFalse(result);
    }

    @Test
    void shouldSetNX() {
        when(valueOps.setIfAbsent("key", "value")).thenReturn(Mono.just(true));
        Boolean result = template.setNX("key", "value").block();
        assertTrue(result);
    }

    @Test
    void shouldSetNxWithMilliseconds() {
        when(valueOps.setIfAbsent(eq("key"), eq("value"), any(Duration.class))).thenReturn(Mono.just(true));
        Boolean result = template.setNx("key", "value", 60000L).block();
        assertTrue(result);
    }

    @Test
    void shouldSetNxWithDuration() {
        when(valueOps.setIfAbsent(eq("key"), eq("value"), any(Duration.class))).thenReturn(Mono.just(true));
        Boolean result = template.setNx("key", "value", Duration.ofSeconds(60)).block();
        assertTrue(result);
    }

    @Test
    void shouldGet() {
        when(valueOps.get("key")).thenReturn(Mono.just("value"));
        Object result = template.get("key").block();
        assertEquals("value", result);
    }

    @Test
    void shouldReturnEmptyMonoWhenGetKeyIsBlank() {
        Object result = template.get("").block();
        assertNull(result);
    }

    @Test
    void shouldGetString() {
        when(valueOps.get("key")).thenReturn(Mono.just("value"));
        String result = template.getString("key").block();
        assertEquals("value", result);
    }

    @Test
    void shouldGetDouble() {
        when(valueOps.get("key")).thenReturn(Mono.just(3.14));
        Double result = template.getDouble("key").block();
        assertEquals(3.14, result);
    }

    @Test
    void shouldGetLong() {
        when(valueOps.get("key")).thenReturn(Mono.just(42L));
        Long result = template.getLong("key").block();
        assertEquals(42L, result);
    }

    @Test
    void shouldGetInteger() {
        when(valueOps.get("key")).thenReturn(Mono.just(42));
        Integer result = template.getInteger("key").block();
        assertEquals(42, result);
    }

    @Test
    void shouldGetForWithClass() {
        when(valueOps.get("key")).thenReturn(Mono.just("value"));
        String result = template.getFor("key", String.class).block();
        assertEquals("value", result);
    }

    @Test
    void shouldGetForWithMapper() {
        when(valueOps.get("key")).thenReturn(Mono.just("VALUE"));
        String result = template.getFor("key", v -> ((String) v).toLowerCase()).block();
        assertEquals("value", result);
    }

    @Test
    void shouldIncrLong() {
        when(valueOps.increment("key", 5)).thenReturn(Mono.just(10L));
        Long result = template.incr("key", 5).block();
        assertEquals(10L, result);
    }

    @Test
    void shouldThrowWhenIncrWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.incr("key", -1).block());
    }

    @Test
    void shouldIncrLongWithSeconds() {
        when(valueOps.increment("key", 5)).thenReturn(Mono.just(10L));
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Long result = template.incr("key", 5, 60).block();
        assertEquals(10L, result);
    }

    @Test
    void shouldIncrDouble() {
        when(valueOps.increment("key", 1.5)).thenReturn(Mono.just(3.0));
        Double result = template.incr("key", 1.5).block();
        assertEquals(3.0, result);
    }

    @Test
    void shouldThrowWhenIncrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.incr("key", -1.0).block());
    }

    @Test
    void shouldDecrLong() {
        when(valueOps.increment("key", -5)).thenReturn(Mono.just(5L));
        Long result = template.decr("key", 5).block();
        assertEquals(5L, result);
    }

    @Test
    void shouldThrowWhenDecrWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.decr("key", -1).block());
    }

    @Test
    void shouldDecrLongWithSeconds() {
        when(valueOps.increment("key", -5)).thenReturn(Mono.just(5L));
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Long result = template.decr("key", 5, 60).block();
        assertEquals(5L, result);
    }

    @Test
    void shouldDecrDouble() {
        when(valueOps.increment("key", -1.5)).thenReturn(Mono.just(1.5));
        Double result = template.decr("key", 1.5).block();
        assertEquals(1.5, result);
    }

    @Test
    void shouldThrowWhenDecrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.decr("key", -1.0).block());
    }

    @Test
    void shouldReturnZeroWhenDelWithEmptyKeys() {
        Long result = template.del(new String[0]).block();
        assertEquals(0L, result);
    }

    @Test
    void shouldReturnZeroWhenDelWithNullKeys() {
        Long result = template.del((String[]) null).block();
        assertEquals(0L, result);
    }

    // ==================== Hash Operations ====================

    @Test
    void shouldHSet() {
        when(hashOps.put("key", "field", "value")).thenReturn(Mono.just(true));
        Boolean result = template.hSet("key", "field", "value").block();
        assertTrue(result);
    }

    @Test
    void shouldHGet() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just("value"));
        Object result = template.hGet("key", "field").block();
        assertEquals("value", result);
    }

    @Test
    void shouldHGetWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(Mono.empty());
        Object result = template.hGet("key", "field", "default").block();
        assertEquals("default", result);
    }

    @Test
    void shouldHGetString() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just("value"));
        String result = template.hGetString("key", "field").block();
        assertEquals("value", result);
    }

    @Test
    void shouldHGetStringWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(Mono.empty());
        String result = template.hGetString("key", "field", "default").block();
        assertEquals("default", result);
    }

    @Test
    void shouldHGetDouble() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just(3.14));
        Double result = template.hGetDouble("key", "field").block();
        assertEquals(3.14, result);
    }

    @Test
    void shouldHGetDoubleWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(Mono.empty());
        Double result = template.hGetDouble("key", "field", 1.0).block();
        assertEquals(1.0, result);
    }

    @Test
    void shouldHGetLong() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just(42L));
        Long result = template.hGetLong("key", "field").block();
        assertEquals(42L, result);
    }

    @Test
    void shouldHGetLongWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(Mono.empty());
        Long result = template.hGetLong("key", "field", 0L).block();
        assertEquals(0L, result);
    }

    @Test
    void shouldHGetInteger() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just(42));
        Integer result = template.hGetInteger("key", "field").block();
        assertEquals(42, result);
    }

    @Test
    void shouldHGetIntegerWithDefault() {
        when(hashOps.get("key", "field")).thenReturn(Mono.empty());
        Integer result = template.hGetInteger("key", "field", 0).block();
        assertEquals(0, result);
    }

    @Test
    void shouldHGetForWithClass() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just("value"));
        String result = template.hGetFor("key", "field", String.class).block();
        assertEquals("value", result);
    }

    @Test
    void shouldHGetForWithMapper() {
        when(hashOps.get("key", "field")).thenReturn(Mono.just("VALUE"));
        String result = template.hGetFor("key", "field", v -> ((String) v).toLowerCase()).block();
        assertEquals("value", result);
    }

    @Test
    void shouldHHasKey() {
        when(hashOps.hasKey("key", "field")).thenReturn(Mono.just(true));
        Boolean result = template.hHasKey("key", "field").block();
        assertTrue(result);
    }

    @Test
    void shouldHmSet() {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "value1");
        when(hashOps.putAll("key", map)).thenReturn(Mono.just(true));
        Boolean result = template.hmSet("key", map).block();
        assertTrue(result);
    }

    @Test
    void shouldHmSetWithSeconds() {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "value1");
        when(hashOps.putAll("key", map)).thenReturn(Mono.just(true));
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Boolean result = template.hmSet("key", map, 60).block();
        assertTrue(result);
    }

    @Test
    void shouldHmSetWithDuration() {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "value1");
        when(hashOps.putAll("key", map)).thenReturn(Mono.just(true));
        when(reactiveRedisTemplate.expire(eq("key"), any(Duration.class))).thenReturn(Mono.just(true));
        Boolean result = template.hmSet("key", map, Duration.ofSeconds(60)).block();
        assertTrue(result);
    }

    @Test
    void shouldHSize() {
        when(hashOps.size("key")).thenReturn(Mono.just(2L));
        Long result = template.hSize("key").block();
        assertEquals(2L, result);
    }

    @Test
    void shouldHIncrInt() {
        when(hashOps.increment("key", "field", 5)).thenReturn(Mono.just(10L));
        Long result = template.hIncr("key", "field", 5).block();
        assertEquals(10L, result);
    }

    @Test
    void shouldThrowWhenHIncrIntWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", -1).block());
    }

    @Test
    void shouldHIncrLong() {
        when(hashOps.increment("key", "field", 5L)).thenReturn(Mono.just(10L));
        Long result = template.hIncr("key", "field", 5L).block();
        assertEquals(10L, result);
    }

    @Test
    void shouldThrowWhenHIncrLongWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", -1L).block());
    }

    @Test
    void shouldHIncrDouble() {
        when(hashOps.increment("key", "field", 1.5)).thenReturn(Mono.just(3.0));
        Double result = template.hIncr("key", "field", 1.5).block();
        assertEquals(3.0, result);
    }

    @Test
    void shouldThrowWhenHIncrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hIncr("key", "field", -1.0).block());
    }

    @Test
    void shouldHDecrInt() {
        when(hashOps.increment("key", "field", -5)).thenReturn(Mono.just(5L));
        Long result = template.hDecr("key", "field", 5).block();
        assertEquals(5L, result);
    }

    @Test
    void shouldThrowWhenHDecrIntWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", -1).block());
    }

    @Test
    void shouldHDecrLong() {
        when(hashOps.increment("key", "field", -5L)).thenReturn(Mono.just(5L));
        Long result = template.hDecr("key", "field", 5L).block();
        assertEquals(5L, result);
    }

    @Test
    void shouldThrowWhenHDecrLongWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", -1L).block());
    }

    @Test
    void shouldHDecrDouble() {
        when(hashOps.increment("key", "field", -1.5)).thenReturn(Mono.just(1.5));
        Double result = template.hDecr("key", "field", 1.5).block();
        assertEquals(1.5, result);
    }

    @Test
    void shouldThrowWhenHDecrDoubleWithNegativeDelta() {
        assertThrows(RedisOperationException.class, () -> template.hDecr("key", "field", -1.0).block());
    }

    @Test
    void shouldHDel() {
        when(hashOps.remove("key", "field1", "field2")).thenReturn(Mono.just(2L));
        Long result = template.hDel("key", "field1", "field2").block();
        assertEquals(2L, result);
    }

    @Test
    void shouldHKeys() {
        when(hashOps.keys("key")).thenReturn(Flux.just("field1", "field2"));
        List<Object> result = template.hKeys("key").collectList().block();
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    // ==================== List Operations ====================

    @Test
    void shouldLRange() {
        when(listOps.range("key", 0, -1)).thenReturn(Flux.just("a", "b", "c"));
        List<Object> result = template.lRange("key", 0, -1).collectList().block();
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    void shouldLRangeString() {
        when(listOps.range("key", 0, -1)).thenReturn(Flux.just("a", "b"));
        List<String> result = template.lRangeString("key", 0, -1).collectList().block();
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void shouldLIndex() {
        when(listOps.index("key", 0)).thenReturn(Mono.just("value"));
        Object result = template.lIndex("key", 0).block();
        assertEquals("value", result);
    }

    @Test
    void shouldLLeftPush() {
        when(listOps.leftPush("key", "value")).thenReturn(Mono.just(1L));
        Long result = template.lLeftPush("key", "value").block();
        assertEquals(1L, result);
    }

    @Test
    void shouldLLeftPushWithSeconds() {
        when(listOps.leftPush("key", "value")).thenReturn(Mono.just(1L));
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Long result = template.lLeftPush("key", "value", 60).block();
        assertEquals(1L, result);
    }

    @Test
    void shouldLLeftPushAll() {
        when(listOps.leftPushAll(eq("key"), any(Object[].class))).thenReturn(Mono.just(2L));
        Long result = template.lLeftPushAll("key", Arrays.asList("a", "b")).block();
        assertEquals(2L, result);
    }

    @Test
    void shouldLLeftPushx() {
        when(listOps.leftPushIfPresent("key", "value")).thenReturn(Mono.just(1L));
        Long result = template.lLeftPushx("key", "value").block();
        assertEquals(1L, result);
    }

    @Test
    void shouldLLeftPop() {
        when(listOps.leftPop("key")).thenReturn(Mono.just("value"));
        Object result = template.lLeftPop("key").block();
        assertEquals("value", result);
    }

    @Test
    void shouldLLeftPopWithDuration() {
        when(listOps.leftPop(eq("key"), any(Duration.class))).thenReturn(Mono.just("value"));
        Object result = template.lLeftPop("key", Duration.ofSeconds(60)).block();
        assertEquals("value", result);
    }

    @Test
    void shouldLRightPush() {
        when(listOps.rightPush("key", "value")).thenReturn(Mono.just(1L));
        Long result = template.lRightPush("key", "value").block();
        assertEquals(1L, result);
    }

    @Test
    void shouldLRightPushAll() {
        when(listOps.rightPushAll(eq("key"), any(Object[].class))).thenReturn(Mono.just(2L));
        Long result = template.lRightPushAll("key", Arrays.asList("a", "b")).block();
        assertEquals(2L, result);
    }

    @Test
    void shouldLRightPop() {
        when(listOps.rightPop("key")).thenReturn(Mono.just("value"));
        Object result = template.lRightPop("key").block();
        assertEquals("value", result);
    }

    @Test
    void shouldLRightPopWithDuration() {
        when(listOps.rightPop(eq("key"), any(Duration.class))).thenReturn(Mono.just("value"));
        Object result = template.lRightPop("key", Duration.ofSeconds(60)).block();
        assertEquals("value", result);
    }

    @Test
    void shouldLRightPopAndLeftPush() {
        when(listOps.rightPopAndLeftPush("src", "dest")).thenReturn(Mono.just("value"));
        Object result = template.lRightPopAndLeftPush("src", "dest").block();
        assertEquals("value", result);
    }

    @Test
    void shouldLSet() {
        when(listOps.set("key", 0, "value")).thenReturn(Mono.just(true));
        Boolean result = template.lSet("key", 0, "value").block();
        assertTrue(result);
    }

    @Test
    void shouldLSize() {
        when(listOps.size("key")).thenReturn(Mono.just(5L));
        Long result = template.lSize("key").block();
        assertEquals(5L, result);
    }

    @Test
    void shouldLRem() {
        when(listOps.remove("key", 1, "value")).thenReturn(Mono.just(1L));
        Long result = template.lRem("key", 1, "value").block();
        assertEquals(1L, result);
    }

    @Test
    void shouldLTrim() {
        when(listOps.trim("key", 0, 5)).thenReturn(Mono.just(true));
        Boolean result = template.lTrim("key", 0, 5).block();
        assertTrue(result);
    }

    // ==================== Set Operations ====================

    @Test
    void shouldSAdd() {
        when(setOps.add("key", "v1", "v2")).thenReturn(Mono.just(2L));
        Long result = template.sAdd("key", "v1", "v2").block();
        assertEquals(2L, result);
    }

    @Test
    void shouldSAddAndExpire() {
        when(setOps.add("key", "v1")).thenReturn(Mono.just(1L));
        when(reactiveRedisTemplate.expire("key", Duration.ofSeconds(60))).thenReturn(Mono.just(true));
        Long result = template.sAddAndExpire("key", 60, "v1").block();
        assertEquals(1L, result);
    }

    @Test
    void shouldSGet() {
        when(setOps.members("key")).thenReturn(Flux.just("v1", "v2"));
        List<Object> result = template.sGet("key").collectList().block();
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void shouldSHasKey() {
        when(setOps.isMember("key", "value")).thenReturn(Mono.just(true));
        Boolean result = template.sHasKey("key", "value").block();
        assertTrue(result);
    }

    @Test
    void shouldSRemove() {
        when(setOps.remove("key", "v1", "v2")).thenReturn(Mono.just(2L));
        Long result = template.sRemove("key", "v1", "v2").block();
        assertEquals(2L, result);
    }

    @Test
    void shouldSSize() {
        when(setOps.size("key")).thenReturn(Mono.just(5L));
        Long result = template.sSize("key").block();
        assertEquals(5L, result);
    }

    @Test
    void shouldSDiff() {
        when(setOps.difference("key1", "key2")).thenReturn(Flux.just("v1"));
        List<Object> result = template.sDiff("key1", "key2").collectList().block();
        assertNotNull(result);
    }

    @Test
    void shouldSIntersect() {
        when(setOps.intersect("key1", "key2")).thenReturn(Flux.just("v1"));
        List<Object> result = template.sIntersect("key1", "key2").collectList().block();
        assertNotNull(result);
    }

    @Test
    void shouldSUnion() {
        when(setOps.union("key1", "key2")).thenReturn(Flux.just("v1", "v2"));
        List<Object> result = template.sUnion("key1", "key2").collectList().block();
        assertNotNull(result);
    }

    // ==================== ZSet Operations ====================

    @Test
    void shouldZAdd() {
        when(zSetOps.add("key", "value", 1.0)).thenReturn(Mono.just(true));
        Boolean result = template.zAdd("key", "value", 1.0).block();
        assertTrue(result);
    }

    @Test
    void shouldZCard() {
        when(zSetOps.size("key")).thenReturn(Mono.just(5L));
        Long result = template.zCard("key").block();
        assertEquals(5L, result);
    }

    @Test
    void shouldZHas() {
        when(zSetOps.score("key", "value")).thenReturn(Mono.just(1.0));
        Boolean result = template.zHas("key", "value").block();
        assertTrue(result);
    }

    @Test
    void shouldZHasReturnFalseWhenNotExists() {
        when(zSetOps.score("key", "value")).thenReturn(Mono.empty());
        Boolean result = template.zHas("key", "value").block();
        // When score is not found, Mono.empty() is returned, block() returns null
        assertNull(result);
    }

    @Test
    void shouldZIncr() {
        when(zSetOps.incrementScore("key", "value", 1.5)).thenReturn(Mono.just(3.0));
        Double result = template.zIncr("key", "value", 1.5).block();
        assertEquals(3.0, result);
    }

    @Test
    void shouldZRem() {
        when(zSetOps.remove("key", "v1", "v2")).thenReturn(Mono.just(2L));
        Long result = template.zRem("key", "v1", "v2").block();
        assertEquals(2L, result);
    }

    @Test
    void shouldZScore() {
        when(zSetOps.score("key", "value")).thenReturn(Mono.just(1.5));
        Double result = template.zScore("key", "value").block();
        assertEquals(1.5, result);
    }

    @Test
    void shouldZRevRank() {
        when(zSetOps.reverseRank("key", "value")).thenReturn(Mono.just(2L));
        Long result = template.zRevRank("key", "value").block();
        assertEquals(2L, result);
    }

    // ==================== Serialization Methods ====================

    @Test
    void shouldGetRawValueWhenByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap("test".getBytes());
        ByteBuffer result = template.getRawValue(buffer);
        assertSame(buffer, result);
    }

    // ==================== Error Handling ====================

    @Test
    void shouldReturnMonoErrorWhenSetFails() {
        when(valueOps.set(anyString(), any())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.set("key", "value").block());
    }

    @Test
    void shouldReturnMonoErrorWhenGetFails() {
        when(valueOps.get(anyString())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.get("key").block());
    }

    @Test
    void shouldReturnMonoErrorWhenHSetFails() {
        when(hashOps.put(anyString(), any(), any())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.hSet("key", "field", "value").block());
    }

    @Test
    void shouldReturnFluxErrorWhenLRangeFails() {
        when(listOps.range(anyString(), anyLong(), anyLong())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.lRange("key", 0, -1).collectList().block());
    }

    @Test
    void shouldReturnMonoErrorWhenSAddFails() {
        when(setOps.add(anyString(), any())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.sAdd("key", "value").block());
    }

    @Test
    void shouldReturnMonoErrorWhenZAddFails() {
        when(zSetOps.add(anyString(), any(), anyDouble())).thenThrow(new RuntimeException("Redis error"));
        assertThrows(RedisOperationException.class, () -> template.zAdd("key", "value", 1.0).block());
    }
}
