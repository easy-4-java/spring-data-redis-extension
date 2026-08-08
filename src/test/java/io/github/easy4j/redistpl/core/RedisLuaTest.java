package io.github.easy4j.redistpl.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RedisLua}.
 */
class RedisLuaTest {

    @Test
    void shouldHaveLockLuaScript() {
        assertNotNull(RedisLua.LOCK_LUA_SCRIPT);
        assertTrue(RedisLua.LOCK_LUA_SCRIPT.contains("setnx"));
        assertTrue(RedisLua.LOCK_LUA_SCRIPT.contains("pexpire"));
    }

    @Test
    void shouldHaveUnlockLuaScript() {
        assertNotNull(RedisLua.UNLOCK_LUA_SCRIPT);
        assertTrue(RedisLua.UNLOCK_LUA_SCRIPT.contains("get"));
        assertTrue(RedisLua.UNLOCK_LUA_SCRIPT.contains("del"));
    }

    @Test
    void shouldHaveIncrScript() {
        assertNotNull(RedisLua.INCR_SCRIPT);
        assertTrue(RedisLua.INCR_SCRIPT.contains("EXISTS"));
        assertTrue(RedisLua.INCR_SCRIPT.contains("INCRBY"));
    }

    @Test
    void shouldHaveDecrScript() {
        assertNotNull(RedisLua.DECR_SCRIPT);
        assertTrue(RedisLua.DECR_SCRIPT.contains("EXISTS"));
        assertTrue(RedisLua.DECR_SCRIPT.contains("INCRBY"));
    }

    @Test
    void shouldHaveDivScript() {
        assertNotNull(RedisLua.DIV_SCRIPT);
        assertTrue(RedisLua.DIV_SCRIPT.contains("EXISTS"));
    }

    @Test
    void shouldHaveIncrByfloatScript() {
        assertNotNull(RedisLua.INCR_BYFLOAT_SCRIPT);
        assertTrue(RedisLua.INCR_BYFLOAT_SCRIPT.contains("INCRBYFLOAT"));
    }

    @Test
    void shouldHaveDecrByfloatScript() {
        assertNotNull(RedisLua.DECR_BYFLOAT_SCRIPT);
        assertTrue(RedisLua.DECR_BYFLOAT_SCRIPT.contains("INCRBYFLOAT"));
    }

    @Test
    void shouldHaveHincrScript() {
        assertNotNull(RedisLua.HINCR_SCRIPT);
        assertTrue(RedisLua.HINCR_SCRIPT.contains("HEXISTS"));
        assertTrue(RedisLua.HINCR_SCRIPT.contains("HINCRBY"));
    }

    @Test
    void shouldHaveHdecrScript() {
        assertNotNull(RedisLua.HDECR_SCRIPT);
        assertTrue(RedisLua.HDECR_SCRIPT.contains("HEXISTS"));
        assertTrue(RedisLua.HDECR_SCRIPT.contains("HINCRBY"));
    }

    @Test
    void shouldHaveHdivScript() {
        assertNotNull(RedisLua.HDIV_SCRIPT);
        assertTrue(RedisLua.HDIV_SCRIPT.contains("HEXISTS"));
    }

    @Test
    void shouldHaveHincrByfloatScript() {
        assertNotNull(RedisLua.HINCR_BYFLOAT_SCRIPT);
        assertTrue(RedisLua.HINCR_BYFLOAT_SCRIPT.contains("HINCRBYFLOAT"));
    }

    @Test
    void shouldHaveHdecrByfloatScript() {
        assertNotNull(RedisLua.HDECR_BYFLOAT_SCRIPT);
        assertTrue(RedisLua.HDECR_BYFLOAT_SCRIPT.contains("HINCRBYFLOAT"));
    }
}
