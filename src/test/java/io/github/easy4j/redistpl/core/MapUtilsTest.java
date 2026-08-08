package io.github.easy4j.redistpl.core;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link MapUtils}.
 */
class MapUtilsTest {

    @Test
    void shouldReturnNullWhenMapIsNull() {
        assertNull(MapUtils.getString(null, "key"));
    }

    @Test
    void shouldReturnNullWhenKeyDoesNotExist() {
        Map<String, String> map = new HashMap<>();
        assertNull(MapUtils.getString(map, "missing"));
    }

    @Test
    void shouldReturnStringValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "hello");
        assertEquals("hello", MapUtils.getString(map, "name"));
    }

    @Test
    void shouldReturnToStringForNonStringValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("count", 42);
        assertEquals("42", MapUtils.getString(map, "count"));
    }

    @Test
    void shouldReturnNullWhenValueIsNull() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", null);
        assertNull(MapUtils.getString(map, "key"));
    }

    @Test
    void shouldHandleNullKey() {
        Map<String, String> map = new HashMap<>();
        assertNull(MapUtils.getString(map, null));
    }

    @Test
    void shouldReturnStringForDoubleValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("pi", 3.14);
        assertEquals("3.14", MapUtils.getString(map, "pi"));
    }
}
