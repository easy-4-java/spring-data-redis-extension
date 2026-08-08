package io.github.easy4j.redistpl.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RedisKeyConstant}.
 */
class RedisKeyConstantTest {

    @Test
    void shouldHaveGeoLocationKey() {
        assertEquals("geo:location", RedisKeyConstant.GEO_LOCATION_KEY);
    }

    @Test
    void shouldHaveIpRegionKey() {
        assertEquals("ip:region", RedisKeyConstant.IP_REGION_KEY);
    }

    @Test
    void shouldHaveIpLocationKey() {
        assertEquals("ip:location", RedisKeyConstant.IP_LOCATION_KEY);
    }

    @Test
    void shouldHaveIpBaiduLocationKey() {
        assertEquals("baidu:ip:location", RedisKeyConstant.IP_BAIDU_LOCATION_KEY);
    }

    @Test
    void shouldHaveIpPconlineLocationKey() {
        assertEquals("pconline:ip:location", RedisKeyConstant.IP_PCONLINE_LOCATION_KEY);
    }
}
