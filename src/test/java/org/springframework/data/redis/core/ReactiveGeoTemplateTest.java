package org.springframework.data.redis.core;

import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GlobalCoordinates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ReactiveGeoTemplate}.
 */
class ReactiveGeoTemplateTest {

    @SuppressWarnings("unchecked")
    private ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate;
    private ReactiveGeoOperations<Object, Object> geoOps;
    private ReactiveGeoTemplate reactiveGeoTemplate;

    @BeforeEach
    void setUp() {
        reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        geoOps = mock(ReactiveGeoOperations.class);
        when(reactiveRedisTemplate.opsForGeo()).thenReturn(geoOps);
        reactiveGeoTemplate = new ReactiveGeoTemplate(reactiveRedisTemplate);
    }

    @Test
    void shouldCreateWithNoArgConstructor() {
        ReactiveGeoTemplate template = new ReactiveGeoTemplate();
        assertNull(template.getReactiveRedisTemplate());
    }

    @Test
    void shouldCreateWithReactiveRedisTemplate() {
        assertNotNull(reactiveGeoTemplate.getReactiveRedisTemplate());
    }

    @Test
    void shouldReturnReactiveRedisTemplate() {
        assertSame(reactiveRedisTemplate, reactiveGeoTemplate.getReactiveRedisTemplate());
    }

    @Test
    void shouldComputeSimpleDistance() {
        double distance = reactiveGeoTemplate.getDistance(32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
        assertTrue(distance > 800000 && distance < 1100000);
    }

    @Test
    void shouldComputeSphereDistance() {
        double distance = reactiveGeoTemplate.getSphereDistance(32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeWGS84Distance() {
        double distance = reactiveGeoTemplate.getWGS84Distance(32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeDistanceWithEllipsoid() {
        double distance = reactiveGeoTemplate.getDistance(Ellipsoid.WGS84, 32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeDistanceBetweenGlobalCoordinates() {
        GlobalCoordinates from = new GlobalCoordinates(32.060168, 118.803805);
        GlobalCoordinates to = new GlobalCoordinates(39.9042, 116.4074);
        double distance = reactiveGeoTemplate.getDistance(from, to, Ellipsoid.WGS84);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeZeroDistanceForSamePoint() {
        double distance = reactiveGeoTemplate.getDistance(32.0, 118.0, 32.0, 118.0);
        assertEquals(0.0, distance, 1.0);
    }

    @Test
    void shouldSetLocation() {
        when(geoOps.add(anyString(), any(Point.class), any())).thenReturn(Mono.just(1L));

        reactiveGeoTemplate.setLocation("user1", 118.803805, 32.060168);

        verify(geoOps).add(anyString(), any(Point.class), eq("user1"));
    }

    @Test
    void shouldGetDistance() {
        Distance distance = new Distance(100.0, Metrics.KILOMETERS);
        when(geoOps.distance(anyString(), any(), any())).thenReturn(Mono.just(distance));

        String result = reactiveGeoTemplate.distance("user1", "user2");
        assertNotNull(result);
    }

    @Test
    void shouldGetCircleUsersByDistance() {
        GeoResult<GeoLocation<Object>> geoResult = new GeoResult<>(
                new GeoLocation<>("user2", new Point(119.0, 33.0)),
                new Distance(50.0, Metrics.KILOMETERS));
        when(geoOps.radius(anyString(), any(), any(Distance.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(Flux.just(geoResult));

        Function<GeoResult<GeoLocation<Object>>, String> mapper = r -> r.getContent().getName().toString();
        Flux<String> flux = reactiveGeoTemplate.getCircleUsersByDistance("user1", 100.0, mapper);

        List<String> result = flux.collectList().block();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("user2", result.get(0));
    }
}
