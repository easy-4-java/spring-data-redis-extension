package org.springframework.data.redis.core;

import io.github.easy4j.redistpl.core.RedisKey;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GlobalCoordinates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link GeoTemplate}.
 */
class GeoTemplateTest {

    private RedisTemplate<String, Object> redisTemplate;
    private BoundGeoOperations<String, Object> boundGeoOps;
    private GeoTemplate geoTemplate;

    @BeforeEach
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        boundGeoOps = mock(BoundGeoOperations.class);
        when(redisTemplate.boundGeoOps(anyString())).thenReturn(boundGeoOps);
        geoTemplate = new GeoTemplate(redisTemplate);
    }

    @Test
    void shouldCreateWithNoArgConstructor() {
        GeoTemplate template = new GeoTemplate();
        assertNull(template.getBoundGeoOperations());
    }

    @Test
    void shouldCreateWithRedisTemplate() {
        assertNotNull(geoTemplate.getBoundGeoOperations());
    }

    @Test
    void shouldReturnBoundGeoOperations() {
        assertSame(boundGeoOps, geoTemplate.getBoundGeoOperations());
    }

    @Test
    void shouldComputeSimpleDistance() {
        double distance = geoTemplate.getDistance(32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
        // Beijing to Nanjing is roughly 900km
        assertTrue(distance > 800000 && distance < 1100000);
    }

    @Test
    void shouldComputeSphereDistance() {
        double distance = geoTemplate.getSphereDistance(32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeWGS84Distance() {
        double distance = geoTemplate.getWGS84Distance(32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeDistanceWithEllipsoid() {
        double distance = geoTemplate.getDistance(Ellipsoid.WGS84, 32.060168, 118.803805, 39.9042, 116.4074);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeDistanceBetweenGlobalCoordinates() {
        GlobalCoordinates from = new GlobalCoordinates(32.060168, 118.803805);
        GlobalCoordinates to = new GlobalCoordinates(39.9042, 116.4074);
        double distance = geoTemplate.getDistance(from, to, Ellipsoid.WGS84);
        assertTrue(distance > 0);
    }

    @Test
    void shouldComputeZeroDistanceForSamePoint() {
        double distance = geoTemplate.getDistance(32.0, 118.0, 32.0, 118.0);
        assertEquals(0.0, distance, 1.0);
    }

    @Test
    void shouldAddGeoLocation() {
        GeoLocation<Object> location = new GeoLocation<>("user1", new Point(118.0, 32.0));
        when(boundGeoOps.add(location)).thenReturn(1L);

        Long result = geoTemplate.geoAdd(location);
        assertEquals(1L, result);
    }

    @Test
    void shouldAddGeoLocations() {
        GeoLocation<Object> loc1 = new GeoLocation<>("user1", new Point(118.0, 32.0));
        GeoLocation<Object> loc2 = new GeoLocation<>("user2", new Point(119.0, 33.0));
        when(boundGeoOps.add(anyIterable())).thenReturn(2L);

        Long result = geoTemplate.geoAdd(Arrays.asList(loc1, loc2));
        assertEquals(2L, result);
    }

    @Test
    void shouldAddPointAndMember() {
        Point point = new Point(118.0, 32.0);
        when(boundGeoOps.add(point, "user1")).thenReturn(1L);

        Long result = geoTemplate.geoAdd(point, "user1");
        assertEquals(1L, result);
    }

    @Test
    void shouldAddMemberCoordinateMap() {
        java.util.Map<Object, Point> map = new java.util.HashMap<>();
        map.put("user1", new Point(118.0, 32.0));
        when(boundGeoOps.add(map)).thenReturn(1L);

        Long result = geoTemplate.geoAdd(map);
        assertEquals(1L, result);
    }

    @Test
    void shouldAddWithMemberAndCoordinates() {
        when(boundGeoOps.add(any(Point.class), eq("user1"))).thenReturn(1L);

        Long result = geoTemplate.geoAdd("user1", 118.803805, 32.060168);
        assertEquals(1L, result);
    }

    @Test
    void shouldGetDistanceBetweenMembers() {
        Distance distance = new Distance(100.0, Metrics.KILOMETERS);
        when(boundGeoOps.distance("user1", "user2")).thenReturn(distance);

        Distance result = geoTemplate.distance("user1", "user2");
        assertNotNull(result);
        assertEquals(100.0, result.getValue());
    }

    @Test
    void shouldGetDistanceValue() {
        Distance distance = new Distance(100.0, Metrics.KILOMETERS);
        when(boundGeoOps.distance("user1", "user2")).thenReturn(distance);

        double result = geoTemplate.distanceValue("user1", "user2");
        assertEquals(100.0, result);
    }

    @Test
    void shouldGetCircleUsersByDistance() {
        GeoResult<GeoLocation<Object>> geoResult = new GeoResult<>(
                new GeoLocation<>("user2", new Point(119.0, 33.0)),
                new Distance(50.0, Metrics.KILOMETERS));
        GeoResults<GeoLocation<Object>> geoResults = new GeoResults<>(Collections.singletonList(geoResult));
        when(boundGeoOps.radius(anyString(), any(Distance.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(geoResults);

        GeoResults<GeoLocation<Object>> result = geoTemplate.getCircleUsersByDistance("user1", 100.0);
        assertNotNull(result);
        assertEquals(1, result.getContent().size());
    }

    @Test
    void shouldGetCircleUsersByDistanceWithMapper() {
        GeoResult<GeoLocation<Object>> geoResult = new GeoResult<>(
                new GeoLocation<>("user2", new Point(119.0, 33.0)),
                new Distance(50.0, Metrics.KILOMETERS));
        GeoResults<GeoLocation<Object>> geoResults = new GeoResults<>(Collections.singletonList(geoResult));
        when(boundGeoOps.radius(anyString(), any(Distance.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(geoResults);

        List<String> result = geoTemplate.getCircleUsersByDistance("user1", 100.0, r -> r.getContent().getName().toString());
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("user2", result.get(0));
    }

    @Test
    void shouldReturnEmptyListWhenNoCircleUsersByDistance() {
        GeoResults<GeoLocation<Object>> geoResults = new GeoResults<>(Collections.emptyList());
        when(boundGeoOps.radius(anyString(), any(Distance.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(geoResults);

        List<String> result = geoTemplate.getCircleUsersByDistance("user1", 100.0, r -> r.getContent().getName().toString());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldGetCircleUsersByRadius() {
        Point userPoint = new Point(118.0, 32.0);
        when(boundGeoOps.position("user1")).thenReturn(Collections.singletonList(userPoint));

        GeoResult<GeoLocation<Object>> geoResult = new GeoResult<>(
                new GeoLocation<>("user2", new Point(119.0, 33.0)),
                new Distance(50.0, Metrics.KILOMETERS));
        GeoResults<GeoLocation<Object>> geoResults = new GeoResults<>(Collections.singletonList(geoResult));
        when(boundGeoOps.radius(any(Circle.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(geoResults);

        GeoResults<GeoLocation<Object>> result = geoTemplate.getCircleUsersByRadius("user1", 100.0);
        assertNotNull(result);
    }

    @Test
    void shouldGetCircleUsersByRadiusWithMapper() {
        Point userPoint = new Point(118.0, 32.0);
        when(boundGeoOps.position("user1")).thenReturn(Collections.singletonList(userPoint));

        GeoResult<GeoLocation<Object>> geoResult = new GeoResult<>(
                new GeoLocation<>("user2", new Point(119.0, 33.0)),
                new Distance(50.0, Metrics.KILOMETERS));
        GeoResults<GeoLocation<Object>> geoResults = new GeoResults<>(Collections.singletonList(geoResult));
        when(boundGeoOps.radius(any(Circle.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(geoResults);

        List<String> result = geoTemplate.getCircleUsersByRadius("user1", 100.0, r -> r.getContent().getName().toString());
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoCircleUsersByRadius() {
        Point userPoint = new Point(118.0, 32.0);
        when(boundGeoOps.position("user1")).thenReturn(Collections.singletonList(userPoint));

        GeoResults<GeoLocation<Object>> geoResults = new GeoResults<>(Collections.emptyList());
        when(boundGeoOps.radius(any(Circle.class), any(RedisGeoCommands.GeoRadiusCommandArgs.class)))
                .thenReturn(geoResults);

        List<String> result = geoTemplate.getCircleUsersByRadius("user1", 100.0, r -> r.getContent().getName().toString());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
