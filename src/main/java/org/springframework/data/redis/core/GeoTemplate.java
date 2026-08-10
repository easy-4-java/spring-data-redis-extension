package org.springframework.data.redis.core;

import io.github.easy4j.redistpl.core.RedisKey;
import lombok.extern.slf4j.Slf4j;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Convenience wrapper around Spring Data Redis' {@link RedisTemplate} that
 * exposes higher-level geographic operations.
 *
 * <p>The class covers the following responsibilities:</p>
 * <ul>
 *   <li>Coordinate registration via {@link #geoAdd(String, double, double)}
 *       and the related overloads.</li>
 *   <li>Distance computation between two coordinates, using either a simple
 *       spherical model or the {@link Ellipsoid} model through the
 *       {@code gavin-harper geodesy} library &mdash; see
 *       {@link #getDistance(double, double, double, double)} and its
 *       overloads.</li>
 *   <li>Geo-radius queries by distance ({@link #getCircleUsersByDistance}) and
 *       by radius ({@link #getCircleUsersByRadius}).</li>
 * </ul>
 *
 * <p>The default storage key is {@link RedisKey#GEO_LOCATION_KEY}, which is
 * passed to {@code RedisTemplate.boundGeoOps} when the template is
 * constructed.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 3.0.0
 * @see RedisKey#GEO_LOCATION_KEY
 */
@Slf4j
public class GeoTemplate extends AbstractOperations<String, Object>  {

	/** Lazily bound operations used by every method on the class. */
	private BoundGeoOperations<String, Object> boundGeoOperations;

	/**
	 * No-arg constructor used when the surrounding framework will inject the
	 * {@link RedisTemplate} later. {@link #boundGeoOperations} remains
	 * {@code null} until then; callers should not invoke any operation before
	 * injection.
	 */
	public GeoTemplate() {
		super(null);
	}

	/**
	 * Builds a {@link GeoTemplate} backed by the supplied {@code redisTemplate}
	 * and binds the default {@link RedisKey#GEO_LOCATION_KEY} key.
	 *
	 * @param redisTemplate the underlying {@link RedisTemplate}; must not be
	 *                      {@code null}
	 */
	public GeoTemplate(RedisTemplate<String, Object> redisTemplate) {
		super(redisTemplate);
		this.boundGeoOperations = redisTemplate.boundGeoOps(RedisKey.GEO_LOCATION_KEY.getKey());
	}

	/**
	 * Computes the great-circle distance between two coordinates using a
	 * simplified spherical model with the Earth mean radius of {@code 6371 km}.
	 *
	 * @param latitude1  the latitude of the first point, in degrees
	 * @param longitude1 the longitude of the first point, in degrees
	 * @param latitude2  the latitude of the second point, in degrees
	 * @param longitude2 the longitude of the second point, in degrees
	 * @return the distance between the points in metres
	 * @see <a href="https://www.cnblogs.com/zhaoyanhaoBlog/p/10121499.html">Reference</a>
	 */
	public double getDistance(double latitude1, double longitude1, double latitude2, double longitude2) {

		double lat1 = (Math.PI / 180) * latitude1;
		double lat2 = (Math.PI / 180) * latitude2;

		double lon1 = (Math.PI / 180) * longitude1;
		double lon2 = (Math.PI / 180) * longitude2;

		// Earth radius in kilometres.
		double R = 6371;

		// Distance between the points in kilometres; multiply by 1000 for metres.
		double d = Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1))
				* R;

		return d * 1000;
	}

	/**
	 * Computes the geodetic distance between two coordinates using the
	 * {@link Ellipsoid#Sphere} model.
	 *
	 * @param latitude1  the latitude of the first point, in degrees
	 * @param longitude1 the longitude of the first point, in degrees
	 * @param latitude2  the latitude of the second point, in degrees
	 * @param longitude2 the longitude of the second point, in degrees
	 * @return the distance between the points in metres
	 */
	public double getSphereDistance(double latitude1, double longitude1, double latitude2, double longitude2) {
		return this.getDistance(Ellipsoid.Sphere, latitude1, longitude1, latitude2, longitude2);
	}

	/**
	 * Computes the geodetic distance between two coordinates using the
	 * {@link Ellipsoid#WGS84} model.
	 *
	 * @param latitude1  the latitude of the first point, in degrees
	 * @param longitude1 the longitude of the first point, in degrees
	 * @param latitude2  the latitude of the second point, in degrees
	 * @param longitude2 the longitude of the second point, in degrees
	 * @return the distance between the points in metres
	 */
	public double getWGS84Distance(double latitude1, double longitude1, double latitude2, double longitude2) {
	    return this.getDistance(Ellipsoid.WGS84, latitude1, longitude1, latitude2, longitude2);
	}

	/**
	 * Computes the geodetic distance between two coordinates using a caller
	 * supplied {@link Ellipsoid} model.
	 *
	 * @param ellipsoid  the ellipsoid model to use (e.g. {@link Ellipsoid#Sphere}
	 *                   or {@link Ellipsoid#WGS84}); must not be {@code null}
	 * @param latitude1  the latitude of the first point, in degrees
	 * @param longitude1 the longitude of the first point, in degrees
	 * @param latitude2  the latitude of the second point, in degrees
	 * @param longitude2 the longitude of the second point, in degrees
	 * @return the distance between the points in metres
	 */
	public double getDistance(Ellipsoid ellipsoid, double latitude1, double longitude1, double latitude2, double longitude2) {

		// Origin coordinates.
		GlobalCoordinates gpsFrom = new GlobalCoordinates(latitude1, longitude1);

		// Destination coordinates.
		GlobalCoordinates gpsTo = new GlobalCoordinates(latitude2, longitude2);

	    // Delegate to the geodesy library using the caller supplied ellipsoid.
	    return this.getDistance(gpsFrom, gpsTo, ellipsoid);

	}

	/**
	 * Computes the geodetic distance between two pre-built
	 * {@link GlobalCoordinates}.
	 *
	 * @param gpsFrom   the origin coordinates; must not be {@code null}
	 * @param gpsTo     the destination coordinates; must not be {@code null}
	 * @param ellipsoid the ellipsoid model to use; must not be {@code null}
	 * @return the ellipsoidal distance in metres, as reported by the geodesy
	 *         library
	 */
	public double getDistance(GlobalCoordinates gpsFrom, GlobalCoordinates gpsTo, Ellipsoid ellipsoid){

        // Run the geodesy calculation and return the ellipsoidal distance.
        GeodeticCurve geoCurve = new GeodeticCalculator().calculateGeodeticCurve(ellipsoid, gpsFrom, gpsTo);

        // Return the ellipsoidal distance (in metres).
        return geoCurve.getEllipsoidalDistance();
    }

    // ===============================Geo=================================

    /**
     * Adds a single geo location to the bound key.
     *
     * @param location the location to add; must not be {@code null}
     * @return the number of entries added (0 or 1)
     */
 	public Long geoAdd(GeoLocation<Object> location) {
 		return getBoundGeoOperations().add(location);
 	}

    /**
     * Adds a batch of geo locations to the bound key.
     *
     * @param locations the locations to add; must not be {@code null}
     * @return the number of entries added
     */
 	public Long geoAdd(Iterable<GeoLocation<Object>> locations) {
 		return getBoundGeoOperations().add(locations);
 	}

    /**
     * Adds a single {@code (point, member)} pair to the bound key.
     *
     * @param point  the location to add; must not be {@code null}
     * @param member the associated member name; must not be {@code null}
     * @return the number of entries added (0 or 1)
     */
 	public Long geoAdd(Point point, Object member) {
 		return getBoundGeoOperations().add(point, member);
 	}

    /**
     * Adds every {@code (member -> point)} entry of {@code memberCoordinateMap}
     * to the bound key.
     *
     * @param memberCoordinateMap the mapping to add; must not be {@code null}
     * @return the number of entries added
     */
 	public Long geoAdd(Map<Object, Point> memberCoordinateMap) {
 		return getBoundGeoOperations().add(memberCoordinateMap);
 	}

    /**
     * Adds (or updates) a user's latest geo coordinates.
     *
     * @param member    the user identifier; must not be {@code null}
     * @param longitude the new longitude, in degrees
     * @param latitude  the new latitude, in degrees
     * @return the number of entries added (0 or 1)
     */
    public Long geoAdd(String member, double longitude, double latitude) {
    	// Example: 89 -> 118.803805, 32.060168.
        Point point = new Point(longitude, latitude);
        return getBoundGeoOperations().add(point, member);
    }

    /**
     * Computes the distance between two user identifiers as a Spring Data
     * {@link Distance} instance.
     *
     * @param uid1 the first user identifier; must not be {@code null}
     * @param uid2 the second user identifier; must not be {@code null}
     * @return the resulting {@link Distance}; never {@code null}
     */
    public Distance distance(String uid1, String uid2) {
    	// Example: 89 -> 118.803805, 32.060168.
    	Distance distance = boundGeoOperations.distance(uid1, uid2);
    	log.info("UserId {} >> UserId {} . distance = {}{}", uid1, uid2, distance.getValue(), distance.getUnit());
    	System.out.println(distance);
    	return distance;
    }

    /**
     * Convenience accessor returning only the numeric portion of
     * {@link #distance(String, String)}.
     *
     * @param uid1 the first user identifier; must not be {@code null}
     * @param uid2 the second user identifier; must not be {@code null}
     * @return the distance value (unit depends on the underlying Redis call)
     */
    public double distanceValue(String uid1, String uid2) {
    	Distance distance = this.distance(uid1, uid2);
    	return distance.getValue();
    }

    /**
     * Returns every geo entry located within {@code distance} metres of
     * {@code uid}'s coordinates, ordered ascending by distance and including
     * coordinates and distance in each result.
     *
     * @param uid      the user identifier whose location anchors the search
     * @param distance the radius, in the configured distance unit
     * @return a {@link GeoResults} container; never {@code null}
     */
    public GeoResults<GeoLocation<Object>> getCircleUsersByDistance(String uid, double distance){

    	// Build geo-radius arguments (include coordinates + distance, sort ascending).
        RedisGeoCommands.GeoRadiusCommandArgs geoRadiusArgs = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs();
        geoRadiusArgs = geoRadiusArgs.includeCoordinates().includeDistance();
        geoRadiusArgs.sortAscending();

        // Issue the radius query against the bound key.
        GeoResults<GeoLocation<Object>> geoResults = boundGeoOperations.radius(uid, new Distance(distance), geoRadiusArgs);

    	return geoResults;

    }

    /**
     * Same as {@link #getCircleUsersByDistance(String, double)} but maps each
     * entry through {@code mapper}.
     *
     * @param <T>    the mapper's output type
     * @param uid    the user identifier whose location anchors the search
     * @param distance the radius, in the configured distance unit
     * @param mapper function applied to every result; must not be {@code null}
     * @return a list of mapped results; empty (never {@code null}) if no entry
     *         matches
     */
    public <T> List<T> getCircleUsersByDistance(String uid, double distance, Function<GeoResult<GeoLocation<Object>>, T> mapper){

        // Run the radius query.
        GeoResults<GeoLocation<Object>> geoResults = this.getCircleUsersByDistance(uid, distance);

        // Map the results; return an empty list when the container is empty.
        List<GeoResult<GeoLocation<Object>>> geoResultList = geoResults.getContent();
        if (CollectionUtils.isEmpty(geoResultList)) {
			return new ArrayList<>();
		}
    	return geoResultList.stream().map(mapper).collect(Collectors.toList());

    }

    /**
     * Returns every geo entry located within the {@code radius} bounding
     * circle centred on {@code uid}'s coordinates, ordered ascending by
     * distance and including coordinates and distance in each result.
     *
     * @param uid    the user identifier whose location anchors the search
     * @param radius the radius of the bounding circle (in the same unit as
     *               {@code uid}'s coordinates)
     * @return a {@link GeoResults} container; never {@code null}
     */
    public GeoResults<GeoLocation<Object>> getCircleUsersByRadius(String uid, double radius){

    	// Build the bounding circle from the user's coordinates.
        Circle within = new Circle(boundGeoOperations.position(uid).get(0), radius);
        // Build geo-radius arguments (include coordinates + distance, sort ascending).
        RedisGeoCommands.GeoRadiusCommandArgs geoRadiusArgs = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs();
        geoRadiusArgs = geoRadiusArgs.includeCoordinates().includeDistance();
        geoRadiusArgs.sortAscending();

        // Issue the radius query.
        GeoResults<GeoLocation<Object>> geoResults = boundGeoOperations.radius(within, geoRadiusArgs);

    	return geoResults;
    }

    /**
     * Same as {@link #getCircleUsersByRadius(String, double)} but maps each
     * entry through {@code mapper}.
     *
     * @param <T>    the mapper's output type
     * @param uid    the user identifier whose location anchors the search
     * @param radius the radius of the bounding circle
     * @param mapper function applied to every result; must not be {@code null}
     * @return a list of mapped results; empty (never {@code null}) if no entry
     *         matches
     */
    public <T> List<T> getCircleUsersByRadius(String uid, double radius, Function<GeoResult<GeoLocation<Object>>, T> mapper){
        // Run the radius query.
        GeoResults<GeoLocation<Object>> geoResults = this.getCircleUsersByRadius(uid, radius);
        // Map the results; return an empty list when the container is empty.
        List<GeoResult<GeoLocation<Object>>> geoResultList = geoResults.getContent();
        if (CollectionUtils.isEmpty(geoResultList)) {
			return new ArrayList<>();
		}
    	return geoResultList.stream().map(mapper).collect(Collectors.toList());
    }

    /**
     * Returns the {@link BoundGeoOperations} that back this template.
     *
     * @return the bound operations instance; may be {@code null} when the
     *         template was built via the no-arg constructor and has not yet
     *         been wired to a {@link RedisTemplate}
     */
    public BoundGeoOperations<String, Object> getBoundGeoOperations() {
		return boundGeoOperations;
	}

}