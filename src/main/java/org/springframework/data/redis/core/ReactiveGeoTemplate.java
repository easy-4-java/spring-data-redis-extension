package org.springframework.data.redis.core;

import io.github.easy4j.redistpl.core.RedisKey;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * Reactive counterpart of {@link GeoTemplate}, wrapping a
 * {@link ReactiveRedisTemplate} and returning Project Reactor publishers for
 * every geo-aware operation.
 *
 * <p>The class is intended for WebFlux-style applications and exposes:</p>
 * <ul>
 *   <li>Coordinate registration via {@link #setLocation(String, double, double)}.</li>
 *   <li>Distance calculation between two user identifiers
 *       ({@link #distance(String, String)}) and helper methods that reuse the
 *       same geodesy helpers as {@link GeoTemplate}.</li>
 *   <li>Geo-radius queries via
 *       {@link #getCircleUsersByDistance(String, double, Function)}.</li>
 * </ul>
 *
 * <p>The default storage key is {@link RedisKey#GEO_LOCATION_KEY}.</p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 3.0.0
 * @see GeoTemplate
 * @see RedisKey#GEO_LOCATION_KEY
 */
public class ReactiveGeoTemplate {

	/** The default Redis key used by every operation on this template. */
	private final static String USER_GEO_KEY = RedisKey.GEO_LOCATION_KEY.getKey();

	/** Backing reactive Redis template; may be {@code null} until injected. */
	private ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate;

	/**
	 * No-arg constructor. The underlying
	 * {@link ReactiveRedisTemplate} must be supplied via the
	 * {@link #ReactiveGeoTemplate(ReactiveRedisTemplate)} constructor or by an
	 * external setter before any operation is invoked.
	 */
	public ReactiveGeoTemplate() {
		super();
	}

	/**
	 * Builds a {@link ReactiveGeoTemplate} backed by the supplied reactive
	 * Redis template.
	 *
	 * @param reactiveRedisTemplate the reactive template; may be {@code null}
	 *                              and injected later via a setter
	 */
	public ReactiveGeoTemplate(ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate) {
		super();
		this.reactiveRedisTemplate = reactiveRedisTemplate;
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
	 * @return the ellipsoidal distance in metres
	 */
	public double getDistance(GlobalCoordinates gpsFrom, GlobalCoordinates gpsTo, Ellipsoid ellipsoid){

        // Run the geodesy calculation and return the ellipsoidal distance.
        GeodeticCurve geoCurve = new GeodeticCalculator().calculateGeodeticCurve(ellipsoid, gpsFrom, gpsTo);

        // Return the ellipsoidal distance (in metres).
        return geoCurve.getEllipsoidalDistance();
    }


    /**
     * Registers (or updates) {@code uid}'s latest coordinates against the
     * default user-geolocation key.
     *
     * @param uid       the user identifier; must not be {@code null}
     * @param longitude the new longitude, in degrees
     * @param latitude  the new latitude, in degrees
     */
    public void setLocation(String uid, double longitude, double latitude) {
    	// Example: 89 -> 118.803805, 32.060168.
        Point point = new Point(longitude, latitude);
        getReactiveRedisTemplate().opsForGeo().add(USER_GEO_KEY, point, uid);
    }

    /**
     * Returns the distance between two user identifiers as a formatted
     * {@code "<value><unit>"} string. The call blocks until the underlying
     * {@link Mono} completes.
     *
     * @param uid1 the first user identifier; must not be {@code null}
     * @param uid2 the second user identifier; must not be {@code null}
     * @return a {@code "<value><unit>"} string, or {@code null} if Redis
     *         returned no distance
     */
    public String distance(String uid1, String uid2) {
    	// Example: 89 -> 118.803805, 32.060168.
    	return getReactiveRedisTemplate().opsForGeo().distance(USER_GEO_KEY, uid1, uid2)
    			.map(obj -> String.valueOf(obj.getValue() + obj.getUnit()))
				.cast(String.class)
				.block();
    }

    /**
     * Returns every geo entry located within {@code distance} metres of
     * {@code uid}'s coordinates, applying {@code mapper} to each result.
     *
     * @param <T>      the mapper's output type
     * @param uid      the user identifier whose location anchors the search
     * @param distance the radius of the search
     * @param mapper   function applied to every result; must not be
     *                 {@code null}
     * @return a {@link Flux} emitting the mapped results; never {@code null}
     */
    public <T> Flux<T> getCircleUsersByDistance(String uid, double distance, Function<GeoResult<GeoLocation<Object>>, T> mapper){
    	 // Build geo-radius arguments (include coordinates + distance, sort ascending).
        RedisGeoCommands.GeoRadiusCommandArgs geoRadiusArgs = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs();
        geoRadiusArgs = geoRadiusArgs.includeCoordinates().includeDistance();
        geoRadiusArgs.sortAscending();

        // Issue the radius query and apply the caller supplied mapper.
        return getReactiveRedisTemplate().opsForGeo()
        		  .radius(USER_GEO_KEY, uid, new Distance(distance), geoRadiusArgs)
       					.map(geoResult -> mapper.apply(geoResult));
    }

	/**
	 * Returns the underlying {@link ReactiveRedisTemplate} used by this
	 * template.
	 *
	 * @return the reactive template; may be {@code null} when no template has
	 *         been injected
	 */
    public ReactiveRedisTemplate<Object, Object> getReactiveRedisTemplate() {
		return reactiveRedisTemplate;
	}

}