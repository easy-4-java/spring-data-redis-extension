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
 * 响应式 Redis 地理位置操作模板类，基于 {@link ReactiveRedisTemplate} 的 Geo 操作封装。
 * <p>
 * 提供了基于 Project Reactor 的响应式地理位置操作方法，适用于 WebFlux 等响应式编程场景。
 * 功能包括：
 * <ul>
 *   <li>坐标设置：{@link #setLocation(String, double, double)}</li>
 *   <li>距离计算：{@link #distance(String, String)}、{@link #getDistance(double, double, double, double)}</li>
 *   <li>范围查询：{@link #getCircleUsersByDistance(String, double, Function)}</li>
 *   <li>坐标系距离计算：支持 Sphere 和 WGS84 两种坐标系</li>
 * </ul>
 *
 * @author wandl
 * @see GeoTemplate
 * @see RedisKey#GEO_LOCATION_KEY
 */
public class ReactiveGeoTemplate {

	private final static String USER_GEO_KEY = RedisKey.GEO_LOCATION_KEY.getKey();
	private ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate;

	/**
	 * 无参构造方法（需后续手动设置 ReactiveRedisTemplate）
	 */
	public ReactiveGeoTemplate() {
		super();
	}

	/**
	 * 构造 ReactiveGeoTemplate 实例
	 *
	 * @param reactiveRedisTemplate 响应式 RedisTemplate 实例
	 */
	public ReactiveGeoTemplate(ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate) {
		super();
		this.reactiveRedisTemplate = reactiveRedisTemplate;
	}

	/**
	 * 计算两点之间距离 https://www.cnblogs.com/zhaoyanhaoBlog/p/10121499.html
	 * @param longitude1	：坐标1经度
	 * @param latitude1		：坐标1维度
	 * @param longitude2	：坐标2经度
	 * @param latitude2		：坐标2维度
	 * @return 计算结果（单位：米）
	 */
	public double getDistance(double latitude1, double longitude1, double latitude2, double longitude2) {

		double lat1 = (Math.PI / 180) * latitude1;
		double lat2 = (Math.PI / 180) * latitude2;

		double lon1 = (Math.PI / 180) * longitude1;
		double lon2 = (Math.PI / 180) * longitude2;

//      double Lat1r = (Math.PI/180)*(gp1.getLatitudeE6()/1E6);
//      double Lat2r = (Math.PI/180)*(gp2.getLatitudeE6()/1E6);
//      double Lon1r = (Math.PI/180)*(gp1.getLongitudeE6()/1E6);
//      double Lon2r = (Math.PI/180)*(gp2.getLongitudeE6()/1E6);

		// 地球半径
		double R = 6371;

		// 两点间距离 km，如果想要米的话，结果*1000就可以了
		double d = Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1))
				* R;

		return d * 1000;
	}

	/**
	 * 1、计算Sphere模式下两个坐标点的距离（单位：米）
	 * @param longitude1	：坐标1经度
	 * @param latitude1		：坐标1维度
	 * @param longitude2	：坐标2经度
	 * @param latitude2		：坐标2维度
	 * @return 计算结果（单位：米）
	 */
	public double getSphereDistance(double latitude1, double longitude1, double latitude2, double longitude2) {
		return this.getDistance(Ellipsoid.Sphere, latitude1, longitude1, latitude2, longitude2);
	}

	/**
	 * 2、计算WGS84模式下两个坐标点的距离（单位：米）
	 * @param longitude1	：坐标1经度
	 * @param latitude1		：坐标1维度
	 * @param longitude2	：坐标2经度
	 * @param latitude2		：坐标2维度
	 * @return	计算结果（单位：米）
	 */
	public double getWGS84Distance(double latitude1, double longitude1, double latitude2, double longitude2) {
	    return this.getDistance(Ellipsoid.WGS84, latitude1, longitude1, latitude2, longitude2);
	}

	/**
	 * 2、计算指定模式下两个坐标点的距离（单位：米）
	 * @param ellipsoid	：计算坐标的模式
	 * @param longitude1	：坐标1经度
	 * @param latitude1		：坐标1维度
	 * @param longitude2	：坐标2经度
	 * @param latitude2		：坐标2维度
	 * @return	计算结果（单位：米）
	 */
	public double getDistance(Ellipsoid ellipsoid, double latitude1, double longitude1, double latitude2, double longitude2) {

		// 1、此处可以传入起始点经纬度
		GlobalCoordinates gpsFrom = new GlobalCoordinates(latitude1, longitude1);

		// 2、此处可以传入目标点经纬度
		GlobalCoordinates gpsTo = new GlobalCoordinates(latitude2, longitude2);

	    // 3、调用计算方法，传入坐标系、经纬度用于计算距离
	    return this.getDistance(gpsFrom, gpsTo, ellipsoid);

	}

	public double getDistance(GlobalCoordinates gpsFrom, GlobalCoordinates gpsTo, Ellipsoid ellipsoid){

        // 1、创建GeodeticCalculator，调用计算方法，传入坐标系、经纬度用于计算距离
        GeodeticCurve geoCurve = new GeodeticCalculator().calculateGeodeticCurve(ellipsoid, gpsFrom, gpsTo);

        // 2、获取计算结果
        return geoCurve.getEllipsoidalDistance();
    }


    /**
     *
     * @param uid 用户ID
     * @param longitude  用户最新位置经度
     * @param latitude  用户最新位置纬度
     */
    public void setLocation(String uid, double longitude, double latitude) {
    	// 例：89 118.803805,32.060168
        Point point = new Point(longitude, latitude);
        getReactiveRedisTemplate().opsForGeo().add(USER_GEO_KEY, point, uid);
    }

    public String distance(String uid1, String uid2) {
    	// 例：89 118.803805,32.060168
    	return getReactiveRedisTemplate().opsForGeo().distance(USER_GEO_KEY, uid1, uid2)
    			.map(obj -> String.valueOf(obj.getValue() + obj.getUnit()))
				.cast(String.class)
				.block();
    }

    public <T> Flux<T> getCircleUsersByDistance(String uid, double distance, Function<GeoResult<GeoLocation<Object>>, T> mapper){
    	 // 1.1、设置geo查询参数
        RedisGeoCommands.GeoRadiusCommandArgs geoRadiusArgs = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs();
        // 1.2、查询返回结果包括距离和坐标
        geoRadiusArgs = geoRadiusArgs.includeCoordinates().includeDistance();
        // 1.3、按查询出的坐标距离中心坐标的距离进行排序
        geoRadiusArgs.sortAscending();

        // 2、根据给定地理位置获取指定范围内的地理位置集合
        return getReactiveRedisTemplate().opsForGeo()
        		  .radius(USER_GEO_KEY, uid, new Distance(distance), geoRadiusArgs)
       					.map(geoResult -> mapper.apply(geoResult));
    }
    /*
    public <T> List<T> getCircleUsersByRadius(String uid, double radius, Function<GeoResult<GeoLocation<Object>>, T> mapper){


    	// 1、根据UID查询指定UID对应坐标点指定范围内的用户
        Circle within = new Circle(boundGeoOperations.position(uid).get(0), radius);
        // 1.1、设置geo查询参数
        RedisGeoCommands.GeoRadiusCommandArgs geoRadiusArgs = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs();
        // 1.2、查询返回结果包括距离和坐标
        geoRadiusArgs = geoRadiusArgs.includeCoordinates().includeDistance();
        // 1.3、按查询出的坐标距离中心坐标的距离进行排序
        geoRadiusArgs.sortAscending();
        //geoRadiusArgs.limit(limit);
        // 2、执行查询操作
        GeoResults<GeoLocation<Object>> geoResults = boundGeoOperations.radius(within, geoRadiusArgs);

        // 3、解析结果判断
        List<GeoResult<GeoLocation<Object>>> geoResultList = geoResults.getContent();
        if (CollectionUtils.isEmpty(geoResultList)) {
			return new ArrayList<>();
		}
    	return geoResultList.stream().map(mapper).collect(Collectors.toList());
    }*/

	/**
	 * 获取底层响应式 RedisTemplate 实例
	 *
	 * @return ReactiveRedisTemplate 实例
	 */
    public ReactiveRedisTemplate<Object, Object> getReactiveRedisTemplate() {
		return reactiveRedisTemplate;
	}

}
