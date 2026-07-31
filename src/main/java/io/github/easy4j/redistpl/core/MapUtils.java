package io.github.easy4j.redistpl.core;

import java.util.Map;

/**
 * Map 工具类，提供对 Map 操作的空安全便捷方法。
 *
 * @author wandl
 */
public class MapUtils {

    /**
     * Gets a String from a Map in a null-safe manner.
     * <p>
     * The String is obtained via <code>toString</code>.
     *
     * @param map  the map to use
     * @param key  the key to look up
     * @return the value in the Map as a String, <code>null</code> if null map input
     */
    public static String getString(final Map map, final Object key) {
        if (map != null) {
            Object answer = map.get(key);
            if (answer != null) {
                return answer.toString();
            }
        }
        return null;
    }

}
