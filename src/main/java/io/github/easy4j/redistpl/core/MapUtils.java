package io.github.easy4j.redistpl.core;

import java.util.Map;

/**
 * Null-safe convenience helpers for {@link Map} lookups.
 *
 * <p>This utility type complements the {@code spring-data-redis-extension}
 * templates by providing defensive accessors that tolerate {@code null} map
 * instances and {@code null} values without throwing {@link NullPointerException}.
 * It intentionally re-uses the semantics of the Apache Commons
 * {@code MapUtils.getString} contract so call sites that were previously
 * written against that API can keep working unchanged.</p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 3.0.0
 */
public class MapUtils {

	/**
	 * Returns the string representation of a value stored in {@code map} under
	 * {@code key}, or {@code null} if either the map itself, or the entry, is
	 * {@code null}.
	 *
	 * <p>Conversion is performed via {@link Object#toString()} so callers
	 * passing custom types should ensure that {@code toString} produces a
	 * meaningful representation.</p>
	 *
	 * @param map the map to look in; may be {@code null}
	 * @param key the key whose value should be returned; may be {@code null}
	 * @return the value's {@code toString} representation, or {@code null}
	 *         when {@code map} is {@code null} or no entry exists for the key
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