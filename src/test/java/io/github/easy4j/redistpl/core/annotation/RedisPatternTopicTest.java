package io.github.easy4j.redistpl.core.annotation;

import org.junit.jupiter.api.Test;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RedisPatternTopic}.
 */
class RedisPatternTopicTest {

    @Test
    void shouldBeRetainedAtRuntime() {
        Retention retention = RedisPatternTopic.class.getAnnotation(Retention.class);
        assertNotNull(retention);
        assertEquals(RetentionPolicy.RUNTIME, retention.value());
    }

    @Test
    void shouldBeDocumented() {
        assertNotNull(RedisPatternTopic.class.getAnnotation(java.lang.annotation.Documented.class));
    }

    @Test
    void shouldBeInherited() {
        assertNotNull(RedisPatternTopic.class.getAnnotation(java.lang.annotation.Inherited.class));
    }

    @Test
    void shouldHaveValueAttribute() throws NoSuchMethodException {
        assertNotNull(RedisPatternTopic.class.getMethod("value"));
    }

    @RedisPatternTopic("news.*")
    static class TestPatternListener {
    }

    @Test
    void shouldReadAnnotationValue() {
        RedisPatternTopic annotation = TestPatternListener.class.getAnnotation(RedisPatternTopic.class);
        assertNotNull(annotation);
        assertEquals("news.*", annotation.value());
    }
}
