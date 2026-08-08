package io.github.easy4j.redistpl.core.annotation;

import org.junit.jupiter.api.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RedisChannelTopic}.
 */
class RedisChannelTopicTest {

    @Test
    void shouldBeRetainedAtRuntime() {
        Retention retention = RedisChannelTopic.class.getAnnotation(Retention.class);
        assertNotNull(retention);
        assertEquals(RetentionPolicy.RUNTIME, retention.value());
    }

    @Test
    void shouldTargetTypes() {
        Target target = RedisChannelTopic.class.getAnnotation(Target.class);
        assertNotNull(target);
        assertTrue(target.value().length > 0);
    }

    @Test
    void shouldBeDocumented() {
        assertNotNull(RedisChannelTopic.class.getAnnotation(java.lang.annotation.Documented.class));
    }

    @Test
    void shouldBeInherited() {
        assertNotNull(RedisChannelTopic.class.getAnnotation(java.lang.annotation.Inherited.class));
    }

    @Test
    void shouldHaveValueAttribute() throws NoSuchMethodException {
        assertNotNull(RedisChannelTopic.class.getMethod("value"));
    }

    @RedisChannelTopic("test-channel")
    static class TestListener {
    }

    @Test
    void shouldReadAnnotationValue() {
        RedisChannelTopic annotation = TestListener.class.getAnnotation(RedisChannelTopic.class);
        assertNotNull(annotation);
        assertEquals("test-channel", annotation.value());
    }
}
