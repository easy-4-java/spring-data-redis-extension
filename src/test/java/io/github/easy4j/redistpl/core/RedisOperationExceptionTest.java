package io.github.easy4j.redistpl.core;

import org.junit.jupiter.api.Test;
import org.springframework.dao.NonTransientDataAccessException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RedisOperationException}.
 */
class RedisOperationExceptionTest {

    @Test
    void shouldCreateExceptionWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("root cause");
        RedisOperationException ex = new RedisOperationException("test message", cause);

        assertTrue(ex.getMessage().contains("test message"));
        assertEquals(cause, ex.getCause());
    }

    @Test
    void shouldCreateExceptionWithMessageOnly() {
        RedisOperationException ex = new RedisOperationException("test message");

        assertEquals("test message", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    void shouldBeNonTransientDataAccessException() {
        RedisOperationException ex = new RedisOperationException("test");
        assertInstanceOf(NonTransientDataAccessException.class, ex);
    }

    @Test
    void shouldAcceptNullMessage() {
        RedisOperationException ex = new RedisOperationException(null);
        assertNull(ex.getMessage());
    }

    @Test
    void shouldAcceptNullCause() {
        RedisOperationException ex = new RedisOperationException("msg", null);
        assertEquals("msg", ex.getMessage());
        assertNull(ex.getCause());
    }
}
