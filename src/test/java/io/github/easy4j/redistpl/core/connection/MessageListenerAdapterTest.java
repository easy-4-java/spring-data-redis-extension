package io.github.easy4j.redistpl.core.connection;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link MessageListenerAdapter}.
 */
class MessageListenerAdapterTest {

    @Test
    void shouldBeAssignableFromMessageListener() {
        assertTrue(MessageListener.class.isAssignableFrom(MessageListenerAdapter.class));
    }

    @Test
    void shouldHaveSetMessageListenerContainerMethod() throws NoSuchMethodException {
        assertNotNull(MessageListenerAdapter.class.getMethod("setMessageListenerContainer", RedisMessageListenerContainer.class));
    }

    @Test
    void shouldAllowMockImplementation() {
        MessageListenerAdapter adapter = mock(MessageListenerAdapter.class);
        RedisMessageListenerContainer container = mock(RedisMessageListenerContainer.class);

        adapter.setMessageListenerContainer(container);
        verify(adapter).setMessageListenerContainer(container);
    }

    @Test
    void shouldAllowOnMessageCall() {
        MessageListenerAdapter adapter = mock(MessageListenerAdapter.class);
        Message message = mock(Message.class);

        adapter.onMessage(message, null);
        verify(adapter).onMessage(message, null);
    }
}
