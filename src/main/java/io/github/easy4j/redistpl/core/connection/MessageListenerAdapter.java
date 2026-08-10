package io.github.easy4j.redistpl.core.connection;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Extension of Spring's {@link MessageListener} contract that lets a listener
 * discover the {@link RedisMessageListenerContainer} it has been registered
 * with.
 *
 * <p>Implementations gain the ability to interact with their owning container
 * &mdash; for example, to dynamically add or remove additional subscriptions
 * during message processing. The framework injects the container reference via
 * {@link #setMessageListenerContainer(RedisMessageListenerContainer)} during
 * listener registration.</p>
 *
 * <p>Together with {@link io.github.easy4j.redistpl.core.annotation.RedisChannelTopic}
 * and {@link io.github.easy4j.redistpl.core.annotation.RedisPatternTopic} this
 * adapter enables declarative Redis pub/sub configuration.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 3.0.0
 * @see MessageListener
 * @see RedisMessageListenerContainer
 */
public interface MessageListenerAdapter extends MessageListener {

    /**
     * Stores the container that owns this listener.
     *
     * @param container the {@link RedisMessageListenerContainer} that has
     *                  registered this listener; must not be {@code null}
     */
    void setMessageListenerContainer(RedisMessageListenerContainer container);

}