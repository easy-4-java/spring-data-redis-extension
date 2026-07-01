package io.github.hiwepy.redistpl.core.connection;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Redis 消息监听器适配器接口。
 * <p>
 * 扩展自 Spring 的 {@link MessageListener}，增加了对 {@link RedisMessageListenerContainer} 的感知能力。
 * 实现此接口的监听器可以获取到所属的容器实例，便于在消息处理过程中进行容器级别的操作。
 * <p>
 * 配合 {@link io.github.hiwepy.redistpl.core.annotation.RedisChannelTopic} 和
 * {@link io.github.hiwepy.redistpl.core.annotation.RedisPatternTopic} 注解使用，
 * 实现声明式的 Redis 消息订阅。
 *
 * @author wandl
 */
public interface MessageListenerAdapter extends MessageListener {

    /**
     * 设置消息监听器所属的 Redis 消息监听容器
     *
     * @param container Redis 消息监听容器实例
     */
    void setMessageListenerContainer(RedisMessageListenerContainer container);

}
