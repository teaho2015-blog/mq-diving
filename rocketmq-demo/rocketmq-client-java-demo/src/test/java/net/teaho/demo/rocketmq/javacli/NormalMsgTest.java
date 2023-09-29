package net.teaho.demo.rocketmq.javacli;

import lombok.extern.slf4j.Slf4j;
import net.teaho.demo.rocketmq.javacli.config.TestConfig;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.*;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * <p>simple send and receive normal msg
 * <p>https://rocketmq.apache.org/zh/docs/featureBehavior/01normalmessage
 *
 * @link https://github.com/apache/rocketmq-clients rocketmq-client-java 5.0.5
 * @see org.apache.rocketmq.client.java.example.ProducerNormalMessageExample
 * @see org.apache.rocketmq.client.java.example.SimpleConsumerExample
 *
 * @author teaho2015@gmail.com
 * @date 2023-09
 */
@Slf4j
public class NormalMsgTest {

    private final TestConfig config = new TestConfig();

    /**
     * product msg
     *
     */
    @Test
    public void testProductSimpleMsg() throws ClientException, IOException, InterruptedException {
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(config.getDefaultProxyEndPoint()).build();
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final ProducerBuilder builder = provider.newProducerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the topic name(s), which is optional but recommended. It makes producer could prefetch
            // the topic route before message publishing.
            .setTopics(config.getTopics());
//        if (checker != null) {
//            // Set the transaction checker.
//            builder.setTransactionChecker(checker);
//        }

        Producer producer = builder.build();
        byte[] body = "This is a normal message for Apache RocketMQ".getBytes(StandardCharsets.UTF_8);
        String tag = config.getTagA();
        final Message message = provider.newMessageBuilder()
            // Set topic for the current message.
            .setTopic(config.getTopic())
            .setTag(tag)
            // Key(s) of the message, another way to mark message besides message id.
            .setKeys("yourMessageKey-1c151062f96e")
            .setBody(body)
            .build();
        try {
            final SendReceipt sendReceipt = producer.send(message);
            log.info("Send message successfully, message={}", sendReceipt);
        } catch (Throwable t) {
            log.error("Failed to send message", t);
        }
        TimeUnit.SECONDS.sleep(5L);
        producer.close();
    }

    /**
     * pull consumer
     */
    @Test
    public void testPullConsumeSimpleMsg() throws ClientException {

        final ClientServiceProvider provider = ClientServiceProvider.loadService();


        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(config.getDefaultProxyEndPoint())
            .build();
        String consumerGroup = config.getConsumerGroup();
        Duration awaitDuration = Duration.ofSeconds(30);
        String tag = config.getTagA();
        String topic = config.getTopic();
        // fliter tag
//        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            // set await duration for long-polling.
            .setAwaitDuration(awaitDuration)
            // Set the subscription for the consumer.
            //sub by filterExpression
//            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // sub all
            .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
            .build();
        // Max message num for each long polling.
        int maxMessageNum = 16;
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(15);
        // Receive message, multi-threading is more recommended.
        do {
            final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
            log.info("Received {} message(s)", messages.size());
            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                try {
                    log.info("Message is acknowledged successfully, body: {}, messageId={}", StandardCharsets.UTF_8.decode(message.getBody()), messageId);
                    consumer.ack(message);
                } catch (Throwable t) {
                    log.error("Message is failed to be acknowledged, messageId={}", messageId, t);
                }
            }
        } while (true);
        // Close the simple consumer when you don't need it anymore.
        // consumer.close();

    }

    /**
     * push consumer
     */
    @Test
    public void testPushConsumeNormalMsg() throws ClientException, InterruptedException, IOException {

        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(config.getDefaultProxyEndPoint())
            .build();

        String tag = config.getTagA();
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        String consumerGroup = config.getConsumerGroup();
        String topic = config.getTopic();
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            // Set the subscription for the consumer.
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .setMessageListener(messageView -> {
                // Handle the received message and return consume result.
                log.info("Consume body={}, message={}", StandardCharsets.UTF_8.decode(messageView.getBody()), messageView);
                return ConsumeResult.SUCCESS;
            })
            .build();
        // Block the main thread, no need for production environment.
        TimeUnit.SECONDS.sleep(30L);
        // Close the push consumer when you don't need it anymore.
        pushConsumer.close();
    }




}
