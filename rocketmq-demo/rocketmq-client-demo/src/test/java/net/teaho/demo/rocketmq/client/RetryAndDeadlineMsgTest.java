package net.teaho.demo.rocketmq.client;

import lombok.extern.slf4j.Slf4j;
import net.teaho.demo.rocketmq.client.config.TestConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Transaction message.
 * <p>https://rocketmq.apache.org/zh/docs/featureBehavior/04transactionmessage</p>
 *
 * @author teaho2015@gmail.com
 * @date 2023-09
 */
@Slf4j
public class RetryAndDeadlineMsgTest {

    private final TestConfig config = new TestConfig();

    private static final String PROP_CUS_KEY = "custom";

    private static final int assert_Key = 10085;

    /**
     *
     */
    @Test
    public void testProductClientRetryMsgMsg() throws InterruptedException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        producer.setRetryTimesWhenSendFailed(3);
        producer.setRetryTimesWhenSendAsyncFailed(3);
//        producer.setMqClientApiTimeout(1000);
//        producer.setPullTimeDelayMillsWhenException(1000);
        producer.start();
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 100; i++) {
            try {

                Message msg =
                    new Message(config.getTopic(), tags[i % tags.length], "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                msg.putUserProperty(PROP_CUS_KEY, String.valueOf(i));
                SendResult sendResult = producer.send(msg, 1_000L);
                log.info("sendResult:{}", sendResult);
            } catch (Exception e) {
                log.error("Error while send!msg: {}", e.getMessage());
            }
        }
        TimeUnit.SECONDS.sleep(30L);
        producer.shutdown();
    }

    @Test
    public void testClusterOrderMsgRetryConsume() throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        consumer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        consumer.subscribe(config.getTopic(), "");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //设置消费超时时间(分钟)
        consumer.setConsumeTimeout(1);
        // 重试次数
        consumer.setMaxReconsumeTimes(2);
        // 统一设置
//        consumer.setSuspendCurrentQueueTimeMillis(100L);

        consumer.registerMessageListener(new MessageListenerOrderly() {
            final AtomicLong consumeTimes = new AtomicLong(0);

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(),
                    msgs.stream().map(messageExt -> new String(messageExt.getBody(), StandardCharsets.UTF_8)).collect(Collectors.toList()),
                    msgs);
                this.consumeTimes.incrementAndGet();
                if ((this.consumeTimes.get() % 2) == 0) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else if ((this.consumeTimes.get() % 5) == 0) {
                    // 单独设置suspend time.
                    context.setSuspendCurrentQueueTimeMillis(3000L);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
        TimeUnit.SECONDS.sleep(30L);
        consumer.shutdown();


    }

    @Test
    public void testClusterNormalMsgRetryConsume() throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
        consumer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        consumer.subscribe(config.getTopic(), "");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //设置消费超时时间(分钟)
        consumer.setConsumeTimeout(1);
        // 重试次数
        consumer.setMaxReconsumeTimes(2);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            final AtomicLong consumeTimes = new AtomicLong(0);

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), msgs.stream().map(messageExt -> new String(messageExt.getBody(), StandardCharsets.UTF_8)).collect(Collectors.toList()), msgs);
                consumeTimes.incrementAndGet();
                if ((this.consumeTimes.get() % 2) == 0) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
        TimeUnit.SECONDS.sleep(30L);
        consumer.shutdown();

    }

    /**
     * 消费死信队列
     * @throws InterruptedException
     * @throws MQClientException
     */
    @Test
    public void testConsumeDLMeg() throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        consumer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        //死信队列格式： %DLQ%{consumerGroup}
        consumer.subscribe(config.getDLQ_PREFIX() + config.getConsumerGroup(), "");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //设置消费超时时间(分钟)
        consumer.setConsumeTimeout(1);
        // 重试次数
        consumer.setMaxReconsumeTimes(2);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            final AtomicLong consumeTimes = new AtomicLong(0);

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), msgs.stream().map(messageExt -> new String(messageExt.getBody(), StandardCharsets.UTF_8)).collect(Collectors.toList()), msgs);
                consumeTimes.incrementAndGet();
                // 死信消费失败不会再重试
                if ((this.consumeTimes.get() % 2) == 0) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
        TimeUnit.SECONDS.sleep(30L);
        consumer.shutdown();

    }



}