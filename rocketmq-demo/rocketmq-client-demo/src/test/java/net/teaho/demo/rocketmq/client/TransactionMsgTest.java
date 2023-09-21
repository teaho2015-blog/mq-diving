package net.teaho.demo.rocketmq.client;

import net.teaho.demo.rocketmq.client.config.TestConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Transaction message.
 * <p>https://rocketmq.apache.org/zh/docs/featureBehavior/04transactionmessage</p>
 *
 * @author teaho2015@gmail.com
 * @date 2023-09
 */
public class TransactionMsgTest {

    private final TestConfig config = new TestConfig();

    private static final String PROP_CUS_KEY = "custom";

    private static final int assert_Key = 10085;

    @Test
    public void testProductTransactionMsg() throws InterruptedException, MQClientException {
        TransactionMQProducer producer = new TransactionMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        producer.setTransactionListener(new DummyTransactionListener());
        producer.start();
        try {
            String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
            for (int i = 0; i < 100; i++) {
                Message msg =
                    new Message(config.getTopic(), tags[i % tags.length], "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                msg.putUserProperty(PROP_CUS_KEY, String.valueOf(i));
                SendResult sendResult = producer.sendMessageInTransaction(msg, i);
                System.out.printf("%s%n", sendResult);
            }
            Message msg =
                new Message(config.getTopic(), "TAG_assert", "KEY_assert",
                    ("Hello RocketMQ 10086").getBytes(RemotingHelper.DEFAULT_CHARSET));
            msg.putUserProperty(PROP_CUS_KEY, String.valueOf(assert_Key));
            SendResult sendResult = producer.sendMessageInTransaction(msg, assert_Key);
            System.out.printf("%s%n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
            throw new MQClientException(e.getMessage(), null);
        }

        TimeUnit.SECONDS.sleep(30L);
        producer.shutdown();
    }

    public static class DummyTransactionListener implements TransactionListener {


        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            String value = msg.getProperty(PROP_CUS_KEY);
            return LocalTransactionState.UNKNOW;
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            String value = msg.getProperty(PROP_CUS_KEY);
            int status = Integer.parseInt(value);
            switch (status % 3) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    System.out.printf("Msg rollback %s! %n", new String(msg.getBody(), StandardCharsets.UTF_8));
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                default:
                    return LocalTransactionState.COMMIT_MESSAGE;
            }
        }
    }

    @Test
    public void testAssertConsumeTransactionMsg() throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
        consumer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        consumer.subscribe(config.getTopic(), "");


        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), msg.stream().map(messageExt -> new String(messageExt.getBody(), StandardCharsets.UTF_8)).collect(Collectors.toList()), msg);
            //assert
            msg.forEach(messageExt ->
                MatcherAssert.assertThat("fail with a rollback msg!",
                    String.valueOf(assert_Key).equals(messageExt.getUserProperty(PROP_CUS_KEY))
                        || Integer.parseInt(messageExt.getUserProperty(PROP_CUS_KEY)) % 3 == 2));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
        TimeUnit.SECONDS.sleep(30L);
        consumer.shutdown();
    }
}