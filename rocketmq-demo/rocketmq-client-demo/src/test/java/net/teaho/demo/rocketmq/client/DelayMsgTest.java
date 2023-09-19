package net.teaho.demo.rocketmq.client;

import net.teaho.demo.rocketmq.client.config.TestConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 * Delay message.
 * <p>https://rocketmq.apache.org/docs/featureBehavior/02delaymessage</p>
 *
 * @author teaho2015@gmail.com
 * @date 2023-09
 */
public class DelayMsgTest {

    private final TestConfig config = new TestConfig();


    @Test
    public void testProductSimpleMsg() throws InterruptedException, MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getDefaultNamesrvAddr());
        producer.start();

        try {

            Message msg = new Message(config.getTopic() /* Topic */,
                config.getTag() /* Tag */,
                ("Hello RocketMQ delay msg").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //delay 5s
            msg.setDeliverTimeMs(System.currentTimeMillis() + 5_000L);

            SendResult sendResult = producer.send(msg);

            System.out.printf("%s%n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }
        /*
         * Shut down once the producer instance is no longer in use.
         */
        producer.shutdown();
    }



    /**
     * simple send
     */
    @Test
    public void testConsumeSimpleMsg() throws InterruptedException, MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getConsumerGroup());

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        consumer.setNamesrvAddr(config.getDefaultNamesrvAddr());

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more topic to consume.
         */
        consumer.subscribe(config.getTopic(), "*");

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();
        System.out.printf("Consumer Started.%n");

        TimeUnit.SECONDS.sleep(30L);
        consumer.shutdown();
    }
}
