package net.teaho.demo.rocketmq.javacli.config;

import lombok.Data;

/**
 * @author teaho2015@gmail.com
 * @date 2023-06
 */
@Data
public class TestConfig {


    private int messageCount = 100;
    private String producerGroup = "test_producer_group";
    private String defaultNamesrvAddr = "127.0.0.1:9876";
    private String defaultProxyEndPoint = "127.0.0.1:8091";
    private String topic = "TopicTest";
    private String[] topics = new String[]{"TopicTest"};
    private String tagA = "TagA";
    private String tagB = "TagB";
    private String consumerGroup = "test_consumer_group";
    private String DLQ_PREFIX = "%DLQ%";


}
