package net.teaho.demo.rocketmq.client.config;

import lombok.Data;

/**
 * @author teaho2015@gmail.com
 * @date 2023-06
 */
@Data
public class TestConfig {


    private int messageCount = 100;
    private String producerGroup = "please_rename_unique_group_name";
    private String defaultNamesrvAddr = "127.0.0.1:9876";
    private String topic = "TopicTest";
    private String tag = "TagA";
    private String consumerGroup = "please_rename_unique_group_name_4";

}
