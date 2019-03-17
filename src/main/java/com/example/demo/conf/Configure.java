package com.example.demo.conf;

/**
 * mq 配置信息
 */
public class Configure {

    /**
     * name server 地址，多个用分号隔开
     */
    public static String NAMESRV_ADDR = "127.0.0.1:9876";

    /**
     * broker 组名
     */
    public static String BROKER_GROUP = "test_broker_group";

    /**
     * consumer 组名
     */
    public static String CONSUMER_GROUP = "test_consumer_group";


    /**
     * 消息主题
     */
    public static String MESSAGE_TOPIC = "test_message_topic";

    /**
     * 消息key 前缀，消息key一般是业务的主键
     */
    public static String MESSAGE_KEY_PREFIX = "message_key_prefix:";

    /**
     * 消息tag
     */
    public static String MESSAGE_TAG_A = "test_tag_a";
    public static String MESSAGE_TAG_B = "test_tag_b";


    /**
     * 默认编码格式
     */
    public static String DEFAULT_CHARSET = "UTF-8";

    /**
     * 重发次数
     */
    public static int RetryTime = 2;

    /**
     * 超时时间
     */
    public static long TIMEOUT_SECONDS = 1L;

}
