package com.example.demo.comsumer;

import com.example.demo.conf.Configure;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class ConsumerClient {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(Configure.CONSUMER_GROUP);
        consumer.setNamesrvAddr(Configure.NAMESRV_ADDR);
        // 批量消费,每次拉取10条
        consumer.setConsumeMessageBatchMaxSize(10);
        // 如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置消费超时时间
        consumer.setConsumeTimeout(Configure.TIMEOUT_SECONDS);
        consumer.subscribe(Configure.MESSAGE_TOPIC, Configure.MESSAGE_TAG_A + " || " + Configure.MESSAGE_TAG_B);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {

                    /*msgs.forEach(m -> {
                        System.out.print("host:" + m.getBornHost() + "--");
                        System.out.print("key:" + m.getKeys() + "--");
                        System.out.print("Topic:" + m.getTopic() + "--");
                        System.out.print("QueueId:" + m.getQueueId() + "--");
                        System.out.print("tags:" + m.getTags() + "--");
                        System.out.print("msg:" + new String(m.getBody()));
                        System.out.print("重试次数："+m.getReconsumeTimes());
                        System.out.println();
                    });*/
                //逐条消费
                Message message = msgs.get(0);
                /*for (int a = 0; a < msgs.size(); a++) {
                    message = msgs.get(a);
                    System.out.println(message.getBody() + " " + ((MessageExt) message).getMsgId() + " " + message.getTopic() + " " + message.getTags());
                    if (((MessageExt) message).getReconsumeTimes() >= Configure.RetryTime) {
                        //列为死信
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }*/
                try {
                    //int i = 1 / 0;
                    //模拟业务异常
                    Thread.sleep(1000L*60*2);
                    System.out.println("消息内容："+new String(message.getBody()) + " " + ((MessageExt) message).getMsgId() + " " + message.getTopic() + " " + message.getTags());
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    System.out.println("尝试次数:" + ((MessageExt) message).getReconsumeTimes());
                    if (((MessageExt) message).getReconsumeTimes() >= Configure.RetryTime) {
                        //列为死信,记录消息内容，然后人工处理
                        System.out.println("出现异常的消息："+new String(message.getBody()) + " " + ((MessageExt) message).getMsgId() + " " + message.getTopic() + " " + message.getTags());
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }


                //RECONSUME_LATER 消费失败，需要稍后重新消费
                //return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.%n");
    }
}
