package com.example.demo.comsumer;


import com.example.demo.conf.Configure;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class ConsumerClient1 {
    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(Configure.CONSUMER_GROUP);
        consumer.setNamesrvAddr(Configure.NAMESRV_ADDR);

        consumer.subscribe(Configure.MESSAGE_TOPIC, Configure.MESSAGE_TAG_A+" || "+Configure.MESSAGE_TAG_B);

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                                                       ConsumeOrderlyContext context) {
                //手动确认
                context.setAutoCommit(false);
                msgs.forEach(m->{
                    System.out.print("host:"+m.getBornHost()+"--");
                    System.out.print("key:"+m.getKeys()+"--");
                    System.out.print("Topic:"+m.getTopic()+"--");
                    System.out.print("QueueId:"+m.getQueueId()+"--");
                    System.out.print("tags:"+m.getTags()+"--");
                    System.out.print("msg:"+new String(m.getBody()));
                    System.out.println();
                });
                return ConsumeOrderlyStatus.SUCCESS;

            }
        });

        consumer.start();

        System.out.println("Consumer Started.%n");
    }
}

