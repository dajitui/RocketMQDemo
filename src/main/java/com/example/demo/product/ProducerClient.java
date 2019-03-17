package com.example.demo.product;

import com.example.demo.conf.Configure;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;


public class ProducerClient {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer(Configure.BROKER_GROUP);
        producer.setNamesrvAddr(Configure.NAMESRV_ADDR);
        //重试5次
        producer.setRetryTimesWhenSendFailed(5);
        //Launch the instance.
        producer.start();

        String[] tags = new String[] {Configure.MESSAGE_TAG_A,Configure.MESSAGE_TAG_B};

        for (int i = 0; i < 100; i++) {
            int orderId = i % 10;

            System.out.println("orderId{}"+orderId);
            //创建消息
            Message msg = new Message(Configure.MESSAGE_TOPIC, tags[i % tags.length], "KEY" + i,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            //发送消息，重写选择MessageQueue 方法，把消息写到对应的ConsumerQueue 中
            // orderId 参数传递到内部方法 select arg 参数
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {

                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    System.out.println("arg{}"+arg);
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    //返回选中的队列
                    return mqs.get(index);
                }
            }, orderId,5000L);

            System.out.printf("%s%n", sendResult);
        }
        //server shutdown
        producer.shutdown();
    }
}
