package org.gushiyu.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;
import java.util.*;

/**
 * @Description
 * @Author Gushiyu
 * @Date 2020-10-10 11:01
 */
public class Consumer {

    private String brokerURL = "tcp://106.13.105.114:61616";
    ConnectionFactory connectionFactory;
    Connection connection;
    Map<String, MessageConsumer> queueMap = new HashMap<String,MessageConsumer>();
    Map<String, MessageConsumer> topicMap = new HashMap<String,MessageConsumer>();

    public Session getSession() throws JMSException {

        // 4、使用Connection对象创建一个Session对象。
        // 参数一是否开启事务，一般不开启事务，保证数据得最终一致性，可以使用消息队列实现数据最终一致性。如果第一个参数为true，第二个参数自动忽略
        // 参数二是消息得应答模式。两种模式，自动应答和手动应答。一般使用自动应答。
        boolean transacted = false;// 不开启事务
        int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;// 1
        return connection.createSession(transacted, acknowledgeMode);
    }

    public void setQueueMessageListener(String queueName,MessageListener listener) throws JMSException {
        Session session = getSession();
        MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
        queueMap.put(queueName,consumer);
        consumer.setMessageListener(listener);
    }

    public void setTopicMessageListener(String topicName,MessageListener listener) throws JMSException {
        Session session = getSession();
        MessageConsumer consumer = session.createConsumer(session.createTopic(topicName));
        topicMap.put(topicName,consumer);
        consumer.setMessageListener(listener);
    }


    public void stopAllConsumer(){
        queueMap.keySet().iterator().forEachRemaining((e)->{
            try {
                queueMap.get(e).close();
            } catch (JMSException jmsException) {
                jmsException.printStackTrace();
            }
        });

        topicMap.keySet().iterator().forEachRemaining((e)->{
            try {
                topicMap.get(e).close();
            } catch (JMSException jmsException) {
                jmsException.printStackTrace();
            }
        });

    }


    public Consumer(String brokerURL) throws JMSException {
        this.brokerURL = brokerURL;
        connectionFactory = new ActiveMQConnectionFactory(this.brokerURL);
        // 2、使用ConnectionFactory创建一个连接Connection对象。
        connection = connectionFactory.createConnection();

    }

    public Consumer() throws JMSException {
        connectionFactory = new ActiveMQConnectionFactory(this.brokerURL);
        // 2、使用ConnectionFactory创建一个连接Connection对象。
        connection = connectionFactory.createConnection();
    }

    public void start() throws JMSException {
        // 3、开启连接。调用Connection对象得start方法。
        connection.start();
    }

    public void close() throws JMSException {
        connection.close();
    }

    public static void main(String[] args) throws JMSException {
        Consumer c = new Consumer();
        c.start();
        c.setQueueMessageListener("queue1", new MessageListener() {

            @Override
            public void onMessage(Message message) {
                if(message instanceof TextMessage){
                    String text = null;
                    try {
                        text = ((TextMessage) message).getText();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                    System.out.println(text);
                }
            }
        });
        c.setTopicMessageListener("topic01", new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if(message instanceof TextMessage){
                    String text = null;
                    try {
                        text = ((TextMessage) message).getText();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                    System.out.println(text);
                }
            }
        });

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        c.stopAllConsumer();
        c.close();
    }

}
