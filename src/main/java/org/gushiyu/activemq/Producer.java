package org.gushiyu.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @Description
 * @Author Gushiyu
 * @Date 2020-10-10 10:35
 */
public class Producer {

    private String brokerURL = "tcp://106.13.105.114:61616";
    ConnectionFactory connectionFactory;
    Connection connection;

    public Session getSession() throws JMSException {

        // 4、使用Connection对象创建一个Session对象。
        // 参数一是否开启事务，一般不开启事务，保证数据得最终一致性，可以使用消息队列实现数据最终一致性。如果第一个参数为true，第二个参数自动忽略
        // 参数二是消息得应答模式。两种模式，自动应答和手动应答。一般使用自动应答。
        boolean transacted = false;// 不开启事务
        int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;// 1
        return connection.createSession(transacted, acknowledgeMode);
    }

    public void exeQueueTextMessage(String queueName,String s) throws JMSException {
        Session session = getSession();
        MessageProducer producer = session.createProducer(session.createQueue(queueName));
        TextMessage message = session.createTextMessage(s);
        producer.send(message);
        producer.close();
        session.close();
    }


    public void exeTopicTextMessage(String topicName ,String s) throws JMSException {
        Session session = getSession();
        MessageProducer producer = session.createProducer(session.createTopic(topicName));
        TextMessage message = session.createTextMessage(s);
        producer.send(message);
        producer.close();
        session.close();
    }





    public Producer(String brokerURL) throws JMSException {
        this.brokerURL = brokerURL;
        connectionFactory = new ActiveMQConnectionFactory(this.brokerURL);
        // 2、使用ConnectionFactory创建一个连接Connection对象。
        connection = connectionFactory.createConnection();

    }

    public Producer() throws JMSException {
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


    public static void main(String[] args) throws JMSException, InterruptedException {
        Producer producer = new Producer();
        producer.start();
        producer.exeQueueTextMessage("queue1","this is queue message!");
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(100);
            producer.exeTopicTextMessage("topic01","this is "+i+"message");
        }

        producer.close();
    }
}
