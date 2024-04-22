package org.example;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.apache.activemq.ActiveMQConnection.DEFAULT_BROKER_URL;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.ArrayList;
import java.util.List;

public class MyTopic {

    private static final String CLIENTID = "omarActiveMq";
    private String topicName;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Topic topic;
//    private Destination destination;
    private MessageProducer producer;
    private List<MessageConsumer> consumers = new ArrayList<MessageConsumer>();
//    private MessageConsumer consumer;

    public MyTopic(String topicName) throws Exception {
        super();
        // The name of the queue.
        this.topicName = topicName;
        // URL of the JMS server is required to create connection factory.
        connectionFactory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);
        // Getting JMS connection from the server and starting it
        connection = connectionFactory.createConnection();

        connection.setClientID(CLIENTID);
        connection.start();

        // Creating a non-transactional session to send/receive JMS message.
        session = connection.createSession(false, AUTO_ACKNOWLEDGE);

        // create topic
        topic = session.createTopic(topicName);

        // MessageProducer is used for sending (producing) messages to the Topic.
        producer = session.createProducer(topic);

        // MessageConsumer is used for receiving (consuming) messages from the Topic.
        MessageConsumer consumer = session.createConsumer(topic);
        consumers.add(consumer);
    }

    public void send(String textMessage) throws Exception {
        // We will send a text message
        TextMessage message = session.createTextMessage(textMessage);
        // push the message into queue
        producer.send(message);
    }

    public void receive() throws Exception {
        // receive the message from the queue.
        consumers.forEach(consumer -> {
            try {
                Message message = consumer.receive();
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.printf("Received message '%s' from the queue '%s' running on local JMS Server.\n", textMessage.getText(), topicName);
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
    }

    public MessageProducer getProducer() {
        return producer;
    }

    public List<MessageConsumer> getConsumers() {
        return consumers;
    }

    public void close() throws JMSException {
        producer.close();
        producer = null;
        consumers.forEach(consumer -> {
            try {
                consumer.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
        consumers.clear();
        session.close();
        session = null;
        connection.close();
        connection = null;
    }
}