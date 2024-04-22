package org.example;
import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.*;
import java.io.Console;
import java.util.Scanner;

public class Subscriber {
    private static final String TOPIC_NAME = "accursedTest";

    public static void main(String[] args) throws Exception {
        // Create a connection to ActiveMQ JMS broker using AMQP protocol
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
        Connection connection = factory.createConnection("admin", "password");
        connection.start();
        // Create a session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Scanner input = new Scanner(System.in);
        System.out.println("please enter topic name you want to listen to: ");
        String topicName = input.nextLine();

        // Create a topic
        Destination destination = session.createTopic(topicName);

        // Create a subscriber specific to topic
        MessageConsumer subscriber = session.createConsumer(destination);
//        subscriber.setMessageListener(new MyTopicListener());
        Console c = System.console();
        String response;
        do {
            // Receive the message
            long start = System.currentTimeMillis();
            Message msg = subscriber.receive();
            long end = System.currentTimeMillis();
            response = ((TextMessage) msg).getText();

            System.out.println("Received = "+response);
            System.out.println("Time taken = "+(end-start) + "ms");
        } while (!response.equalsIgnoreCase("Quit"));

        // Close the connection
        connection.close();
    }

    public static class MyTopicListener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try{
                if(message instanceof TextMessage) {
                    String response = ((TextMessage) message).getText();
                    System.out.println("Received = " + response);
                }else{
                    System.out.println("Message of wrong type: " + message.getClass().getName());
                }
            } catch (JMSException e){

            }
        }
    }
}
