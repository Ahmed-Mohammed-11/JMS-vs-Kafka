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

        Console c = System.console();
        String response;
        do {
            // Receive the message
            Message msg = subscriber.receive();
            response = ((TextMessage) msg).getText();

            System.out.println("Received = "+response);

        } while (!response.equalsIgnoreCase("Quit"));

        // Close the connection
        connection.close();
    }
}
