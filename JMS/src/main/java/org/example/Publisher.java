package org.example;

import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.*;
import java.util.Scanner;

public class Publisher {
    private static final String TOPIC_NAME = "accursedTest";
    private static final int NO_OF_CONSUMERS = 1;

    public static void main(String[] args) throws Exception {
        // Create a connection to ActiveMQ JMS broker using AMQP protocol
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
        Connection connection = factory.createConnection("admin", "password");
        connection.start();

        // Create a session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Scanner input = new Scanner(System.in);
        System.out.println("please enter topic name you want to publish to: ");
        String topicName = input.nextLine();
        // Create a topic
        Destination destination = session.createTopic(topicName);

        // Create a publisher specific to topic
        MessageProducer publisher = session.createProducer(destination);

        //6) write message
//        TextMessage msg = session.createTextMessage();

//        msg.setText("loneliness has followed me my while life, everywhere." +
//                " In bars, in cars, sidewalks, stores, everywhere. There's no escape.");
//        publisher.send(msg);

        String response;
        do {
            System.out.println("Enter message: ");
            response = input.nextLine();
            // Create a message object
            TextMessage msg = session.createTextMessage(response);

            // Send the message to the topic
            publisher.send(msg);

        } while (!response.equalsIgnoreCase("Quit"));
        input.close();

        // Close the connection
        connection.close();
    }

}
