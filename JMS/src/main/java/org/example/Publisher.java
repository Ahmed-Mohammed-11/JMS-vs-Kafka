package org.example;

import org.apache.activemq.memory.list.MessageList;
import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.*;
import java.io.BufferedReader;
import java.io.FileReader;
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
        String filePath = "src/main/resources/message.txt";
        // Read the content of the file
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line);
                content.append(System.lineSeparator());
            }
        }

        TextMessage msg = session.createTextMessage(content.toString());
        sendMessagesIndefinitely(publisher, msg);
//        sendNMessages(publisher, msg, 10000);

        TextMessage quitMsg = session.createTextMessage("Quit");
        publisher.send(quitMsg);



//        String response;
//        do {
//            System.out.println("Enter message: ");
//            response = input.nextLine();
//            // Create a message object
//            TextMessage msg = session.createTextMessage(response);
//
//            // Send the message to the topic
//            long start = System.currentTimeMillis();
//            publisher.send(msg);
//            long end = System.currentTimeMillis();
//            System.out.println("Time taken = "+(end-start) + "ms");
//
//        } while (!response.equalsIgnoreCase("Quit"));
//        input.close();

        // Close the connection
//        connection.close();
    }

    public static void sendMessagesIndefinitely(MessageProducer publisher, TextMessage msg) throws JMSException {
        // Send the message to the topic
        long count = 0, avg = 0, sum = 0;
        while(true){
            long start = System.currentTimeMillis();
            publisher.send(msg);
            long end = System.currentTimeMillis();
            System.out.println("Time taken = " + (end - start) + "ms");
            sum += (end - start);
            count++;
            avg = sum / count;
            System.out.println("------------------------------------Average time taken = " + avg + "ms");
        }
    }
    public static void sendNMessages(MessageProducer publisher, TextMessage msg, int n) throws JMSException {
        for(int i = 0;i < n;i++){
            // add the current time to the message
            msg.setJMSTimestamp(System.currentTimeMillis());
            publisher.send(msg);
        }
    }
    public static void sendNMessagesPerSecond(MessageProducer publisher, TextMessage msg, int messagesPerSecond) throws JMSException, InterruptedException {
        // send X number of messages per second
        double sleepTime = 1000.0 / messagesPerSecond;
        for (int i = 0;i < messagesPerSecond;i++){
            long start = System.currentTimeMillis();
            publisher.send(msg);
            long end = System.currentTimeMillis();
            System.out.println("Time taken = " + (end - start) + "ms");
            Thread.sleep((long) (sleepTime - 0.2 * sleepTime));
        }
    }
}
