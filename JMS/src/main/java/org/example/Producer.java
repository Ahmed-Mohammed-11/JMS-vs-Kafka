package org.example;

import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Scanner;

public class Producer {
    private static final String TOPIC_NAME = "accursedTest";
    private static final int NO_OF_CONSUMERS = 1;

    public static void main(String[] args) throws Exception {
        // Create a connection to ActiveMQ JMS broker using AMQP protocol
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
        Connection connection = factory.createConnection("admin", "password");
        connection.start();

        // Create a session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create a topic
        Destination destination = session.createTopic(TOPIC_NAME);

        // Create a publisher specific to topic
        MessageProducer publisher = session.createProducer(destination);

        //6) write message
//        TextMessage msg = session.createTextMessage();

//        msg.setText("loneliness has followed me my while life, everywhere." +
//                " In bars, in cars, sidewalks, stores, everywhere. There's no escape.");
//        publisher.send(msg);

        Scanner input = new Scanner(System.in);
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

//        try
//        {   //Create and start connection
//            InitialContext ctx=new InitialContext();
//            TopicConnectionFactory f=(TopicConnectionFactory)ctx.lookup("myTopicConnectionFactory");
//            TopicConnection con=f.createTopicConnection();
//            con.start();
//            //2) create queue session
//            TopicSession ses=con.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
//            //3) get the Topic object
//            Topic t=(Topic)ctx.lookup("TOPIC_NAME");
//            //4)create TopicPublisher object
//            TopicPublisher publisher=ses.createPublisher(t);
//            //5) create TextMessage object
//            TextMessage msg = ses.createTextMessage();
//
//            //6) write message
//            msg.setText("loneliness has followed me my while life, everywhere." +
//                    " In bars, in cars, sidewalks, stores, everywhere. There's no escape.");
//            publisher.publish(msg);
//            con.close();
//        }catch(Exception e){System.out.println(e);}
//    }

//
//    MyTopic myTopic = new MyTopic(TOPIC_NAME);
//        myTopic.send("why?");
//        Thread.sleep(2000);
//
//        myTopic.receive();
//        myTopic.close();
//    }
}
