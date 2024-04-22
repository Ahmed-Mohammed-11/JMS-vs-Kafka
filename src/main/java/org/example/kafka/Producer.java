package org.example.kafka;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.example.constants.Files.MESSAGE_FILE_PATH;
import static org.example.constants.Kafka.*;

public class Producer implements Runnable {
    private static final Logger logger = Logger.getLogger(Consumer.class.getName());
    // response time code
//    public static void main(String[] args) {
//        //create a producer and configure it
//        Properties props = getProperties();
//        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//
//        //get the value of message from file in resources
//        String message = "";
//        try {
//            message = Files.readString(Path.of(MESSAGE_FILE_PATH));
//        } catch (IOException e) {
//            System.err.println("Error reading the file: " + e.getMessage());
//        }
//
//        //send the message to the topic
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
//        long startTime = System.currentTimeMillis();
//        producer.send(record);
//        long responseTime = System.currentTimeMillis() - startTime;
//
//        System.out.println("Message sent successfully in " + responseTime + " ms");
//        producer.close();
//    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);
        return props;
    }

    @Override
    public void run() {
        //create a producer and configure it
        Properties props = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int numOfMessages = 10000;

        //get the value of message from file in resources
        String message = "";
        try {
            message = Files.readString(Path.of(MESSAGE_FILE_PATH));
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }
        try {
            for (int i = 1 ; i <= numOfMessages ; i ++) {
                long timestamp = System.currentTimeMillis();
                producer.send(new ProducerRecord<>(TOPIC, Long.toString(timestamp), message));
                logger.info("Sent message: " + i + " - Timestamp: " + timestamp);
                Thread.sleep(100);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to send message", e);
        } finally {
            producer.close();
        }
    }

    // trial as processes not threads
    public static void main(String[] args) {
        //create a producer and configure it
        Properties props = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int numOfMessages = 10000;

        //get the value of message from file in resources
        String message = "";
        try {
            message = Files.readString(Path.of(MESSAGE_FILE_PATH));
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }
        try {
            for (int i = 1 ; i <= numOfMessages ; i ++) {
                long timestamp = System.currentTimeMillis();
                producer.send(new ProducerRecord<>(TOPIC, Long.toString(timestamp), message));
                logger.info("Sent message: " + i + " - Timestamp: " + timestamp);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to send message", e);
        } finally {
            producer.close();
        }
    }
}


