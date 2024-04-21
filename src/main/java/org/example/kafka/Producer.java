package org.example.kafka;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.example.constants.Files.MESSAGE_FILE_PATH;
import static org.example.constants.Kafka.*;

public class Producer {
    public static void main(String[] args) {
        //create a producer and configure it
        Properties props = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //get the value of message from file in resources
        String message = "";
        try {
            message = Files.readString(Path.of(MESSAGE_FILE_PATH));
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }

        //send the message to the topic
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
        long startTime = System.currentTimeMillis();
        producer.send(record);
        long responseTime = System.currentTimeMillis() - startTime;

        System.out.println("Message sent successfully in " + responseTime + " ms");
        producer.close();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);
        return props;
    }
}


