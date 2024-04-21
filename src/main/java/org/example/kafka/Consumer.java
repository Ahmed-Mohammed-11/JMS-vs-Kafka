package org.example.kafka;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.example.constants.Kafka.*;

public class Consumer {
    public static void main(String[] args) {

        //create a producer and configure it
        Properties props = getProperties();
        try (final KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props)) {
            // subscribe to the topic.
            consumer.subscribe(List.of(TOPIC));
            // poll messages from the topic and print them to the console
            long startTime = System.currentTimeMillis();
            consumer.poll(Duration.ofSeconds(5));
            long responseTime = System.currentTimeMillis() - startTime;
            System.out.println("Message received successfully in " + responseTime + " ms");
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + (int) (Math.random() * 100));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
