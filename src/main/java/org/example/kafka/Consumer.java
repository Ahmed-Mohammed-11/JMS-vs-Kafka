package org.example.kafka;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.example.constants.Kafka.*;

public class Consumer implements Runnable{
    private static final Logger logger = Logger.getLogger(Consumer.class.getName());
    // response time code
//    public static void main(String[] args) {
        //create a producer and configure it
//        Properties props = getProperties();
//        try (final KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props)) {
//            // subscribe to the topic.
//            consumer.subscribe(List.of(TOPIC));
//            // poll messages from the topic and print them to the console
//            long startTime = System.currentTimeMillis();
//            consumer.poll(Duration.ofSeconds(5));
//            long responseTime = System.currentTimeMillis() - startTime;
//            System.out.println("Message received successfully in " + responseTime + " ms");
//        }
//    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + (int) (Math.random() * 100));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Override
    public void run() {
        long medianLatency = 0L;
        long[] latencies = new long[10000];
        Properties props = getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));
        int i = 0 ;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(i >= 10000) break;
                for (ConsumerRecord<String, String> record : records) {
                    long timestamp = Long.parseLong(record.key());
                    long currentTimestamp = System.currentTimeMillis();
                    long latency = currentTimestamp - timestamp;
                    latencies[i++] = latency;
                    logger.info("Received message - with timestamp: " + currentTimestamp + " - Latency: " + latency + " ms");
                }
            }
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE, "Failed to parse timestamp", e);
        } finally {
            Arrays.sort(latencies);
            System.out.println("length: " + latencies.length);
            medianLatency = latencies[latencies.length / 2];
            long ninetiethPercentile = latencies[(int) (latencies.length * 0.90)];
            long ninetyNinthPercentile = latencies[(int) (latencies.length * 0.99)];
            System.out.println("Median latency: " + medianLatency + " ms");
            System.out.println("90th percentile latency: " + ninetiethPercentile + " ms");
            System.out.println("99th percentile latency: " + ninetyNinthPercentile + " ms");
            consumer.close();
        }
    }

    // trial as processes not threads
    public static void main(String[] args) {
        long medianLatency = 0L;
        long[] latencies = new long[10000];
        Properties props = getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));
        int i = 0 ;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(i >= 10000) break;
                for (ConsumerRecord<String, String> record : records) {
                    long timestamp = Long.parseLong(record.key());
                    long currentTimestamp = System.currentTimeMillis();
                    long latency = currentTimestamp - timestamp;
                    latencies[i++] = latency;
                    logger.info("Received message - with timestamp: " + currentTimestamp + " - Latency: " + latency + " ms");
                }
            }
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE, "Failed to parse timestamp", e);
        } finally {
            Arrays.sort(latencies);
            System.out.println("length: " + latencies.length);
            medianLatency = latencies[latencies.length / 2];
            long ninetiethPercentile = latencies[(int) (latencies.length * 0.90)];
            long ninetyNinthPercentile = latencies[(int) (latencies.length * 0.99)];
            System.out.println("Median latency: " + medianLatency + " ms");
            System.out.println("90th percentile latency: " + ninetiethPercentile + " ms");
            System.out.println("99th percentile latency: " + ninetyNinthPercentile + " ms");
            consumer.close();
        }
    }
}
