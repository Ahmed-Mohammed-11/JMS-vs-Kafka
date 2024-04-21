package org.example.constants;

public record Kafka() {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC = "lab4";
    public static final String GROUP_ID = "lab4-group";
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
}
