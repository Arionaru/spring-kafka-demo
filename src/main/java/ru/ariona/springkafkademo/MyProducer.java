package ru.ariona.springkafkademo;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

public class MyProducer implements Closeable {

    private final String topic;
    private final KafkaProducer<String, String> producer;

    public MyProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @SneakyThrows
    public void send(String key, String value) {
        producer
                .send(new ProducerRecord<>(topic, key,value))
                .get();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
