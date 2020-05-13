package ru.ariona.springkafkademo;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SpringBootApplication
public class SpringKafkaDemoApplication {

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaDemoApplication.class, args);
        String topic = "test";

        MyProducer producer = new MyProducer(topic);

        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                producer.send(String.valueOf(i), "Hello from MyProducer");
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        MyConsumer myConsumer = new MyConsumer(topic);
        myConsumer.consume(stringStringConsumerRecord ->
                System.out.println(stringStringConsumerRecord.key() + stringStringConsumerRecord.value()));

        TimeUnit.MINUTES.sleep(5);
        producer.close();
        myConsumer.close();
    }

}
