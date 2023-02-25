package com.app.kafka;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
public class SpringKafkaExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    @Bean
    public NewTopic hobbit2() {
        return TopicBuilder
                .name("hobbit2")
                .partitions(15)
                .replicas(3)
                .build();
    }
}

@RequiredArgsConstructor
@Component
class Producer {

    private final KafkaTemplate<Integer, String> template;
    Faker faker;

    @EventListener(ApplicationStartedEvent.class)
    public void generate() {
        faker = Faker.instance();
        final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
        final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));
        Flux.zip(interval, quotes)
                .map(it -> template.send(
                        "hobbit",
                        faker.random().nextInt(42),
                        it.getT2()
                )).blockLast();
    }
}

@Component
class Consumer {

    @KafkaListener(topics = {"hobbit"}, groupId = "spring-boot-kafka")
    public void consume(ConsumerRecord<Integer, String> record) {
        System.out.println("received = " + record.value() + " with key " + record.key());
    }
}
