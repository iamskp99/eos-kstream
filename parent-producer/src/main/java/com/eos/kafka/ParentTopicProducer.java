package com.eos.kafka;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

public class ParentTopicProducer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final String DEFAULT_TOPIC = "parent-topic";
    private static final int DEFAULT_MESSAGE_COUNT = 100;
    private static final long DEFAULT_PRODUCE_INTERVAL_MS = 1_000;

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = env("TOPIC", DEFAULT_TOPIC);
        int messageCount = intEnv("MESSAGE_COUNT", DEFAULT_MESSAGE_COUNT);
        long produceIntervalMs = longEnv("PRODUCE_INTERVAL_MS", DEFAULT_PRODUCE_INTERVAL_MS);

        Properties commonProps = new Properties();
        commonProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        waitForKafka(commonProps);
        createTopicIfMissing(commonProps, topic);

        Properties producerProps = new Properties();
        producerProps.putAll(commonProps);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "parent-topic-producer-" + UUID.randomUUID());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            if (messageCount == 0) {
                System.out.printf("Producing messages to %s forever...%n", topic);
                int id = 1;
                while (true) {
                    produce(producer, topic, id, "Produced message " + id);
                    id++;
                    Thread.sleep(produceIntervalMs);
                }
            }

            System.out.printf("Producing %d messages to %s...%n", messageCount, topic);
            for (int id = 1; id <= messageCount; id++) {
                produce(producer, topic, id, "Produced message " + id + "/" + messageCount);
                Thread.sleep(produceIntervalMs);
            }
        }

        System.out.printf("Done producing messages to %s.%n", topic);
    }

    private static void waitForKafka(Properties props) throws InterruptedException {
        System.out.printf("Waiting for Kafka at %s...%n", props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        while (true) {
            try (AdminClient admin = AdminClient.create(props)) {
                admin.describeCluster().nodes().get();
                return;
            } catch (ExecutionException e) {
                Thread.sleep(2_000);
            }
        }
    }

    private static void createTopicIfMissing(Properties props, String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> topicNames = admin.listTopics().names().get();
            if (topicNames.contains(topic)) {
                System.out.printf("Topic %s already exists.%n", topic);
                return;
            }

            NewTopic newTopic = new NewTopic(topic, 3, (short) 3);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.printf("Created topic %s.%n", topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.printf("Topic %s already exists.%n", topic);
                return;
            }
            throw e;
        }
    }

    private static void produce(KafkaProducer<String, String> producer, String topic, int id, String logMessage)
            throws ExecutionException, InterruptedException {
        String key = "parent-" + id;
        String value = String.format(
                "{\"id\":%d,\"parentId\":\"parent-%d\",\"name\":\"Parent %d\",\"createdAt\":\"%s\"}",
                id,
                id,
                id,
                Instant.now());

        producer.send(new ProducerRecord<>(topic, key, value)).get();
        System.out.println(logMessage);
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int intEnv(String name, int defaultValue) {
        return Integer.parseInt(env(name, Integer.toString(defaultValue)));
    }

    private static long longEnv(String name, long defaultValue) {
        return Long.parseLong(env(name, Long.toString(defaultValue)));
    }
}
