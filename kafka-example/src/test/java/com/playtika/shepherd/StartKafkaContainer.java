package com.playtika.shepherd;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

public class StartKafkaContainer {

    private static final Logger logger = LoggerFactory.getLogger(StartKafkaContainer.class);

    static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "400")
            .withReuse(true);
    public static final String POPULATION_TOPIC_NAME = "population";

    public static void main(String[] args) {

        try(kafkaContainer){
            kafkaContainer.start();

            try (var adminClient = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers()))) {
                adminClient.createTopics(List.of(new NewTopic(POPULATION_TOPIC_NAME, 1, (short) 1)));
            }

            logger.info("*********************************************");
            logger.info("*  Started Kafka container: "+getKafkaBrokers());
            logger.info("*  Created topic: population");
            logger.info("*********************************************\n");

            waitForExit();
        }
    }

    static void waitForExit() {
        Scanner scanner = new Scanner(System.in);
        while (!scanner.nextLine().equals("Exit")) {
        }
    }

    static String getKafkaBrokers() {
        Integer mappedPort = kafkaContainer.getFirstMappedPort();
        return String.format("%s:%d", "localhost", mappedPort);
    }
}