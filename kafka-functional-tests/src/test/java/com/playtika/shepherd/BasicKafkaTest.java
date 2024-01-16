package com.playtika.shepherd;

import com.playtika.shepherd.inernal.DistributedConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class BasicKafkaTest {

    protected static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "400");

    protected static final Map<String, String> TEST_PROPERTIES = Map.of(
            DistributedConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500",
            DistributedConfig.SESSION_TIMEOUT_MS_CONFIG, "1500"
    );

    @BeforeAll
    public static void setUp() {
        kafka.start();
    }

    public static String getBootstrapServers(){
        return kafka.getBootstrapServers();
    }

    @AfterAll
    public static void tearDown() {
        kafka.stop();
    }

}
