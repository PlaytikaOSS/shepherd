package com.playtika.shepherd;

import com.playtika.shepherd.common.push.Pasture;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.playtika.shepherd.BasicKafkaTest.TEST_PROPERTIES;
import static com.playtika.shepherd.inernal.utils.BytesUtils.toBytes;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class KafkaToxiFarmTest {

    private static final Logger logger = LoggerFactory.getLogger(KafkaToxiFarmTest.class);

    private static final String KAFKA_ALIAS = "kafka-container";
    private static final int EXTERNAL_PLAINTEXT_PORT = 9093;
    private static final int PROXY_PORT = 8666;
    private static final int INTERNAL_TOXI_PORT = 9094;


    private static final Network network = Network.newNetwork();
    private static final ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);
    private static final Proxy proxy;
    static {
        toxiproxy.start();

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        try {
            proxy = toxiproxyClient.createProxy("kafka", "0.0.0.0:" + PROXY_PORT,
                    KAFKA_ALIAS+":"+INTERNAL_TOXI_PORT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1")){
        @Override
        public String getBootstrapServers() {
            List<String> servers = new ArrayList<>();
            servers.add("EXTERNAL_PLAINTEXT://" + getHost() + ":" + getMappedPort(EXTERNAL_PLAINTEXT_PORT));
            servers.add("TOXIPROXY_INTERNAL_PLAINTEXT://" + getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT));
            return String.join(",", servers);
        }
    }
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "EXTERNAL_PLAINTEXT:PLAINTEXT," +
                            "BROKER:PLAINTEXT," +
                            "TOXIPROXY_INTERNAL_PLAINTEXT:PLAINTEXT"
            )
            .withEnv("KAFKA_LISTENERS",
                    "EXTERNAL_PLAINTEXT://0.0.0.0:" + EXTERNAL_PLAINTEXT_PORT + "," +
                            "TOXIPROXY_INTERNAL_PLAINTEXT://0.0.0.0:" + INTERNAL_TOXI_PORT + "," +
                            "BROKER://0.0.0.0:9092"
            )
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")

            .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "400")
            .withNetwork(network)
            .withNetworkAliases(KAFKA_ALIAS);




    @BeforeAll
    public static void setUp() {
        kafka.start();
    }

    @AfterAll
    public static void tearDown() {
        toxiproxy.close();
        kafka.close();
        network.close();
    }

    public static String getBootstrapServers(){
        return String.format("%s:%d", kafka.getHost(), kafka.getMappedPort(EXTERNAL_PLAINTEXT_PORT));
    }

    public static String getToxiBootstrapServers(){
        return String.format("%s:%d", toxiproxy.getHost(), toxiproxy.getMappedPort(PROXY_PORT));
    }

    public static void stopToxiProxy() throws IOException {
        proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
        proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);
    }

    public static void restoreToxiProxy() throws IOException {
        proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
        proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
    }

    @Test
    public void shouldRestoreBalanceForStaticHerd() throws IOException {

        KafkaPushFarm kafkaRanch = new KafkaPushFarm(getBootstrapServers(), TEST_PROPERTIES);

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});

        ByteBuffer[] cows = new ByteBuffer[]{cow1, cow2};

        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());

        String herdName = "static-herd";
        Pasture<ByteBuffer> pasture1 = kafkaRanch.addPasture(herdName, (population, assignmentData) -> {
            logger.info("Assigned cows1 [{}]", toBytes(population));
            cows1.set(population);
        });

        pasture1.getShepherd().setPopulation(cows, -1);
        pasture1.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //setup toxi pasture
        KafkaPushFarm kafkaToxiRanch = new KafkaPushFarm(getToxiBootstrapServers(), TEST_PROPERTIES);
        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        Pasture<ByteBuffer> pasture2 = kafkaToxiRanch.addPasture(herdName, (population, assignmentData) -> {
            logger.info("Assigned cows2 [{}]", toBytes(population));
            cows2.set(population);
        });
        pasture2.getShepherd().setPopulation(cows, -1);
        pasture2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
        });

        //cut connection to toxy pasture
        stopToxiProxy();

        await().timeout(ofSeconds(20)).untilAsserted(() -> {
            assertThat(cows1.get()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //restore connection to toxy pasture
        restoreToxiProxy();

        await().timeout(ofSeconds(20)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
        });
    }
}
