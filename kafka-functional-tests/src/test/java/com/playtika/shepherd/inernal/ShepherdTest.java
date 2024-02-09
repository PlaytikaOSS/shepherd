package com.playtika.shepherd.inernal;

import com.playtika.shepherd.BasicKafkaTest;
import com.playtika.shepherd.common.PastureListener;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.playtika.shepherd.inernal.CheckedHerd.checked;
import static com.playtika.shepherd.inernal.utils.BytesUtils.toBytes;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;


public class ShepherdTest extends BasicKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(ShepherdTest.class);

    @Test
    public void shouldBalanceStaticHerd() {

        String groupId = "static-herd";

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});
        AtomicInteger pasturesCountParameter = new AtomicInteger();

        Herd herd = checked(new Herd() {
            @Override
            public Population getPopulation(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
                pasturesCountParameter.set(allMemberMetadata.size());
                return new Population(List.of(cow1, cow2), -1);
            }

            @Override
            public void reset() {
            }
        });


        LinkedBlockingQueue<ByteBuffer> cows1 = new LinkedBlockingQueue<>();
        PastureListener<ByteBuffer> rebalanceListener1 = new PastureListener<>() {
            @Override
            public void assigned(List<ByteBuffer> population, long version, int generation, boolean isLeader) {
                logger.info("Assigned cows1 [{}]", toBytes(population));
                cows1.addAll(population);
            }

            @Override
            public void cleanup() {
                cows1.clear();
            }
        };

        PastureShepherd herder1 = new PastureShepherdBuilder()
                .setBootstrapServers(getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener1)
                .setProperties(TEST_PROPERTIES)
                .build();

        herder1.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1).containsExactlyInAnyOrder(cow1, cow2);
            assertThat(pasturesCountParameter.get()).isEqualTo(1);
        });

        //setup another pasture
        LinkedBlockingQueue<ByteBuffer> cows2 = new LinkedBlockingQueue<>();
        PastureListener<ByteBuffer> rebalanceListener2 = new PastureListener<>() {
            @Override
            public void assigned(List<ByteBuffer> population, long version, int generation, boolean isLeader) {
                logger.info("Assigned cows2 [{}]", toBytes(population));
                cows2.addAll(population);
            }

            @Override
            public void cleanup() {
                cows2.clear();
            }
        };

        PastureShepherd herder2 = new PastureShepherdBuilder()
                .setBootstrapServers(getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener2)
                .setProperties(TEST_PROPERTIES)
                .build();
        herder2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.size()).isEqualTo(1);
            assertThat(cows2.size()).isEqualTo(1);
            assertThat(pasturesCountParameter.get()).isEqualTo(2);
        });

        //stop first pasture
        herder1.stop(Duration.ofSeconds(10).toMillis());

        await().timeout(ofSeconds(3)).untilAsserted(() -> {
            assertThat(cows2).containsExactlyInAnyOrder(cow1, cow2);
            assertThat(pasturesCountParameter.get()).isEqualTo(1);
        });

    }

    @Test
    public void shouldBalanceDynamicHerd() {

        String groupId = "dynamic-group";

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});
        List<ByteBuffer> population = new CopyOnWriteArrayList<>(List.of(cow1, cow2));
        AtomicInteger version = new AtomicInteger(1);
        AtomicInteger pasturesCountParameter = new AtomicInteger();

        Herd herd = checked(new Herd() {
            @Override
            public Population getPopulation(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
                pasturesCountParameter.set(allMemberMetadata.size());
                return new Population(population, version.get());
            }

            @Override
            public void reset() {
            }
        });

        LinkedBlockingQueue<ByteBuffer> cows1 = new LinkedBlockingQueue<>();
        PastureListener<ByteBuffer> rebalanceListener1 = new PastureListener<>() {
            @Override
            public void assigned(List<ByteBuffer> population, long version, int generation, boolean isLeader) {
                logger.info("Assigned cows1 [{}]", toBytes(population));
                cows1.addAll(population);
            }

            @Override
            public void cleanup() {
                cows1.clear();
            }
        };

        PastureShepherd herder1 = new PastureShepherdBuilder()
                .setBootstrapServers(getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener1)
                .setProperties(TEST_PROPERTIES)
                .build();


        //setup another pasture
        LinkedBlockingQueue<ByteBuffer> cows2 = new LinkedBlockingQueue<>();
        PastureListener<ByteBuffer> rebalanceListener2 = new PastureListener<>() {
            @Override
            public void assigned(List<ByteBuffer> population, long version, int generation, boolean isLeader) {
                logger.info("Assigned cows2 [{}]", toBytes(population));
                cows2.addAll(population);
            }

            @Override
            public void cleanup() {
                cows2.clear();
            }
        };

        PastureShepherd herder2 = new PastureShepherdBuilder()
                .setBootstrapServers(getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener2)
                .setProperties(TEST_PROPERTIES)
                .build();

        herder1.start();
        herder2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.size()).isEqualTo(1);
            assertThat(cows2.size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.stream(), cows2.stream()).toList()).containsExactlyInAnyOrder(cow1, cow2);
            assertThat(pasturesCountParameter.get()).isEqualTo(2);
        });

        //add cows to herd
        ByteBuffer cow3 = ByteBuffer.wrap(new byte[]{2});
        ByteBuffer cow4 = ByteBuffer.wrap(new byte[]{3});

        population.addAll(List.of(cow3, cow4));
        version.set(2);

        Stream.of(herder1, herder2).filter(PastureShepherd::isLeaderElected).forEach(PastureShepherd::setNeedsReconfigRebalance);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.size()).isEqualTo(2);
            assertThat(cows2.size()).isEqualTo(2);
            assertThat(Stream.concat(cows1.stream(), cows2.stream()).toList()).containsExactlyInAnyOrder(cow1, cow2, cow3, cow4);
        });

        //removed cows from herd
        population.removeAll(List.of(cow1, cow2));
        version.set(3);

        Stream.of(herder1, herder2).filter(PastureShepherd::isLeaderElected).forEach(PastureShepherd::setNeedsReconfigRebalance);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.size()).isEqualTo(1);
            assertThat(cows2.size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.stream(), cows2.stream()).toList()).containsExactlyInAnyOrder(cow3, cow4);
        });
    }

}
