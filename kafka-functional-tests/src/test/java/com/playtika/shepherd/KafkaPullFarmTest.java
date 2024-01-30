package com.playtika.shepherd;

import com.playtika.shepherd.common.pull.Herd;
import com.playtika.shepherd.common.pull.Pasture;
import com.playtika.shepherd.common.pull.Shepherd;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.playtika.shepherd.KafkaPushFarmTest.BlackSheep;
import static com.playtika.shepherd.inernal.utils.BytesUtils.toBytes;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class KafkaPullFarmTest extends BasicKafkaTest{

    private static final Logger logger = LoggerFactory.getLogger(KafkaPullFarmTest.class);

    @Test
    public void shouldBalanceStaticHerd() {

        KafkaPullFarm kafkaRanch = new KafkaPullFarm(getBootstrapServers(), TEST_PROPERTIES);

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});

        AtomicReference<Herd.Population<ByteBuffer>> populationGlobal = new AtomicReference<>();
        int versionGlobal = 1;
        Herd<ByteBuffer> herd = new TestHerd<>("pull-static-herd", populationGlobal);

        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());

        Pasture pasture1 = kafkaRanch.addPasture(herd, (population, version, generation, isLeader) -> {
            logPopulation(1, population, version, isLeader);
            cows1.set(population);
        });

        populationGlobal.set(new Herd.Population<>(new ByteBuffer[]{cow1, cow2}, versionGlobal));
        pasture1.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //setup another pasture
        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        Pasture pasture2 = kafkaRanch.addPasture(herd, (population, version, generation, isLeader) -> {
            logPopulation(2, population, version, isLeader);
            cows2.set(population);
        });
        pasture2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
        });

        //setup third pasture
        AtomicReference<List<ByteBuffer>> cows3 = new AtomicReference<>(List.of());
        Pasture pasture3 = kafkaRanch.addPasture(herd, (population, version, generation, isLeader) -> {
            logPopulation(3, population, version, isLeader);
            cows3.set(population);
        });
        pasture3.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(Stream.of(cows1.get().stream(), cows2.get().stream(), cows3.get().stream())
                    .flatMap(byteBufferStream -> byteBufferStream).toList())
                    .containsExactlyInAnyOrder(cow1, cow2);
        });

        //stop first 2 pastures
        pasture1.close(Duration.ofSeconds(10));
        pasture2.close(Duration.ofSeconds(10));

        await().timeout(ofSeconds(3)).untilAsserted(() -> {
            assertThat(cows3.get()).containsExactlyInAnyOrder(cow1, cow2);
        });
    }

    @Test
    public void shouldBalanceDynamicHerd() {
        shouldBalanceDynamicHerd(false);
    }

    @Test
    public void shouldBalanceDynamicHerdVersioned() {
        shouldBalanceDynamicHerd(true);
    }

    private int nextVersion(int version, boolean versioned){
        return versioned ? ++version : -1;
    }

    private void shouldBalanceDynamicHerd(boolean versioned) {

        KafkaPullFarm kafkaRanch = new KafkaPullFarm(getBootstrapServers(), TEST_PROPERTIES);

        AtomicReference<Herd.Population<ByteBuffer>> populationGlobal = new AtomicReference<>();
        Herd<ByteBuffer> herd = new TestHerd<>(versioned ? "pull-dynamic-group-versioned" : "pull-dynamic-group", populationGlobal);

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});
        int ver1 = nextVersion(0, versioned);

        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());
        AtomicLong version1 = new AtomicLong();
        Pasture pasture1 = kafkaRanch.addPasture(herd, (population, version, generation, isLeader) -> {
            logPopulation(1, population, version, isLeader);
            cows1.set(population);
            version1.set(version);
        });
        populationGlobal.set(new Herd.Population<>(new ByteBuffer[]{cow1, cow2}, ver1));
        pasture1.start();

        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        AtomicLong version2 = new AtomicLong();
        Pasture pasture2 = kafkaRanch.addPasture(herd, (population, version, generation, isLeader) -> {
            logPopulation(2, population, version, isLeader);
            cows2.set(population);
            version2.set(version);
        });
        pasture2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver1);
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(version2.get()).isEqualTo(ver1);
            assertThat(cows2.get().size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //add cows to herd
        ByteBuffer cow3 = ByteBuffer.wrap(new byte[]{2});
        ByteBuffer cow4 = ByteBuffer.wrap(new byte[]{3});
        int ver2 = nextVersion(ver1, versioned);
        populationGlobal.set(new Herd.Population<>(new ByteBuffer[]{cow1, cow2, cow3, cow4}, ver2));
        Stream.of(pasture1.getShepherd(), pasture2.getShepherd()).forEach(Shepherd::rebalanceHerd);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver2);
            assertThat(cows1.get().size()).isEqualTo(2);
            assertThat(version2.get()).isEqualTo(ver2);
            assertThat(cows2.get().size()).isEqualTo(2);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow1, cow2, cow3, cow4);
        });

        //removed cows from herd
        int ver3 = nextVersion(ver2, versioned);
        populationGlobal.set(new Herd.Population<>(new ByteBuffer[]{cow3, cow4}, ver3));
        Stream.of(pasture1.getShepherd(), pasture2.getShepherd()).forEach(Shepherd::rebalanceHerd);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver3);
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(version2.get()).isEqualTo(ver3);
            assertThat(cows2.get().size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow3, cow4);
        });

        //leave only one cow
        int ver4 = nextVersion(ver3, versioned);
        populationGlobal.set(new Herd.Population<>(new ByteBuffer[]{cow4}, ver4));
        Stream.of(pasture1.getShepherd(), pasture2.getShepherd()).forEach(Shepherd::rebalanceHerd);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver4);
            assertThat(version2.get()).isEqualTo(ver4);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow4);
        });

        //leave no cow
        int ver5 = nextVersion(ver4, versioned);
        populationGlobal.set(new Herd.Population<>(new ByteBuffer[]{}, ver5));
        Stream.of(pasture1.getShepherd(), pasture2.getShepherd()).forEach(Shepherd::rebalanceHerd);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver5);
            assertThat(version2.get()).isEqualTo(ver5);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).isEmpty();
        });
    }


    @Test
    public void shouldBalanceBreedingStaticHerd() {

        KafkaPullFarm kafkaRanch = new KafkaPullFarm(getBootstrapServers(), TEST_PROPERTIES);

        BlackSheep sheep1 = new BlackSheep("hello", 2.37);
        BlackSheep sheep2 = new BlackSheep("world", 1.37);

        AtomicReference<Herd.Population<BlackSheep>> populationGlobal = new AtomicReference<>();
        populationGlobal.set(new Herd.Population<>(new BlackSheep[]{sheep1, sheep2}, 1));

        Herd<BlackSheep> herd = new TestHerd<>("pull-static-breeding-herd", populationGlobal);

        AtomicReference<List<BlackSheep>> subHerd1 = new AtomicReference<>(List.of());

        Pasture pasture1 = kafkaRanch.addBreedingPasture(herd, BlackSheep.class,
                (population, version, generation, isLeader) -> {
            logger.info("Assigned sheep1 [{}]", population);
            subHerd1.set(population);
        });
        pasture1.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(subHerd1.get()).containsExactlyInAnyOrder(sheep1, sheep2);
        });

        //setup another pasture
        AtomicReference<List<BlackSheep>> subHerd2 = new AtomicReference<>(List.of());
        Pasture pasture2 = kafkaRanch.addBreedingPasture(herd, BlackSheep.class,
                (population, version, generation, isLeader) -> {
            logger.info("Assigned sheep2 [{}]", population);
            subHerd2.set(population);
        });
        pasture2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(subHerd1.get().size()).isEqualTo(1);
            assertThat(subHerd2.get().size()).isEqualTo(1);
        });

        //setup third pasture
        AtomicReference<List<BlackSheep>> subHerd3 = new AtomicReference<>(List.of());
        Pasture pasture3 = kafkaRanch.addBreedingPasture(herd, BlackSheep.class, (population, version, generation, isLeader) -> {
            logger.info("Assigned cows3 [{}]", population);
            subHerd3.set(population);
        });
        pasture3.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(Stream.of(subHerd1.get().stream(), subHerd2.get().stream(), subHerd3.get().stream())
                    .flatMap(byteBufferStream -> byteBufferStream).toList())
                    .containsExactlyInAnyOrder(sheep1, sheep2);
        });

        //stop first 2 pastures
        pasture1.close(Duration.ofSeconds(10));
        pasture2.close(Duration.ofSeconds(10));

        await().timeout(ofSeconds(3)).untilAsserted(() -> {
            assertThat(subHerd3.get()).containsExactlyInAnyOrder(sheep1, sheep2);
        });
    }

    private static void logPopulation(int pastureIndex, List<ByteBuffer> population, long version, boolean isLeader) {
        logger.info("Assigned to pasture{} leader={} version={} [{}]", pastureIndex, isLeader, version, toBytes(population));
    }

    public static class TestHerd<Breed> implements Herd<Breed> {

        private final String name;
        private final AtomicReference<Herd.Population<Breed>> populationGlobal;

        public TestHerd(String name, AtomicReference<Population<Breed>> populationGlobal) {
            this.name = name;
            this.populationGlobal = populationGlobal;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Population<Breed> getPopulation() {
            return populationGlobal.get();
        }

        @Override
        public void reset() {}
    }

}
