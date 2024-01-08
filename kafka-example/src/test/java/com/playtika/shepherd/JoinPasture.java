package com.playtika.shepherd;

import com.playtika.shepherd.common.Pasture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.playtika.shepherd.StartKafkaContainer.POPULATION_TOPIC_NAME;
import static com.playtika.shepherd.StartKafkaContainer.kafkaContainer;
import static org.testcontainers.shaded.org.apache.commons.lang3.StringUtils.isEmpty;

public class JoinPasture {

    private static final Logger logger = LoggerFactory.getLogger(JoinPasture.class);

    public static void main(String[] args) {

        kafkaContainer.start();

        Consumer<String, String> consumer = createConsumer();

        KafkaFarm kafkaFarm = new KafkaFarm(kafkaContainer.getBootstrapServers());


        Pasture<String> skyNet = kafkaFarm.addBreedingPasture("SkyNet", String.class,
                (population, version, generation, isLeader) -> {
                    logger.info("Assigned leader={} version={} [{}]", isLeader, version, population);
                });


        System.out.println("****************************************************");
        System.out.println("*  Joined the Kafka Farm: \n"+kafkaFarm);
        System.out.println("*  Herd name: SkyNet");
        System.out.println("****************************************************");

        runKafkaConsumer(consumer, (version, population)
                -> skyNet.getShepherd().setPopulation(population, version.intValue()));
    }

    static void runKafkaConsumer(
            Consumer<String, String> kafkaConsumer,
            BiConsumer<Long, String[]> populationConsumer) {

        Map<String, String> populationMap = new HashMap<>();
        AtomicLong maxOffset = new AtomicLong(-1);

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            consumerRecords.forEach(record -> {
                if(isEmpty(record.value())){
                    populationMap.remove(record.key());
                } else {
                    populationMap.put(record.key(), record.value());
                }
                maxOffset.set(Long.max(maxOffset.get(), record.offset()));
            });

            if(!consumerRecords.isEmpty()) {
                populationConsumer.accept(maxOffset.get(), populationMap.values().toArray(new String[0]));
            }
        }
    }

    private static final Random random = new Random();

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "PopulationConsumer-"+random.nextLong());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(POPULATION_TOPIC_NAME));
        return consumer;
    }
}
