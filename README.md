[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.playtika.shepherd/shepherd-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.playtika.shepherd/shepherd-parent)

# Shepherd
Allows to balance set of POJO objects in distributed environment. 
May be extremely useful if you need to scale your service horizontally by distributing jobs/worker among instances.

It relies on Kafka Rebalance Protocol extending `org.apache.kafka.clients.consumer.internals.AbstractCoordinator`.

Thereby it inherits well-known Kafka heartbeat mechanics and its config parameters (`heartbeat.interval.ms`, `session.timeout.ms`) 

### Usage

```java
     
/* Specify Kafka servers (will be used for rebalance only) */
KafkaPushFarm kafkaPushFarm = new KafkaPushFarm(kafkaBootstrapServers);

/* Join the pasture to herd of specific name and class */
Pasture<String> skyNet = kafkaPushFarm.addBreedingPasture("SkyNet", String.class,
    /* listener that will be use to update local assignment to this pasture */
    (population, version, generation, isLeader) -> {
    logger.info("Assigned leader={} version={} [{}]", isLeader, version, population);
    });

/* set global population that will be distributed among all members of this herd */
skyNet.getShepherd().setPopulation(population, version.intValue())

skyNet.start();
```

### Modules

#### common
Contains basic interfaces

`Farm` - entry point to join herd

`Pasture` - represents local pasture

`Shepherd` - allows to set up global population for herd

`PastureListener` - listen for any changes in population on local pasture

#### kafka
Kafka based implementation 

#### kafka-example
Demo example that reads messages from Kafka aggregate them in local repository and distribute among service instances
You need to have Docker in the running environment.
1. run StartKafkaContainer
2. run several times JoinPasture
3. add messages to "population" topic