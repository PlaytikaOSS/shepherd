package com.playtika.shepherd;

import com.playtika.shepherd.common.AssignmentData;
import com.playtika.shepherd.common.PastureListener;
import com.playtika.shepherd.common.pull.Farm;
import com.playtika.shepherd.common.pull.Herd;
import com.playtika.shepherd.common.pull.Pasture;
import com.playtika.shepherd.common.pull.Shepherd;
import com.playtika.shepherd.inernal.PastureShepherd;
import com.playtika.shepherd.inernal.PastureShepherdBuilder;
import com.playtika.shepherd.inernal.Population;
import com.playtika.shepherd.serde.SerDe;
import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.playtika.shepherd.serde.SerDeUtils.BYTE_BUFFER_DE_SER;
import static com.playtika.shepherd.serde.SerDeUtils.getSerDe;

public class KafkaPullFarm implements Farm {

    private final String bootstrapServers;
    private final Map<String, String> properties;

    public KafkaPullFarm(String bootstrapServers) {
        this(bootstrapServers, Map.of());
    }

    public KafkaPullFarm(String bootstrapServers, Map<String, String> properties) {
        this.bootstrapServers = bootstrapServers;
        this.properties = properties;
    }

    @Override
    public Pasture addPasture(Herd<ByteBuffer> herd, PastureListener<ByteBuffer> pastureListener) {
        return addBreedingPasture(herd, BYTE_BUFFER_DE_SER, pastureListener);
    }

    @Override
    public <Breed> Pasture addBreedingPasture(Herd<Breed> herd, Class<Breed> breedClass, PastureListener<Breed> pastureListener) {
        return addBreedingPasture(herd, getSerDe(breedClass), pastureListener);
    }

    private  <Breed> Pasture addBreedingPasture(Herd<Breed> herd, SerDe<Breed> serDe, PastureListener<Breed> pastureListener){
        PullHerd<Breed> pullHerd = new PullHerd<>(herd, serDe, pastureListener);

        PastureShepherd pastureShepherd = new PastureShepherdBuilder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(herd.getName())
                .setProperties(properties)
                .setRebalanceListener(pullHerd)
                .setHerd(pullHerd)
                .build();

        pullHerd.setPastureShepherd(pastureShepherd);

        return pullHerd;
    }

    static class PullHerd<Breed> implements com.playtika.shepherd.inernal.Herd, Pasture, Shepherd, PastureListener<ByteBuffer>{

        private final Herd<Breed> herd;
        private final SerDe<Breed> serDe;
        private final PastureListener<Breed> pastureListener;
        private PastureShepherd pastureShepherd;

        PullHerd(Herd<Breed> herd, SerDe<Breed> serDe, PastureListener<Breed> pastureListener) {
            this.herd = herd;
            this.serDe = serDe;
            this.pastureListener = pastureListener;
        }

        @Override
        public Population getPopulation(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
            Herd.Population<Breed> population = herd.getPopulation(allMemberMetadata.size());
            return new Population(serDe.serialize(List.of(population.population())), population.version());
        }

        @Override
        public void reset() {
            herd.reset();
        }

        @Override
        public Shepherd getShepherd() {
            return this;
        }

        @Override
        public void start() {
            pastureShepherd.start();
        }

        @Override
        public void close(Duration timeout) {
            pastureShepherd.stop(timeout.toMillis());
        }

        @Override
        public void rebalanceHerd() {
            if(pastureShepherd.isLeaderElected()){
                pastureShepherd.setNeedsReconfigRebalance();
            }
        }

        @Override
        public void assigned(List<ByteBuffer> population, AssignmentData assignmentData) {
            pastureListener.assigned(serDe.deserialize(population), assignmentData);
        }

        @Override
        public void cleanup() {
            pastureListener.cleanup();
        }

        public void setPastureShepherd(PastureShepherd pastureShepherd) {
            this.pastureShepherd = pastureShepherd;
        }
    }
}
