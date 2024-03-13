package com.playtika.shepherd;

import com.playtika.shepherd.common.PastureListener;
import com.playtika.shepherd.common.push.Farm;
import com.playtika.shepherd.common.push.Pasture;
import com.playtika.shepherd.common.push.Shepherd;
import com.playtika.shepherd.inernal.Herd;
import com.playtika.shepherd.inernal.PastureShepherd;
import com.playtika.shepherd.inernal.PastureShepherdBuilder;
import com.playtika.shepherd.inernal.Population;
import com.playtika.shepherd.serde.SerDe;
import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.playtika.shepherd.inernal.utils.CacheUtils.memoize;
import static com.playtika.shepherd.serde.SerDeUtils.BYTE_BUFFER_DE_SER;
import static com.playtika.shepherd.serde.SerDeUtils.getSerDe;

public class KafkaPushFarm implements Farm {

    public static final int NO_VERSION = -1;
    private final String bootstrapServers;
    private final Map<String, String> properties;

    public KafkaPushFarm(String bootstrapServers) {
        this(bootstrapServers, Map.of());
    }

    public KafkaPushFarm(String bootstrapServers, Map<String, String> properties) {
        this.bootstrapServers = bootstrapServers;
        this.properties = properties;
    }

    @Override
    public Pasture<ByteBuffer> addPasture(String herdName, PastureListener<ByteBuffer> pastureListener) {
        return addBreedingPasture(herdName, BYTE_BUFFER_DE_SER, pastureListener);
    }

    @Override
    public <Breed> Pasture<Breed> addBreedingPasture(String herdName, Class<Breed> breedClass, PastureListener<Breed> pastureListener) {
        return addBreedingPasture(herdName, getSerDe(breedClass), pastureListener);
    }

    private  <Breed> Pasture<Breed> addBreedingPasture(String herdName, SerDe<Breed> serDe, PastureListener<Breed> pastureListener){
        PushHerd<Breed> pushHerd = new PushHerd<>(pastureListener, serDe);

        PastureShepherd pastureShepherd = new PastureShepherdBuilder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(herdName)
                .setProperties(properties)
                .setRebalanceListener(pushHerd)
                .setHerd(pushHerd)
                .build();

        pushHerd.setPastureShepherd(pastureShepherd);

        return pushHerd;
    }

    @Override
    public String toString() {
        return "KafkaFarm{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", properties=" + properties +
                '}';
    }

    static final class PushHerd<Breed> implements Herd, Pasture<Breed>, Shepherd<Breed>, PastureListener<ByteBuffer> {

        private final PastureListener<Breed> pastureListener;
        private final SerDe<Breed> serDe;

        private PastureShepherd pastureShepherd;

        private Population snapshot;
        private Population latest;

        private long assignedVersion = Long.MIN_VALUE;

        PushHerd(PastureListener<Breed> pastureListener, SerDe<Breed> serDe) {
            this.pastureListener = pastureListener;
            this.serDe = serDe;
        }

        @Override
        public synchronized boolean setPopulation(Breed[] population, long version) {
            //Ignore outdated non-static version
            if(version >=0 && version <= assignedVersion){
                return false;
            }

            Supplier<List<ByteBuffer>> latest = memoize(() -> serDe.serialize(Arrays.asList(population)));
            if(this.snapshot == null
                    || version >= 0 && version > this.snapshot.getVersion()
                    || version < 0 && !this.snapshot.getSheep().equals(latest.get())){
                this.latest = new Population(latest.get(), version);
                //call rebalance on new population for leader only
                if(snapshot != null){
                    this.snapshot = null;
                    pastureShepherd.setNeedsReconfigRebalance();
                    return true;
                }
            }

            return false;
        }

        @Override
        public synchronized Population getPopulation(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
            if(snapshot != null){
                throw new IllegalStateException("Should be called only once on rebalance");
            }
            if(latest == null){
                throw new IllegalStateException("Herd was not initialized before rebalance");
            }

            snapshot = latest;
            latest = null;
            return snapshot;
        }

        @Override
        public synchronized void reset() {
            if(snapshot != null) {
                latest = snapshot;
                snapshot = null;
            }
        }

        @Override
        public synchronized void assigned(List<ByteBuffer> population, long version, int generation, boolean isLeader) {
            this.pastureListener.assigned(serDe.deserialize(population), version, generation, isLeader);
            this.assignedVersion = version;
        }

        @Override
        public void cleanup() {
            this.pastureListener.cleanup();
        }

        @Override
        public Shepherd<Breed> getShepherd() {
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

        public void setPastureShepherd(PastureShepherd pastureShepherd) {
            this.pastureShepherd = pastureShepherd;
        }
    }
}
