package com.playtika.shepherd;

import com.playtika.shepherd.common.AssignmentData;
import com.playtika.shepherd.common.PastureListener;
import com.playtika.shepherd.inernal.PastureShepherd;
import com.playtika.shepherd.inernal.Population;
import com.playtika.shepherd.serde.SerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static com.playtika.shepherd.KafkaPushFarm.NO_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

abstract public class AbstractPushHerdTest<Breed> {

    private final PastureListener<Breed> pastureListener = mock(PastureListener.class);
    private final PastureShepherd pastureShepherd = mock(PastureShepherd.class);

    private KafkaPushFarm.PushHerd<Breed> pushHerd;

    abstract protected SerDe<Breed> getSerDe();

    @BeforeEach
    public void setUp(){
        pushHerd = new KafkaPushFarm.PushHerd<>(pastureListener, getSerDe());
        pushHerd.setPastureShepherd(pastureShepherd);
    }

    abstract Breed[] getPopulation0();

    abstract Breed[] getPopulation1();

    abstract Breed[] getPopulation2();

    @Test
    public void shouldCallRebalanceForLeaderOnly(){
        //set initial population
        Breed[] population0 = getPopulation0();
        boolean rebalance = pushHerd.setPopulation(population0, -1);
        assertThat(rebalance).isFalse();
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        assertThat(pushHerd.getPopulation(null).getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));

        rebalance = pushHerd.setPopulation(getPopulation1(), 0);
        assertThat(rebalance).isTrue();
        //now it triggered rebalance because now it's leader
        verify(pastureShepherd).setNeedsReconfigRebalance();
        reset(pastureShepherd);

        Breed[] population2 = getPopulation2();
        rebalance = pushHerd.setPopulation(population2, 0);
        assertThat(rebalance).isFalse();
        //consecutive call will not trigger rebalance
        verifyNoInteractions(pastureShepherd);

        assertThat(pushHerd.getPopulation(null).getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population2)));
    }

    @Test
    public void shouldNotUpdateIfTheSamePopulation(){
        //set initial population
        Breed[] population0 = getPopulation0();
        boolean rebalance = pushHerd.setPopulation(population0, NO_VERSION);
        assertThat(rebalance).isFalse();
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        Population population = pushHerd.getPopulation(null);
        assertThat(population.getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));

        //set update with the same population
        rebalance = pushHerd.setPopulation(population0, NO_VERSION);
        assertThat(rebalance).isFalse();
        verifyNoInteractions(pastureShepherd);

        //set update with the new population
        rebalance = pushHerd.setPopulation(getPopulation1(), NO_VERSION);
        assertThat(rebalance).isTrue();
        verify(pastureShepherd).setNeedsReconfigRebalance();
    }

    @Test
    public void shouldNotUpdateIfOutdatedVersion(){
        //set initial population
        Breed[] population0 = getPopulation0();
        boolean rebalance = pushHerd.setPopulation(population0, 0);
        assertThat(rebalance).isFalse();
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        Population population = pushHerd.getPopulation(null);
        assertThat(population.getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));
        pushHerd.assigned(new ArrayList<>(population.getSheep()),
                new AssignmentData(0,"test-member-id", 1,  true));

        //set update with the same version
        Breed[] population1 = getPopulation1();
        rebalance = pushHerd.setPopulation(population1, 0);
        assertThat(rebalance).isFalse();
        verifyNoInteractions(pastureShepherd);

        //set update with the new population
        rebalance = pushHerd.setPopulation(population1, 1);
        assertThat(rebalance).isTrue();
        verify(pastureShepherd).setNeedsReconfigRebalance();
    }

    @Test
    public void shouldNotUpdateIfTheSameVersion(){
        //set initial population
        Breed[] population0 = getPopulation0();
        boolean rebalance = pushHerd.setPopulation(population0, 1);
        assertThat(rebalance).isFalse();
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        Population population = pushHerd.getPopulation(null);
        assertThat(population.getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));

        //set update with the same version
        rebalance = pushHerd.setPopulation(getPopulation1(), 1);
        assertThat(rebalance).isFalse();
        verifyNoInteractions(pastureShepherd);

        //set update with the new version
        rebalance = pushHerd.setPopulation(getPopulation1(), 2);
        assertThat(rebalance).isTrue();
        verify(pastureShepherd).setNeedsReconfigRebalance();
    }
}
