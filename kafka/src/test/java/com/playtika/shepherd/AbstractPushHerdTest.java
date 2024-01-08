package com.playtika.shepherd;

import com.playtika.shepherd.common.PastureListener;
import com.playtika.shepherd.inernal.PastureShepherd;
import com.playtika.shepherd.inernal.Population;
import com.playtika.shepherd.serde.SerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.playtika.shepherd.KafkaFarm.NO_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

abstract public class AbstractPushHerdTest<Breed> {

    private final PastureListener<Breed> pastureListener = mock(PastureListener.class);
    private final PastureShepherd pastureShepherd = mock(PastureShepherd.class);

    private KafkaFarm.PushHerd<Breed> pushHerd;

    abstract protected SerDe<Breed> getSerDe();

    @BeforeEach
    public void setUp(){
        pushHerd = new KafkaFarm.PushHerd<>(pastureListener, getSerDe());
        pushHerd.setPastureShepherd(pastureShepherd);
    }

    abstract Breed[] getPopulation0();

    abstract Breed[] getPopulation1();

    abstract Breed[] getPopulation2();

    @Test
    public void shouldCallRebalanceForLeaderOnly(){
        //set initial population
        Breed[] population0 = getPopulation0();
        pushHerd.setPopulation(population0, -1);
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        assertThat(pushHerd.getPopulation().getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));

        pushHerd.setPopulation(getPopulation1(), 0);
        //now it triggered rebalance because now it's leader
        verify(pastureShepherd).setNeedsReconfigRebalance();
        reset(pastureShepherd);

        Breed[] population2 = getPopulation2();
        pushHerd.setPopulation(population2, 0);
        //consecutive call will not trigger rebalance
        verifyNoInteractions(pastureShepherd);

        assertThat(pushHerd.getPopulation().getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population2)));
    }

    @Test
    public void shouldNotUpdateIfTheSamePopulation(){
        //set initial population
        Breed[] population0 = getPopulation0();
        pushHerd.setPopulation(population0, NO_VERSION);
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        Population population = pushHerd.getPopulation();
        assertThat(population.getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));

        //set update with the same population
        pushHerd.setPopulation(population0, NO_VERSION);
        verifyNoInteractions(pastureShepherd);

        //set update with the new population
        pushHerd.setPopulation(getPopulation1(), NO_VERSION);
        verify(pastureShepherd).setNeedsReconfigRebalance();
    }

    @Test
    public void shouldNotUpdateIfTheSameVersion(){
        //set initial population
        Breed[] population0 = getPopulation0();
        pushHerd.setPopulation(population0, 1);
        //no interaction cause not elected as leader
        verifyNoInteractions(pastureShepherd);

        //called when elected as leader
        Population population = pushHerd.getPopulation();
        assertThat(population.getSheep()).containsExactlyElementsOf(
                getSerDe().serialize(Arrays.asList(population0)));

        //set update with the same version
        pushHerd.setPopulation(getPopulation1(), 1);
        verifyNoInteractions(pastureShepherd);

        //set update with the new version
        pushHerd.setPopulation(getPopulation1(), 2);
        verify(pastureShepherd).setNeedsReconfigRebalance();
    }
}
