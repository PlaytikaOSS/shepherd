package com.playtika.shepherd.common.pull;

/**
 * Provides population
 * @param <Breed>
 */
public interface Herd<Breed> {

    String getName();

    Population<Breed> getPopulation();

    void reset();

    record Population<Breed>(Breed[] population, long version) {
    }
}
