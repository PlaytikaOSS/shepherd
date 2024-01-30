package com.playtika.shepherd.common.push;

/**
 * Allows to set herd population
 * @param <Breed>
 */
public interface Shepherd<Breed> {

    /**
     *
     * @param population
     * @param version
     * @return true if it will cause rebalance, false if population will be ignored
     */

    boolean setPopulation(Breed[] population, long version);

}
