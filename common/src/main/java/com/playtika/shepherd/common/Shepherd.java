package com.playtika.shepherd.common;

/**
 * Allows to set herd population
 * @param <Breed>
 */
public interface Shepherd<Breed> {

    void setPopulation(Breed[] population, int version);

}
