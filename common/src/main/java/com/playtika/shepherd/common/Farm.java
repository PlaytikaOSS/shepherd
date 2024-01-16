package com.playtika.shepherd.common;

import java.nio.ByteBuffer;

/**
 * Farm has many herds distributed evenly by pastures
 */
public interface Farm {

    /**
     * Here we come to Farm with our Pasture to graze specific Herd on it.
     *
     * @param herdName  Herd we want to inhabit this pasture
     * @param pastureListener  Will listen for animals from Herd that will be assigned to our Pasture
     * @return Shepherd allows to set Herd population
     */
    Pasture<ByteBuffer> addPasture(String herdName, PastureListener<ByteBuffer> pastureListener);

    /**
     * Here we come to Farm with our Pasture to graze specific Breeding Herd on it.
     *
     * @param herdName  Herd we want to inhabit this pasture
     * @param breedClass Only elements of this class accepted in this herd
     * @param pastureListener  Will listen for animals from Herd that will be assigned to our Pasture
     * @return Shepherd allows to set Herd population
     */
    <Breed> Pasture<Breed> addBreedingPasture(String herdName, Class<Breed> breedClass, PastureListener<Breed> pastureListener);

}
