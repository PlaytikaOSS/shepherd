package com.playtika.shepherd.common.pull;

import com.playtika.shepherd.common.PastureListener;

import java.nio.ByteBuffer;

/**
 * Farm has many herds distributed evenly by pastures
 */
public interface Farm {

    /**
     * Here we come to Farm with our Pasture to graze specific Herd on it.
     *
     * @param herd  Herd we want to inhabit this pasture
     * @param pastureListener  Will listen for animals from Herd that will be assigned to our Pasture
     * @return Shepherd allows to set Herd population
     */
    Pasture addPasture(Herd<ByteBuffer> herd, PastureListener<ByteBuffer> pastureListener);

    /**
     * Here we come to Farm with our Pasture to graze specific Breeding Herd on it.
     *
     * @param herd  Herd we want to inhabit this pasture
     * @param breedClass Only elements of this class accepted in this herd
     * @param pastureListener  Will listen for animals from Herd that will be assigned to our Pasture
     * @return Shepherd allows to set Herd population
     */
    <Breed> Pasture addBreedingPasture(Herd<Breed> herd, Class<Breed> breedClass, PastureListener<Breed> pastureListener);

}
