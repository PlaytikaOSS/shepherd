package com.playtika.shepherd.common;

import java.util.List;

/**
 * Invoked when new subpopulation assigned to this pasture
 */
public interface PastureListener<Breed> {

    void assigned(List<Breed> population, int version, int generation, boolean isLeader);

}
