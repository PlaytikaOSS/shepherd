package com.playtika.shepherd.common;

import java.util.List;

/**
 * `cleanup` and `assign` divided by global sync barrier
 * None `assign` will be called until all `cleanup` finished
 * @param <Breed>
 */
public interface PastureListener<Breed> {

    /**
     * Invoked when new subpopulation assigned to this pasture
     */
    void assigned(List<Breed> population, AssignmentData populationAssignment);

    /**
     * Invoked on first phase of rebalance
     */
    default void cleanup() {}

}
