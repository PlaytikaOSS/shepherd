package com.playtika.shepherd.common.push;

import java.time.Duration;

public interface Pasture<Breed> {

    Shepherd<Breed> getShepherd();

    /**
     * Set population via Shepherd before calling this method
     */
    void start();

    void close(Duration timeout);
}
