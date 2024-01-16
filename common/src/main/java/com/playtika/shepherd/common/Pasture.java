package com.playtika.shepherd.common;

import java.time.Duration;

public interface Pasture<Breed> {

    Shepherd<Breed> getShepherd();

    void close(Duration timeout);
}
