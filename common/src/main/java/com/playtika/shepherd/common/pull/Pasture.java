package com.playtika.shepherd.common.pull;

import java.time.Duration;

public interface Pasture {

    Shepherd getShepherd();

    void start();

    void close(Duration timeout);
}
