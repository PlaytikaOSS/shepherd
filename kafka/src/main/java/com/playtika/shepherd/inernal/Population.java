package com.playtika.shepherd.inernal;

import java.nio.ByteBuffer;
import java.util.List;

public class Population {

    private final List<ByteBuffer> sheep;
    private final long version;

    public Population(List<ByteBuffer> sheep, long version) {
        this.sheep = sheep;
        this.version = version;
    }

    public List<ByteBuffer> getSheep() {
        return sheep;
    }

    public long getVersion() {
        return version;
    }
}
