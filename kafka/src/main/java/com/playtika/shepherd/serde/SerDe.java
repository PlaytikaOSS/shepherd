package com.playtika.shepherd.serde;

import java.nio.ByteBuffer;
import java.util.List;

public interface SerDe<Breed> {
    List<ByteBuffer> serialize(List<Breed> population);

    List<Breed> deserialize(List<ByteBuffer> assignment);
}
