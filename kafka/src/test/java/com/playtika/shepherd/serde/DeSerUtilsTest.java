package com.playtika.shepherd.serde;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DeSerUtilsTest {

    @Test
    public void shouldDeserString(){
        SerDe<String> deSer = SerDeUtils.getSerDe(String.class);

        List<String> population = List.of("1", "2");
        List<ByteBuffer> serialized = deSer.serialize(population);
        List<String> deserialized = deSer.deserialize(List.copyOf(serialized));

        assertThat(deserialized).containsExactlyInAnyOrderElementsOf(population);
    }

    @Test
    public void shouldDeserByteBuffer(){
        SerDe<ByteBuffer> serDe = SerDeUtils.BYTE_BUFFER_DE_SER;

        List<ByteBuffer> population = List.of(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2}));
        List<ByteBuffer> serialized = serDe.serialize(population);
        List<ByteBuffer> deserialized = serDe.deserialize(List.copyOf(serialized));

        assertThat(deserialized).containsExactlyInAnyOrderElementsOf(population);
    }
}
