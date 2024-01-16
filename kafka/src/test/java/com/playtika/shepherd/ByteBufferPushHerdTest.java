package com.playtika.shepherd;

import com.playtika.shepherd.serde.SerDe;

import java.nio.ByteBuffer;

import static com.playtika.shepherd.serde.SerDeUtils.BYTE_BUFFER_DE_SER;

public class ByteBufferPushHerdTest extends AbstractPushHerdTest<ByteBuffer>{

    @Override
    protected SerDe<ByteBuffer> getSerDe() {
        return BYTE_BUFFER_DE_SER;
    }

    @Override
    ByteBuffer[] getPopulation0() {
        return new ByteBuffer[0];
    }

    @Override
    ByteBuffer[] getPopulation1() {
        return new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1})};
    }

    @Override
    ByteBuffer[] getPopulation2() {
        return new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1, 2, 3})};
    }
}
