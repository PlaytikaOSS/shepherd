package com.playtika.shepherd.simple;

import com.playtika.shepherd.KafkaPushFarmTest;
import com.playtika.shepherd.inernal.Protocol;

public class SimpleKafkaPushFarmTest extends KafkaPushFarmTest {
    @Override
    protected Protocol getProtocol() {
        return Protocol.SIMPLE;
    }
}
