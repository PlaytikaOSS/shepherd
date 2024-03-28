package com.playtika.shepherd.simple;

import com.playtika.shepherd.KafkaPullFarmTest;
import com.playtika.shepherd.inernal.Protocol;

public class SimpleKafkaPullFarmTest extends KafkaPullFarmTest {
    @Override
    protected Protocol getProtocol() {
        return Protocol.SIMPLE;
    }
}
