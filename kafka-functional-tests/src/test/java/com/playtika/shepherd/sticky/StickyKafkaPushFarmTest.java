package com.playtika.shepherd.sticky;

import com.playtika.shepherd.KafkaPushFarmTest;
import com.playtika.shepherd.inernal.Protocol;

public class StickyKafkaPushFarmTest extends KafkaPushFarmTest {
    @Override
    protected Protocol getProtocol() {
        return Protocol.COOPERATIVE;
    }
}
