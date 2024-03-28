package com.playtika.shepherd.sticky;

import com.playtika.shepherd.KafkaPullFarmTest;
import com.playtika.shepherd.inernal.Protocol;

public class StickyKafkaPullFarmTest extends KafkaPullFarmTest {
    @Override
    protected Protocol getProtocol() {
        return Protocol.COOPERATIVE;
    }
}
