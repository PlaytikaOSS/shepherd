package com.playtika.shepherd.sticky;

import com.playtika.shepherd.inernal.Protocol;
import com.playtika.shepherd.inernal.ShepherdTest;

public class StickyShepherdTest extends ShepherdTest {

    @Override
    protected Protocol getProtocol() {
        return Protocol.COOPERATIVE;
    }
}
