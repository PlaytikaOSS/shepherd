package com.playtika.shepherd.simple;

import com.playtika.shepherd.inernal.Protocol;
import com.playtika.shepherd.inernal.ShepherdTest;

public class SimpleShepherdTest extends ShepherdTest {

    @Override
    protected Protocol getProtocol() {
        return Protocol.SIMPLE;
    }
}
