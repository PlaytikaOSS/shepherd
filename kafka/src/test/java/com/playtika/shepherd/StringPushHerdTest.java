package com.playtika.shepherd;

import com.playtika.shepherd.serde.SerDe;

import static com.playtika.shepherd.serde.SerDeUtils.STRING_DE_SER;

public class StringPushHerdTest extends AbstractPushHerdTest<String>{

    @Override
    protected SerDe<String> getSerDe() {
        return STRING_DE_SER;
    }

    @Override
    String[] getPopulation0() {
        return new String[0];
    }

    @Override
    String[] getPopulation1() {
        return new String[]{"1"};
    }

    @Override
    String[] getPopulation2() {
        return new String[]{"123"};
    }
}
