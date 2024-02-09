package com.playtika.shepherd.inernal;

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.util.List;

public class CheckedHerd implements Herd {

    private final Herd herd;
    private boolean requested = false;

    public static Herd checked(Herd herd){
        return new CheckedHerd(herd);
    }

    private CheckedHerd(Herd herd) {
        this.herd = herd;
    }

    @Override
    public Population getPopulation(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata){
        if(requested){
            throw new IllegalStateException("Should be called only once on rebalance");
        }
        try {
            return herd.getPopulation(allMemberMetadata);
        } finally {
            requested = true;
        }
    }

    @Override
    public void reset() {
        herd.reset();
        requested = false;
    }
}
