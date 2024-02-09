package com.playtika.shepherd.inernal;

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.util.List;

public interface Herd {

    /**
     * Assignor will request it once on assignment
     * @return creatures that should be evenly distributed across pastures
     */
    Population getPopulation(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);

    /**
     * called each time on first phase of rebalance
     */
    void reset();

}
