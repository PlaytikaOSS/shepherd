package com.playtika.shepherd.inernal;

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.playtika.shepherd.inernal.ProtocolHelper.compress;
import static com.playtika.shepherd.inernal.ProtocolHelper.serializeAssignment;

/**
 * Sort population lexicographically before assignment in round-robin way
 */
public class RoundRobinAssignor implements Assignor {

    @Override
    public Map<String, ByteBuffer> performAssignment(
            String leaderId, String protocol, Set<ByteBuffer> population, int version,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {

        ArrayList<ByteBuffer> herd = new ArrayList<>(population);
        Collections.sort(herd);

        int herdSize = herd.size();
        int pasturesCount = allMemberMetadata.size();
        int sheepPerPasture = herdSize / pasturesCount + 1;

        List<Assignment> assignments = allMemberMetadata.stream()
                .map(member -> new Assignment(leaderId, version, new ArrayList<>(sheepPerPasture)))
                .toList();

        for(int sheepId = 0; sheepId < herdSize; sheepId++){
            assignments.get(sheepId % pasturesCount).getAssigned().add(herd.get(sheepId));
        }

        Map<String, ByteBuffer> assignmentsMap = new HashMap<>(assignments.size());
        for(int assignmentId = 0; assignmentId < pasturesCount; assignmentId++){
            assignmentsMap.put(allMemberMetadata.get(assignmentId).memberId(), compress(serializeAssignment(assignments.get(assignmentId))));
        }
        return assignmentsMap;
    }
}
