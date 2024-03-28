package com.playtika.shepherd.inernal.assignor;

import com.playtika.shepherd.inernal.Assignment;
import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.playtika.shepherd.inernal.ProtocolHelper.compress;
import static com.playtika.shepherd.inernal.ProtocolHelper.serializeAssignment;
import static com.playtika.shepherd.inernal.utils.MathUtils.ceilDivide;

/**
 * Sort population lexicographically before assignment in round-robin way
 */
public class RoundRobinAssignor implements Assignor {

    @Override
    public Map<String, ByteBuffer> performAssignment(
            String leaderId, String protocol, List<ByteBuffer> population, long version,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {

        int herdSize = population.size();
        int pasturesCount = allMemberMetadata.size();
        int sheepPerPasture = ceilDivide(herdSize, pasturesCount);

        List<Assignment> assignments = allMemberMetadata.stream()
                .map(member -> new Assignment(leaderId, version, new ArrayList<>(sheepPerPasture)))
                .toList();

        for(int sheepId = 0; sheepId < herdSize; sheepId++){
            assignments.get(sheepId % pasturesCount).assigned().add(population.get(sheepId));
        }

        return assignmentsToMap(assignments, allMemberMetadata);
    }

    static Map<String, ByteBuffer> assignmentsToMap(
            List<Assignment> assignments,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
        Map<String, ByteBuffer> assignmentsMap = new HashMap<>(assignments.size());
        for(int assignmentId = 0, pasturesCount = assignments.size(); assignmentId < pasturesCount; assignmentId++){
            assignmentsMap.put(allMemberMetadata.get(assignmentId).memberId(), compress(serializeAssignment(assignments.get(assignmentId))));
        }
        return assignmentsMap;
    }
}
