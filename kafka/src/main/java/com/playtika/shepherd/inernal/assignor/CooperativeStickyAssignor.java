package com.playtika.shepherd.inernal.assignor;

import com.playtika.shepherd.inernal.Assignment;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.playtika.shepherd.inernal.ProtocolHelper.decompress;
import static com.playtika.shepherd.inernal.ProtocolHelper.deserializeAssignment;
import static com.playtika.shepherd.inernal.assignor.RoundRobinAssignor.assignmentsToMap;
import static com.playtika.shepherd.inernal.utils.MathUtils.ceilDivide;
import static java.util.stream.Collectors.toMap;

public class CooperativeStickyAssignor implements Assignor{

    private final Logger logger = LoggerFactory.getLogger(CooperativeStickyAssignor.class);

    @Override
    public Map<String, ByteBuffer> performAssignment(
            String leaderId, String protocol,
            List<ByteBuffer> population, long version,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {

        Map<String, Assignment> oldAssignments = toAssignmentsMap(allMemberMetadata);

        int herdSize = population.size();
        int pasturesCount = allMemberMetadata.size();
        int sheepPerPasture = ceilDivide(herdSize, pasturesCount);

        Set<ByteBuffer> keepStickyAll = new HashSet<>();

        List<AssignmentWithCapacity> newAssignmentsWithCapacity = new ArrayList<>();
        List<Assignment> newAssignments = allMemberMetadata.stream()
                .map(member -> {
                    List<ByteBuffer> oldSheep = oldAssignments.get(member.memberId()).assigned();
                    List<ByteBuffer> keepSticky = oldSheep.subList(0, Math.min(sheepPerPasture, oldSheep.size()));
                    keepStickyAll.addAll(keepSticky);
                    ArrayList<ByteBuffer> newSheep = new ArrayList<>(sheepPerPasture);
                    newSheep.addAll(keepSticky);
                    Assignment assignment = new Assignment(leaderId, version, newSheep);
                    if(newSheep.size() < sheepPerPasture){
                        newAssignmentsWithCapacity.add(new AssignmentWithCapacity(assignment.assigned()));
                    }
                    return assignment;
                })
                .toList();

        logger.info("Sticky sheep [{}] out of [{}] will remain in the same pastures after rebalance",
                keepStickyAll.size(), herdSize);

        //add not sticky sheep
        for(ByteBuffer sheep : population){
            if(keepStickyAll.contains(sheep)){
                continue;
            }

            //add sheep to assignment wih minimal similarity
            AssignmentWithCapacity selectedAssignment = newAssignmentsWithCapacity.get(0);
            Similarity selectedSimilarity = selectedAssignment.getSimilarity(sheep);
            for(AssignmentWithCapacity assignment : newAssignmentsWithCapacity.subList(1, newAssignmentsWithCapacity.size())){
                Similarity similarity = assignment.getSimilarity(sheep);
                if(similarity.similarity() < selectedSimilarity.similarity()
                        || similarity.similarity() == selectedSimilarity.similarity()
                        && assignment.getSize() < selectedAssignment.getSize()){
                    selectedAssignment = assignment;
                    selectedSimilarity = similarity;
                }
            }

            selectedAssignment.addSheep(sheep, selectedSimilarity.insertionPoint());
            if(selectedAssignment.getSize() == sheepPerPasture){
                newAssignmentsWithCapacity.remove(selectedAssignment);
            }
        }

        return assignmentsToMap(newAssignments, allMemberMetadata);
    }

    private Map<String, Assignment> toAssignmentsMap(List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata){
        return allMemberMetadata.stream()
                .collect(toMap(JoinGroupResponseData.JoinGroupResponseMember::memberId,
                        member -> {
                            if(member.metadata().length > 0){
                                return deserializeAssignment(decompress(ByteBuffer.wrap(member.metadata())));
                            } else {
                                return new Assignment(null, -1, List.of());
                            }
                        }));
    }

    private static class AssignmentWithCapacity {
        private final List<ByteBuffer> assigned;
        private final List<ByteBuffer> assignedSorted;


        public AssignmentWithCapacity(List<ByteBuffer> assigned) {
            this.assigned = assigned;
            this.assignedSorted = new ArrayList<>(assigned);
            assignedSorted.sort(Comparator.naturalOrder());
        }

        public Similarity getSimilarity(ByteBuffer sheep){
            int pos = Collections.binarySearch(assignedSorted, sheep);
            if(pos >= 0){
                throw new IllegalArgumentException("Already contains");
            }

            int insertionPoint = -(pos + 1);

            int similarity = Math.max(
                    insertionPoint > 0 ? getSimilarity(sheep, assignedSorted.get(insertionPoint - 1)) : 0,
                    insertionPoint < assignedSorted.size() ? getSimilarity(sheep, assignedSorted.get(insertionPoint)) : 0
            );

            return new Similarity(similarity, insertionPoint);
        }

        public void addSheep(ByteBuffer sheep, int insertionPoint){
            assigned.add(sheep);
            assignedSorted.add(insertionPoint, sheep);
        }

        public int getSize(){
            return assignedSorted.size();
        }

        private static int getSimilarity(ByteBuffer buf1, ByteBuffer buf2){
            return buf1.mismatch(buf2);
        }

    }

    private record Similarity(int similarity, int insertionPoint){
    }
}
