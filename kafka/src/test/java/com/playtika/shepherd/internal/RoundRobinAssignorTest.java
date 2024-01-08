package com.playtika.shepherd.internal;

import com.playtika.shepherd.inernal.RoundRobinAssignor;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.playtika.shepherd.inernal.ProtocolHelper.decompress;
import static com.playtika.shepherd.inernal.ProtocolHelper.deserializeAssignment;
import static com.playtika.shepherd.inernal.utils.BytesUtils.toBuffs;
import static org.assertj.core.api.Assertions.assertThat;

public class RoundRobinAssignorTest {

    private final RoundRobinAssignor assignor = new RoundRobinAssignor();

    @Test
    public void shouldAssignInSortedOrder(){
        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{2},
                new byte[]{1},
                new byte[]{3}));
        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                new HashSet<>(buffs), 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("member1"),
                        new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("member2")));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).getAssigned())
                .containsExactlyInAnyOrder(buffs.get(1), buffs.get(2));

        assertThat(deserializeAssignment(decompress(assignments.get("member2"))).getAssigned())
                .containsExactlyInAnyOrder(buffs.get(0));
    }

    @Test
    public void shouldAssignAll(){
        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{2},
                new byte[]{1},
                new byte[]{3}));
        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                new HashSet<>(buffs), 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("member1")));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).getAssigned())
                .containsExactlyInAnyOrderElementsOf(buffs);
    }

}
