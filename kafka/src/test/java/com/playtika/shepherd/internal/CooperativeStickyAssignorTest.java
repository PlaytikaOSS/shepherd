package com.playtika.shepherd.internal;

import com.playtika.shepherd.inernal.Assignment;
import com.playtika.shepherd.inernal.assignor.Assignor;
import com.playtika.shepherd.inernal.assignor.CooperativeStickyAssignor;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.playtika.shepherd.inernal.ProtocolHelper.compress;
import static com.playtika.shepherd.inernal.ProtocolHelper.decompress;
import static com.playtika.shepherd.inernal.ProtocolHelper.deserializeAssignment;
import static com.playtika.shepherd.inernal.ProtocolHelper.serializeAssignment;
import static com.playtika.shepherd.inernal.utils.BytesUtils.toBuffs;
import static com.playtika.shepherd.inernal.utils.BytesUtils.toBytes;
import static org.assertj.core.api.Assertions.assertThat;

public class CooperativeStickyAssignorTest {

    private final Assignor assignor = new CooperativeStickyAssignor();

    @Test
    public void shouldKeepOriginalOrder(){
        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{2},
                new byte[]{1},
                new byte[]{3}));
        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                buffs, 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("member1"),
                        new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("member2")));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(0), buffs.get(2));

        assertThat(deserializeAssignment(decompress(assignments.get("member2"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(1));
    }

    @Test
    public void shouldAssignAll(){

        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{2},
                new byte[]{1},
                new byte[]{3}));

        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                buffs, 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("member1")));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).assigned())
                .containsExactlyInAnyOrderElementsOf(buffs);
    }

    @Test
    public void shouldKeepSticky(){
        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{1},
                new byte[]{2},
                new byte[]{3},
                new byte[]{4},
                new byte[]{5},
                new byte[]{6}));


        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                buffs, 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member1")
                                .setMetadata(toBytes(compress(serializeAssignment(
                                        new Assignment("", -1, List.of(buffs.get(0), buffs.get(1), buffs.get(2)))
                                )))),
                        new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member2")
                                .setMetadata(toBytes(compress(serializeAssignment(
                                        new Assignment("", -1, List.of(buffs.get(3), buffs.get(4), buffs.get(5)))
                                )))),
                        new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member3")));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(0), buffs.get(1));

        assertThat(deserializeAssignment(decompress(assignments.get("member2"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(3), buffs.get(4));

        assertThat(deserializeAssignment(decompress(assignments.get("member3"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(2), buffs.get(5));
    }

    @Test
    public void shouldSelectWithMinSimilarity(){
        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{1, 0},
                new byte[]{2},
                new byte[]{3, 0},
                new byte[]{4},
                new byte[]{1, 1},
                new byte[]{3, 1}));


        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                buffs, 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member1")
                                .setMetadata(toBytes(compress(serializeAssignment(
                                        new Assignment("", -1, List.of(buffs.get(0), buffs.get(1)))
                                )))),
                        new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member2")
                                .setMetadata(toBytes(compress(serializeAssignment(
                                        new Assignment("", -1, List.of(buffs.get(2), buffs.get(3)))
                                ))))));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(0), buffs.get(1), buffs.get(5));

        assertThat(deserializeAssignment(decompress(assignments.get("member2"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(2), buffs.get(3), buffs.get(4));
    }

    @Test
    public void shouldDistributeWithMinSimilarity(){
        List<ByteBuffer> buffs = toBuffs(List.of(
                new byte[]{1, 0},  //0
                new byte[]{2, 0},  //1
                new byte[]{3, 0},  //2
                new byte[]{1, 1},  //3
                new byte[]{2, 1},  //4
                new byte[]{3, 1},  //5
                new byte[]{1, 2},  //6
                new byte[]{2, 2},  //7
                new byte[]{3, 2}));//8


        Map<String, ByteBuffer> assignments = assignor.performAssignment("leader-test", "",
                buffs, 1,
                List.of(new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member1"),
                        new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member2"),
                        new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId("member3")));

        assertThat(deserializeAssignment(decompress(assignments.get("member1"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(0), buffs.get(4), buffs.get(5));

        assertThat(deserializeAssignment(decompress(assignments.get("member2"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(1), buffs.get(3), buffs.get(8));

        assertThat(deserializeAssignment(decompress(assignments.get("member3"))).assigned())
                .containsExactlyInAnyOrder(buffs.get(2), buffs.get(6), buffs.get(7));
    }
}
