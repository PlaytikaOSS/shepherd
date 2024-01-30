package com.playtika.shepherd.inernal;

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface Assignor {
    Map<String, ByteBuffer> performAssignment(
            String leaderId, String protocol, List<ByteBuffer> population, long version,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);
}
