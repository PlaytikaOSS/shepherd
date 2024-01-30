package com.playtika.shepherd.inernal;

import java.nio.ByteBuffer;
import java.util.List;

public record Assignment(String leader, long version, List<ByteBuffer> assigned) {
}
