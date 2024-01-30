package com.playtika.shepherd.inernal;

import net.jpountz.lz4.LZ4CompressorWithLength;
import net.jpountz.lz4.LZ4DecompressorWithLength;
import net.jpountz.lz4.LZ4Factory;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static net.jpountz.lz4.LZ4DecompressorWithLength.getDecompressedLength;
import static org.apache.kafka.common.protocol.types.Type.BYTES;

public class ProtocolHelper {

    public static final String LEADER_KEY_NAME = "leader";
    public static final String VERSION_KEY_NAME = "version";
    public static final String ASSIGNED_KEY_NAME = "assigned";

    public static final LZ4Factory lz4Factory = LZ4Factory.fastestJavaInstance();

    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(VERSION_KEY_NAME, Type.INT64),
            new Field(ASSIGNED_KEY_NAME, new ArrayOf(BYTES)));
    public static final int COMPRESSION_LEVEL = 17;

    public static ByteBuffer compress(ByteBuffer buffer) {
        LZ4CompressorWithLength compressor = new LZ4CompressorWithLength(lz4Factory.highCompressor(COMPRESSION_LEVEL));
        ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedLength(buffer.remaining()));
        compressor.compress(buffer, compressed);
        compressed.flip();
        return compressed;
    }

    public static ByteBuffer decompress(ByteBuffer buffer) {
        ByteBuffer decompressed = ByteBuffer.allocate(getDecompressedLength(buffer));
        LZ4DecompressorWithLength decompressor = new LZ4DecompressorWithLength(lz4Factory.safeDecompressor());
        decompressor.decompress(buffer, decompressed);
        decompressed.flip();
        return decompressed;
    }

    public static ByteBuffer serializeAssignment(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V0);
        struct.set(LEADER_KEY_NAME, assignment.leader());
        struct.set(VERSION_KEY_NAME, assignment.version());
        struct.set(ASSIGNED_KEY_NAME, assignment.assigned().toArray());

        ByteBuffer buffer = ByteBuffer.allocate(ASSIGNMENT_V0.sizeOf(struct));
        ASSIGNMENT_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct struct = ASSIGNMENT_V0.read(buffer);
        String leader = struct.getString(LEADER_KEY_NAME);
        long version = struct.getLong(VERSION_KEY_NAME);
        List<ByteBuffer> assigned = new ArrayList<>();
        for (Object element : struct.getArray(ASSIGNED_KEY_NAME)) {
            assigned.add((ByteBuffer) element);
        }
        return new Assignment(leader, version, assigned);
    }

}
