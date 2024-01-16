package com.playtika.shepherd.inernal.utils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public class BytesUtils {

    public static List<byte[]> toBytes(Collection<ByteBuffer> bufs){
        return bufs.stream().map(BytesUtils::toBytes).toList();
    }

    public static byte[] toBytes(ByteBuffer buf){
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        buf.rewind();
        return arr;
    }

    public static List<ByteBuffer> toBuffs(Collection<byte[]> bites){
        return bites.stream().map(ByteBuffer::wrap).toList();
    }
}
