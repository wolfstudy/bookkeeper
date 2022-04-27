package org.apache.bookkeeper.bookie.storage.ldb;

import java.nio.ByteBuffer;
import org.caffinitas.ohc.CacheSerializer;

/**
 * convert long pair to bytebuffer as ohc key 
 */
public class LongPairSerializer implements CacheSerializer<LongPairWrapper> {

    @Override
    public void serialize(LongPairWrapper value, ByteBuffer buf) {
        byte[] byteValue = value.array;
        buf.put(byteValue);
    }

    @Override
    public LongPairWrapper deserialize(ByteBuffer buf) {
        long first = buf.getLong();
        long second = buf.getLong();
        return LongPairWrapper.get(first, second);
    }

    @Override
    public int serializedSize(LongPairWrapper value) {
        return 16;
    }
}
