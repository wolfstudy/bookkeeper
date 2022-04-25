package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.caffinitas.ohc.CacheSerializer;

/**
 * convert data between netty bytebuf and ohc direct bytebuffer 
 */
public class ByteBufSerializer implements CacheSerializer<ByteBuf> {
    
    @Override
    public void serialize(ByteBuf value, ByteBuffer byteBuffer) {
        byteBuffer.put(value.nioBuffer());
    }

    @Override
    public ByteBuf deserialize(ByteBuffer byteBuffer) {
        return Unpooled.wrappedBuffer(byteBuffer);
    }

    @Override
    public int serializedSize(ByteBuf value) {
        return value.readableBytes();
    }
}
