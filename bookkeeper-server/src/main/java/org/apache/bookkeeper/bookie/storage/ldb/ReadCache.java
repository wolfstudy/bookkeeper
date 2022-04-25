package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;

public interface ReadCache extends Closeable {


    void put(long ledgerId, long entryId, ByteBuf entry);

    ByteBuf get(long ledgerId, long entryId);

    long size();

    long count();
}
