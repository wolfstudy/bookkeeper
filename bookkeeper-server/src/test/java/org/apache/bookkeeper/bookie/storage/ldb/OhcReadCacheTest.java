package org.apache.bookkeeper.bookie.storage.ldb;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.junit.Assert;
import org.junit.Test;


public class OhcReadCacheTest {

    @Test
    public void testOhcBase() {

        OHCache<String, String> ohCache = OHCacheBuilder.<String, String>newBuilder()
                .keySerializer(new StringSerializer())
                .valueSerializer(new StringSerializer())
                .eviction(Eviction.LRU)
                .build();

        ohCache.put("hello", "world");
        System.out.println(ohCache.get("hello")); // world
        assertEquals(ohCache.get("hello"), "world");
    }

    @Test
    public void testLongPairSerializer() {

        OHCache<LongPairWrapper, ByteBuf> ohCache = OHCacheBuilder.<LongPairWrapper, ByteBuf>newBuilder()
                .keySerializer(new LongPairSerializer())
                .valueSerializer(new ByteBufSerializer())
                .eviction(Eviction.LRU)
                .build();

        for (int i = 1; i < 10000; i++) {
            long first = ThreadLocalRandom.current().nextLong();
            long second = ThreadLocalRandom.current().nextLong();

            LongPairWrapper key = LongPairWrapper.get(first, second);
            LongPairWrapper value = LongPairWrapper.get(first * first, second * second);

            ByteBuf putValueBuf = ByteBufAllocator.DEFAULT.buffer(16);
            putValueBuf.writeBytes(value.array);
            assertEquals(putValueBuf.readableBytes(), 16);
            ohCache.put(key, putValueBuf);

            assertEquals(i, ohCache.size());

            putValueBuf.release();

            ByteBuf getValueBuf = ohCache.get(key);

            assertEquals(getValueBuf.readableBytes(), 16);

            long getFirst = getValueBuf.getLong(0);
            long getSecond = getValueBuf.getLong(8);
//            System.out.println(String.format("get value first: %d second: %d", getFirst, getSecond));
            assertEquals("first long check ", getFirst, first * first);
            assertEquals("second long check", getSecond, second * second);
            getValueBuf.release();
        }
    }


    @Test
    public void testByteBufSerializer() {

        OHCache<LongPairWrapper, String> ohCache = OHCacheBuilder.<LongPairWrapper, String>newBuilder()
                .keySerializer(new LongPairSerializer())
                .valueSerializer(new StringSerializer())
                .eviction(Eviction.LRU)
                .build();

        for (int i = 0; i < 10000; i++) {
            long first = ThreadLocalRandom.current().nextLong();
            long second = ThreadLocalRandom.current().nextLong();
            LongPairWrapper key = LongPairWrapper.get(first, second);
            ohCache.put(key, String.valueOf(first * second));

            String value = ohCache.get(key);
            assertEquals(value, String.valueOf(first * second));
        }
    }

    @Test
    public void testByteBuf() {
        String str = "hello";
        ByteBuf testBuf = ByteBufAllocator.DEFAULT.buffer(16);
        testBuf.writeBytes(str.getBytes());
        assertEquals(testBuf.readableBytes(), 5);
        byte[] dest = new byte[str.length()];
        testBuf.readBytes(dest, 0, dest.length);
        assertEquals(new String(dest), str);

        testBuf.release();

        LongPairWrapper longPairWrapper = LongPairWrapper.get(3, 6);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(16);

        assertEquals(byteBuffer.position(), 0);
        byteBuffer.putLong(longPairWrapper.getFirst());
        byteBuffer.putLong(longPairWrapper.getSecond());

        assertEquals(byteBuffer.position(), 16);
        assertEquals(byteBuffer.remaining(), 0);

        byteBuffer.flip();

        assertEquals(byteBuffer.position(), 0);
        assertEquals(byteBuffer.remaining(), 16);

        testBuf = ByteBufAllocator.DEFAULT.buffer(16);
        testBuf.writeBytes(byteBuffer);

        assertEquals(testBuf.readableBytes(), 16);


    }

    @Test
    public void simple() throws IOException {
        OhcReadCache cache = new OhcReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
        cache.put(1, 0, entry);

        assertEquals(1, cache.count());
        assertTrue(1024 < cache.size());

        assertEquals(entry, cache.get(1, 0));
        assertNull(cache.get(1, 1));

        for (int i = 1; i < 10; i++) {
            cache.put(1, i, entry);
        }

        assertEquals(10, cache.count());
        assertTrue(10 * 1024 < cache.size());

        cache.put(1, 10, entry);

        for (int i = 0; i < 5; i++) {
            assertNotNull(cache.get(1, i));
        }

        for (int i = 5; i < 11; i++) {
            assertEquals(entry, cache.get(1, i));
        }

        cache.close();
    }

    @Test
    public void emptyCache() throws IOException {
        OhcReadCache cache = new OhcReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        assertEquals(null, cache.get(0, 0));

        cache.close();
    }


    @Test
    public void testToString() {

        OhcReadCache cache = new OhcReadCache(PooledByteBufAllocator.DEFAULT, 10 * 1024 * 1024);

        cache.printStatsLog();
        for (long i = 0; i < 10000; i++) {
            LongPairWrapper key = LongPairWrapper.get(i, i * i);

            if (i % 100 != 0) {
                ByteBuf value = Unpooled.wrappedBuffer(("hello" + i).getBytes());
                cache.put(i, i, value);
            }
        }
        cache.printStatsLog();
        System.out.println(cache);

        for (long i = 0; i < 10000; i++) {

            ByteBuf value1 = Unpooled.wrappedBuffer(("hello" + i).getBytes());
            ByteBuf value2 = cache.get(i, i);

            if (i % 100 != 0) {
                Assert.assertEquals(value1, value2);
            } else {
                Assert.assertNull(value2);
            }

        }
        cache.printStatsLog();
        System.out.println(cache);


    }


}
