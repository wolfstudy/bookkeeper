package org.apache.bookkeeper.util;

import junit.framework.TestCase;
import org.junit.Test;

public class ThreadNumUtilsTest extends TestCase {

    

    @Test
    public void testAdaptThreadNum() {

        int psize = Runtime.getRuntime().availableProcessors();

        if (psize >= ThreadNumUtils.DEFAULT_MIN_THREAD_NUM && psize <= ThreadNumUtils.DEFAULT_MAX_THREAD_NUM) {
            assertEquals(psize, ThreadNumUtils.adaptThreadNum());
        }
        assertEquals(psize, ThreadNumUtils.adaptThreadNum());
        assertEquals(psize, ThreadNumUtils.adaptThreadNum(1, 1000));
        assertEquals(1, ThreadNumUtils.adaptThreadNum(1, 1));
        assertEquals(6, ThreadNumUtils.adaptThreadNum(4, 8, 6));

        try {
            ThreadNumUtils.adaptThreadNum(0, 1);
            assertNotNull("should not run here");
        } catch (IllegalArgumentException ex) {
            assertNotNull(ex);
        }

    }
}
