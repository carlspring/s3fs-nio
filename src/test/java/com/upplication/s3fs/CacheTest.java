package com.upplication.s3fs;

import com.upplication.s3fs.util.Cache;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class CacheTest {

    @Test
    public void cacheIsInclusive() {
        Cache cache = spy(new Cache());
        doReturn(300L).when(cache).getCurrentTime();

        S3FileAttributes attributes = new S3FileAttributes("key", null, 0, false, true);
        attributes.setCacheCreated(0);

        boolean result = cache.isInTime(300, attributes);
        assertTrue(result);
    }

    @Test
    public void outOfTime() {
        Cache cache = spy(new Cache());
        doReturn(200L).when(cache).getCurrentTime();

        S3FileAttributes attributes = new S3FileAttributes("key", null, 0, false, true);
        attributes.setCacheCreated(0);

        boolean result = cache.isInTime(100, attributes);
        assertFalse(result);
    }

    @Test
    public void infinite() {
        Cache cache = spy(new Cache());
        doReturn(200L).when(cache).getCurrentTime();

        S3FileAttributes attributes = new S3FileAttributes("key", null, 0, false, true);
        attributes.setCacheCreated(100);

        boolean result = cache.isInTime(-1, attributes);
        assertTrue(result);
    }
}
