package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.util.Cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class CacheTest
{

    @Test
    void cacheIsInclusive()
    {
        Cache cache = spy(new Cache());

        doReturn(300L).when(cache).getCurrentTime();

        S3BasicFileAttributes attributes = new S3BasicFileAttributes("key", null, 0, false, true);
        attributes.setCacheCreated(0);

        boolean result = cache.isInTime(300, attributes);

        assertTrue(result);
    }

    @Test
    void outOfTime()
    {
        Cache cache = spy(new Cache());

        doReturn(200L).when(cache).getCurrentTime();

        S3BasicFileAttributes attributes = new S3BasicFileAttributes("key", null, 0, false, true);
        attributes.setCacheCreated(0);

        boolean result = cache.isInTime(100, attributes);

        assertFalse(result);
    }

    @Test
    void infinite()
    {
        Cache cache = spy(new Cache());

        doReturn(200L).when(cache).getCurrentTime();

        S3BasicFileAttributes attributes = new S3BasicFileAttributes("key", null, 0, false, true);
        attributes.setCacheCreated(100);

        boolean result = cache.isInTime(-1, attributes);

        assertTrue(result);
    }

}
