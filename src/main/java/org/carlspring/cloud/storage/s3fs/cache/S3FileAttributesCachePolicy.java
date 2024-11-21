package org.carlspring.cloud.storage.s3fs.cache;

import com.github.benmanes.caffeine.cache.Expiry;
import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class S3FileAttributesCachePolicy implements Expiry<String, Optional<S3BasicFileAttributes>>
{

    private int cacheTTL;

    public S3FileAttributesCachePolicy(int cacheTTL)
    {
        this.cacheTTL = cacheTTL;
    }

    public int getTTL()
    {
        return cacheTTL;
    }

    public void setTTL(int cacheTTL)
    {
        this.cacheTTL = cacheTTL;
    }

    @Override
    public long expireAfterCreate(String key, Optional<S3BasicFileAttributes> value, long currentTime)
    {
        // Set initial TTL upon creation
        return TimeUnit.MILLISECONDS.toNanos(cacheTTL);
    }

    @Override
    public long expireAfterUpdate(String key, Optional<S3BasicFileAttributes> value, long currentTime, long currentDuration)
    {
        // Reset TTL on update
        return TimeUnit.MILLISECONDS.toNanos(cacheTTL);
    }

    @Override
    public long expireAfterRead(String key, Optional<S3BasicFileAttributes> value, long currentTime, long currentDuration)
    {
        // Use already assigned TTL.
        return currentDuration;
    }
}
