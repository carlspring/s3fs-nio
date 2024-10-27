package org.carlspring.cloud.storage.s3fs.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.carlspring.cloud.storage.s3fs.S3ObjectId;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.attribute.S3PosixFileAttributes;
import org.carlspring.cloud.storage.s3fs.util.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;

public class S3FileAttributesCache
{

    private static final Logger logger = LoggerFactory.getLogger(S3FileAttributesCache.class);

    private final S3Utils s3Utils = new S3Utils();

    // This should be volatile, despite what IntelliJ / Sonar says.
    // When using `Files.exists` / `Files.isDirectory` / etc -- different threads will be entering this class
    // which could cause problems.
    private volatile Cache<String, Optional<S3BasicFileAttributes>> cache;

    /**
     * @param cacheTTL  TTL in milliseconds
     * @param cacheSize Total cache size.
     */
    public S3FileAttributesCache(int cacheTTL, int cacheSize)
    {
        this.cache = cacheBuilder(cacheTTL, cacheSize).build();
    }

    /**
     * Generates a cache key based on S3Path and the attribute class type.
     * The key is a combination of the S3Path's hashCode and the attribute class name.
     *
     * @param path The {@link S3Path}.
     * @param attributeClass The class type of {@link BasicFileAttributes}.
     * @return A unique string key.
     */
    public static String generateCacheKey(S3Path path, Class<? extends BasicFileAttributes> attributeClass)
    {
        S3ObjectId s3ObjectId = path.toS3ObjectId();
        return generateCacheKey(s3ObjectId, attributeClass);
    }

    /**
     * Generates a cache key based on S3Path and the attribute class type.
     * The key is a combination of the S3Path's hashCode and the attribute class name.
     *
     * @param s3ObjectId An {@link software.amazon.awssdk.services.s3.model.S3Object} instance.
     * @param attributeClass The class type of {@link BasicFileAttributes}.
     * @return A unique string key.
     */
    public static String generateCacheKey(S3ObjectId s3ObjectId, Class<? extends BasicFileAttributes> attributeClass)
    {
        StringBuilder key = new StringBuilder();
        key.append(s3ObjectId.getBucket().replaceAll("/", "%2F"))
           .append("_")
           .append(s3ObjectId.getKey().replaceAll("/", "%2F"))
           .append("_");

        if (attributeClass == BasicFileAttributes.class) {
            key.append(S3BasicFileAttributes.class.getSimpleName());
        } else if (attributeClass == PosixFileAttributes.class) {
            key.append(S3PosixFileAttributes.class.getSimpleName());
        } else {
            key.append(attributeClass.getSimpleName());
        }

        return key.toString();
    }


    /**
     * Retrieves the file attributes of the given S3Path (either BasicFileAttributes or PosixFileAttributes)
     *
     * @param path The {@link S3Path}
     *
     * @return The {@link S3BasicFileAttributes} or {@link S3PosixFileAttributes} for the given {@link S3Path}. Is null
     * when `attrType` is not {@link BasicFileAttributes}, {@link PosixFileAttributes} or the path does not exist.
     *
     * @throws CompletionException if a checked exception was thrown while loading the value from AWS.
     */
    public S3BasicFileAttributes get(final S3Path path, final Class<? extends BasicFileAttributes> attrType)
    {
        String key = generateCacheKey(path, attrType);
        logger.trace("Get cache for key {}", key);

        Optional<S3BasicFileAttributes> attrs = cache.getIfPresent(key);

        // Don't get confused - Caffeine returns `null` if the key does not exist.
        if(attrs == null)
        {
            logger.trace("No cache found for key {}", key);
            // We need a way to preserve non-existing files/paths.
            // This is necessary, because the Files.exist() method is called multiple times from different threads
            // during checks. As a result multiple requests for the same path are executed within milliseconds.
            logger.trace("Fetch data for key {}", key);
            attrs = Optional.ofNullable(fetchAttribute(path, key));
            put(path, attrs);
        }

        return attrs.orElse(null);
    }

    public boolean contains(final S3Path path, final Class<? extends BasicFileAttributes> attrType)
    {
        String key = generateCacheKey(path, attrType);
        return contains(key);
    }

    public boolean contains(final String key)
    {
        return cache.asMap().containsKey(key);
    }


    /**
     * @param path  The S3 path.
     * @param attrs the file attributes to store in the cache. Can be the posix ones
     */
    public void put(final S3Path path, final S3BasicFileAttributes attrs)
    {
        put(path, Optional.ofNullable(attrs));
    }

    /**
     * @param path  The S3 path.
     * @param attrs the file attributes to store in the cache. Can be the posix ones
     */
    public void put(final S3Path path, final Optional<S3BasicFileAttributes> attrs)
    {
        // There is an off-chance we could have both BasicFileAttributes and PosixFileAttributes cached at different times.
        // This could cause a temporary situation where the cache serves slightly outdated instance of BasicFileAttributes.
        // To ensure this does not happen we always need to replace the BasicFileAttributes instances when
        // the PosixFileAttributes type is cached/updated.
        String basicKey = generateCacheKey(path, BasicFileAttributes.class);
        logger.trace("Save response for key {}", basicKey);
        cache.put(basicKey, attrs);

        if(attrs.isPresent() && attrs.get() instanceof PosixFileAttributes)
        {
            String posixKey = generateCacheKey(path, PosixFileAttributes.class);
            logger.trace("Save response for key {}", posixKey);
            cache.put(posixKey, attrs);
        }
    }

    /**
     * Invalidates the file attributes in the cache for the given s3Path
     *
     * @param path The S3 path.
     */
    public void invalidate(final S3Path path, final Class<? extends BasicFileAttributes> attrType)
    {
        String key = generateCacheKey(path, attrType);
        logger.trace("Invalidate cache key {}", key);
        cache.invalidate(key);
    }

    /**
     * Invalidates the file attributes in the cache for the given s3Path
     *
     * @param key The cache key
     */
    public void invalidate(final String key)
    {
        cache.invalidate(key);
    }

    public void invalidate(final S3Path path)
    {
        invalidate(path.toS3ObjectId());
    }

    public void invalidate(final S3ObjectId objectId)
    {
        List<String> keys = new ArrayList<>();

        keys.add(generateCacheKey(objectId, BasicFileAttributes.class));
        keys.add(generateCacheKey(objectId, PosixFileAttributes.class));

        /**
         * This handles an edge case - depending on where the code is triggered from, there might be a fallback check
         * that attempts to resolve a file OR virtual directory. We need to invalidate the cache for both when
         * this method is called.
         */
        if (objectId.getKey().endsWith("/"))
        {
            S3ObjectId fileObjectId = S3ObjectId.builder()
                                                .bucket(objectId.getBucket())
                                                .key(objectId.getKey().substring(0, objectId.getKey().length() - 1))
                                                .build();

            keys.add(generateCacheKey(fileObjectId, BasicFileAttributes.class));
            keys.add(generateCacheKey(fileObjectId, PosixFileAttributes.class));
        }
        else
        {
            S3ObjectId fileObjectId = S3ObjectId.builder()
                                                .bucket(objectId.getBucket())
                                                .key(objectId.getKey() + S3Path.PATH_SEPARATOR)
                                                .build();

            keys.add(generateCacheKey(fileObjectId, BasicFileAttributes.class));
            keys.add(generateCacheKey(fileObjectId, PosixFileAttributes.class));
        }


        for (String key : keys)
        {
            try
            {
                logger.trace("Invalidate cache key {}", key);
                cache.invalidate(key);
            }
            catch (NullPointerException e)
            {
                // noop
            }
        }

    }


    public void invalidateAll()
    {
        logger.trace("Invalidate all cache");
        cache.invalidateAll();
    }

    public CacheStats stats()
    {
        return cache.stats();
    }

    protected Caffeine<String, Optional<S3BasicFileAttributes>> cacheBuilder(int cacheTTL, int cacheSize)
    {
        Caffeine<String, Optional<S3BasicFileAttributes>> builder = Caffeine.newBuilder()
                                                                            .expireAfter(new S3FileAttributesCachePolicy(cacheTTL));

        builder.maximumSize(cacheSize);
        builder.recordStats();
        builder.evictionListener((String key, Optional<S3BasicFileAttributes> value, RemovalCause cause) ->
                logger.trace("Key {} was evicted (reason: {})", key, cause));
        builder.removalListener((String key, Optional<S3BasicFileAttributes> value, RemovalCause cause) ->
                logger.trace("Key {} was removed (reason: {})", key, cause));

        return builder;
    }

    protected S3BasicFileAttributes fetchAttribute(S3Path path, String key)
    {
        try
        {
            if (key.contains(BasicFileAttributes.class.getSimpleName()))
            {
                return s3Utils.getS3FileAttributes(path);
            }
            else if (key.contains(PosixFileAttributes.class.getSimpleName()))
            {
                return s3Utils.getS3PosixFileAttributes(path);
            }
            return null;
        }
        catch (NoSuchFileException e)
        {
            return null;
        }
    }


}
