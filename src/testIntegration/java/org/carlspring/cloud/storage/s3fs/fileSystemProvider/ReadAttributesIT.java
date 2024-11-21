package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.carlspring.cloud.storage.s3fs.BaseIntegrationTest;
import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.cache.S3FileAttributesCache;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ReadAttributesIT
        extends BaseIntegrationTest
{

    private static final String bucket = EnvironmentBuilder.getBucket();

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private S3FileSystem fileSystemAmazon;

    private S3FileSystemProvider provider;

    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);

        fileSystemAmazon = (S3FileSystem) build();
        provider = fileSystemAmazon.provider();
    }

    private static FileSystem build()
            throws IOException
    {
        try
        {
            FileSystems.getFileSystem(uriGlobal).close();

            return createNewFileSystem();
        }
        catch (FileSystemNotFoundException e)
        {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem()
            throws IOException
    {
        return FileSystems.newFileSystem(uriGlobal, EnvironmentBuilder.getRealEnv());
    }

    private Path uploadSingleFile(String content)
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            if (content != null)
            {
                Files.write(linux.getPath("/index.html"), content.getBytes());
            }
            else
            {
                Files.createFile(linux.getPath("/index.html"));
            }

            Path result = fileSystemAmazon.getPath(bucket, getTestBasePathWithUUID());

            Files.copy(linux.getPath("/index.html"), result);

            return result;
        }
    }

    @Test
    void readAttributesRegenerateCacheWhenNotExistsBasic()
            throws IOException
    {
        S3FileSystem fs = fileSystemAmazon;
        S3Path file = fileSystemAmazon.getPath(bucket, getTestBasePathWithUUID(), "1234");
        Files.write(file, "1234".getBytes());

        // No cache assertion
        S3FileAttributesCache cache = fs.getFileAttributesCache();
        CacheStats stats = cache.stats(); // temporary snapshot
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Pre-requisites (cache entry key should not exist)
        String fileAttrCacheKey = cache.generateCacheKey(file, BasicFileAttributes.class);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Reading the attributes should create the cache entry.
        BasicFileAttributes attrs = provider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = provider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(3);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = provider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(5);
        assertThat(stats.missCount()).isEqualTo(1);

        // Invalidate cache manually.
        cache.invalidate(fileAttrCacheKey);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();

        // Should populate the cache again.
        attrs = provider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertNotNull(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(6);
        assertThat(stats.missCount()).isEqualTo(2);
    }

    @Test
    void readAttributesRegenerateCacheWhenNotExistsPosix()
            throws IOException
    {
        S3FileSystem fs = fileSystemAmazon;
        S3Path file = fileSystemAmazon.getPath(bucket, getTestBasePathWithUUID(), "1234");
        Files.write(file, "1234".getBytes());

        // No cache assertion
        S3FileAttributesCache cache = fs.getFileAttributesCache();
        CacheStats stats = cache.stats(); // temporary snapshot
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Pre-requisites (cache entry key should not exist)
        String fileAttrCacheKey = cache.generateCacheKey(file, PosixFileAttributes.class);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Reading the attributes should create the cache entry.
        PosixFileAttributes attrs = provider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = provider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(3);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = provider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(5);
        assertThat(stats.missCount()).isEqualTo(1);

        // Invalidate cache manually.
        cache.invalidate(fileAttrCacheKey);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();

        // Should populate the cache again.
        attrs = provider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertNotNull(file.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(6);
        assertThat(stats.missCount()).isEqualTo(2);

    }

    @Test
    void readAttributesCastDownFromPosixToBasic()
            throws IOException
    {
        S3FileSystem fs = fileSystemAmazon;
        S3Path file = fileSystemAmazon.getPath(bucket, getTestBasePathWithUUID(), "1234");
        Files.write(file, "1234".getBytes());

        // No cache assertion
        S3FileAttributesCache cache = fs.getFileAttributesCache();
        CacheStats stats = cache.stats(); // temporary snapshot
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Pre-requisites (cache entry key should not exist)
        String basicFileAttrCacheKey = cache.generateCacheKey(file, BasicFileAttributes.class);
        String posixFileAttrCacheKey = cache.generateCacheKey(file, PosixFileAttributes.class);
        assertThat(cache.contains(basicFileAttrCacheKey)).isFalse();
        assertThat(cache.contains(posixFileAttrCacheKey)).isFalse();

        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Reading the attributes should create the cache entry.
        BasicFileAttributes attrs = provider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = provider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(3);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = provider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(5);
        assertThat(stats.missCount()).isEqualTo(1);

        // Invalidate cache manually.
        cache.invalidate(basicFileAttrCacheKey);
        assertThat(cache.contains(basicFileAttrCacheKey)).isFalse();

        // Should populate the cache again.
        attrs = provider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertNotNull(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(6);
        assertThat(stats.missCount()).isEqualTo(2);
    }

}
