package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.Sets;
import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.cache.S3FileAttributesCache;
import org.carlspring.cloud.storage.s3fs.util.MockBucket;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.carlspring.cloud.storage.s3fs.util.FileAttributeBuilder.build;
import static org.junit.jupiter.api.Assertions.*;

class ReadAttributesTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    void readAttributesFileEmpty()
            throws IOException
    {
        // fixtures
        final String content = "";

        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes());

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertFalse(fileAttributes.isDirectory());
        assertTrue(fileAttributes.isRegularFile());
        assertFalse(fileAttributes.isSymbolicLink());
        assertFalse(fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
    }

    @Test
    void readAttributesFile()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        MockBucket mockBucket = client.bucket("bucketA").dir("dir");

        final String content = "sample";
        Path memoryFile = Files.write(mockBucket.resolve("dir/file"), content.getBytes());

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryFile, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(file, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertFalse(fileAttributes.isDirectory());
        assertTrue(fileAttributes.isRegularFile());
        assertFalse(fileAttributes.isSymbolicLink());
        assertFalse(fileAttributes.isOther());
        assertEquals(content.getBytes().length, fileAttributes.size());
        assertEquals("dir/file", fileAttributes.fileKey());
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.lastModifiedTime());
        // TODO: creation and access are the same as last modified time
        assertEquals(fileAttributes.lastModifiedTime(), fileAttributes.creationTime());
        assertEquals(fileAttributes.lastModifiedTime(), fileAttributes.lastAccessTime());
    }

    @Test
    void readAttributesDirectory()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        MockBucket mocket = client.bucket("bucketA").dir("dir");
        Path memoryDir = mocket.resolve("dir/");

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertTrue(fileAttributes.isDirectory());
        assertFalse(fileAttributes.isRegularFile());
        assertFalse(fileAttributes.isSymbolicLink());
        assertFalse(fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
        assertEquals("dir/", fileAttributes.fileKey());
        assertEquals(expectedAttributes.lastModifiedTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.creationTime().to(TimeUnit.SECONDS),
                     fileAttributes.creationTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.lastAccessTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
        // TODO: creation and access are the same that last modified time
        assertEquals(fileAttributes.creationTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(fileAttributes.lastAccessTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
    }

    @Test
    void readAttributesDirectoryNotExistsAtAmazon()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        MockBucket mockBucket = client.bucket("bucketA").dir("dir", "dir/dir2");
        Path memoryDir = mockBucket.resolve("dir/dir2/");

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertTrue(fileAttributes.isDirectory());
        assertFalse(fileAttributes.isRegularFile());
        assertFalse(fileAttributes.isSymbolicLink());
        assertFalse(fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
        assertEquals("dir/", fileAttributes.fileKey());
        assertEquals(expectedAttributes.lastModifiedTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.creationTime().to(TimeUnit.SECONDS),
                     fileAttributes.creationTime().to(TimeUnit.SECONDS));
        assertEquals(expectedAttributes.lastAccessTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
        // TODO: creation and access are the same that last modified time
        assertEquals(fileAttributes.creationTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
        assertEquals(fileAttributes.lastAccessTime().to(TimeUnit.SECONDS),
                     fileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
    }

    @Test
    void readAttributesRegenerateCacheWhenNotExistsBasic()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file-basic", "".getBytes());

        S3FileSystem fs = createNewS3FileSystem();

        // No cache assertion
        S3FileAttributesCache cache = fs.getFileAttributesCache();
        CacheStats stats = cache.stats(); // temporary snapshot
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Pre-requisites (cache entry key should not exist)
        S3Path file1 = fs.getPath("/bucketA/dir/file-basic");
        String fileAttrCacheKey = cache.generateCacheKey(file1, BasicFileAttributes.class);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Reading the attributes should create the cache entry.
        BasicFileAttributes attrs = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file1.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file1.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(3);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file1.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(5);
        assertThat(stats.missCount()).isEqualTo(1);

        // Invalidate cache manually.
        cache.invalidate(fileAttrCacheKey);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();

        // Should populate the cache again.
        attrs = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertNotNull(file1.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(6);
        assertThat(stats.missCount()).isEqualTo(2);
    }

    @Test
    void readAttributesRegenerateCacheWhenNotExistsPosix()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file-posix", "".getBytes());

        S3FileSystem fs = createNewS3FileSystem();

        // No cache assertion
        S3FileAttributesCache cache = fs.getFileAttributesCache();
        CacheStats stats = cache.stats(); // temporary snapshot
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Pre-requisites (cache entry key should not exist)
        S3Path file1 = fs.getPath("/bucketA/dir/file-posix");
        String fileAttrCacheKey = cache.generateCacheKey(file1, PosixFileAttributes.class);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Reading the attributes should create the cache entry.
        PosixFileAttributes attrs = s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file1.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file1.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(3);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file1.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(5);
        assertThat(stats.missCount()).isEqualTo(1);

        // Invalidate cache manually.
        cache.invalidate(fileAttrCacheKey);
        assertThat(cache.contains(fileAttrCacheKey)).isFalse();

        // Should populate the cache again.
        attrs = s3fsProvider.readAttributes(file1, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertNotNull(file1.getFileAttributes(PosixFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(6);
        assertThat(stats.missCount()).isEqualTo(2);

    }

    @Test
    void readAttributesCastDownFromPosixToBasic()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file-posix2", "".getBytes());

        S3FileSystem fs = createNewS3FileSystem();

        // No cache assertion
        S3FileAttributesCache cache = fs.getFileAttributesCache();
        CacheStats stats = cache.stats(); // temporary snapshot
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Pre-requisites (cache entry key should not exist)
        S3Path file = fs.getPath("/bucketA/dir/file-posix2");
        String basicFileAttrCacheKey = cache.generateCacheKey(file, BasicFileAttributes.class);
        String posixFileAttrCacheKey = cache.generateCacheKey(file, PosixFileAttributes.class);
        assertThat(cache.contains(basicFileAttrCacheKey)).isFalse();
        assertThat(cache.contains(posixFileAttrCacheKey)).isFalse();

        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(0);
        assertThat(stats.missCount()).isEqualTo(0);

        // Reading the attributes should create the cache entry.
        BasicFileAttributes attrs = s3fsProvider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = s3fsProvider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(3);
        assertThat(stats.missCount()).isEqualTo(1);

        // Should hit the cache.
        attrs = s3fsProvider.readAttributes(file, BasicFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertThat(attrs).isEqualTo(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(5);
        assertThat(stats.missCount()).isEqualTo(1);

        // Invalidate cache manually.
        cache.invalidate(basicFileAttrCacheKey);
        assertThat(cache.contains(basicFileAttrCacheKey)).isFalse();

        // Should populate the cache again.
        attrs = s3fsProvider.readAttributes(file, PosixFileAttributes.class);
        assertThat(attrs).isNotNull();
        assertNotNull(file.getFileAttributes(BasicFileAttributes.class));
        stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(6);
        assertThat(stats.missCount()).isEqualTo(2);
    }

    @Test
    void readAttributesFileNotExists()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NoSuchFileException.class, () -> {
            // fixtures
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            client.bucket("bucketA").dir("dir");

            FileSystem fs = createNewS3FileSystem();
            Path file1 = fs.getPath("/bucketA/dir/file1");

            s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        });

        assertNotNull(exception);
    }

    @Test
    void readAttributesFileNotExistsButExistsAnotherThatContainsTheKey()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NoSuchFileException.class, () -> {
            // fixtures
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            client.bucket("bucketA").dir("dir").file("dir/file1hello", "content".getBytes());

            FileSystem fs = createNewS3FileSystem();
            Path file1 = fs.getPath("/bucketA/dir/file1");

            s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
        });

        assertNotNull(exception);
    }

    @Test
    void readAttributesNotAcceptedSubclass()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            // fixtures
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            client.bucket("bucketA").dir("dir");

            FileSystem fs = createNewS3FileSystem();
            Path dir = fs.getPath("/bucketA/dir");

            s3fsProvider.readAttributes(dir, DosFileAttributes.class);
        });

        assertNotNull(exception);
    }

    // readAttributes permission PosixFileAttributes.class

    @Test
    void readPosixPermissionOwnerWriteAttributes()
            throws IOException
    {
        // fixtures
        S3MockFactory.getS3ClientMock()
                     .bucket("bucketA")
                     .file("file",
                                 "content".getBytes(),
                                 build("posix:permissions",
                                       Sets.newHashSet(PosixFilePermission.OWNER_READ,
                                                       PosixFilePermission.OWNER_WRITE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_WRITE));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
    }

    @Test
    void readPosixPermissionOwnerExecuteAttributes()
            throws IOException
    {
        // fixtures
        S3MockFactory.getS3ClientMock()
                     .bucket("bucketA")
                     .file("file",
                                 "content".getBytes(),
                                 build("posix:permissions",
                                       Sets.newHashSet(PosixFilePermission.OWNER_READ,
                                                       PosixFilePermission.OWNER_EXECUTE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
    }

    @Test
    void readPosixPermissionGroupWriteAttributes()
            throws IOException
    {
        // fixtures
        S3MockFactory.getS3ClientMock()
                     .bucket("bucketA")
                     .file("file",
                                 "content".getBytes(),
                                 build("posix:permissions",
                                       Sets.newHashSet(PosixFilePermission.OWNER_READ,
                                                       PosixFilePermission.GROUP_WRITE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_WRITE));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
    }

    @Test
    void readPosixPermissionGroupExecuteAttributes()
            throws IOException
    {
        // fixtures
        S3MockFactory.getS3ClientMock()
                     .bucket("bucketA")
                     .file("file",
                                 "content".getBytes(),
                                 build("posix:permissions",
                                       Sets.newHashSet(PosixFilePermission.OWNER_READ,
                                                       PosixFilePermission.GROUP_EXECUTE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
    }

    @Test
    void readPosixPermissionOtherWriteAttributes()
            throws IOException
    {
        // fixtures
        S3MockFactory.getS3ClientMock()
                     .bucket("bucketA")
                     .file("file",
                                 "content".getBytes(),
                                 build("posix:permissions",
                                       Sets.newHashSet(PosixFilePermission.OWNER_READ,
                                                       PosixFilePermission.OTHERS_WRITE)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_READ));
        assertTrue(fileAttributes.permissions().contains(PosixFilePermission.OWNER_WRITE));
    }

    // readAttributes owner and group PosixFileAttributes

    @Test
    void readPosixPermissionOwnerAndGroupAttributes()
            throws IOException
    {
        // fixtures
        S3MockFactory.getS3ClientMock()
                     .bucket("bucketA")
                     .file("file",
                                 "content".getBytes(),
                                 build("posix:permissions",
                                       Sets.newHashSet(PosixFilePermission.OWNER_READ)));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/file");

        PosixFileAttributes fileAttributes = s3fsProvider.readAttributes(file, PosixFileAttributes.class);

        assertNotNull(fileAttributes.owner());
        assertNull(fileAttributes.group());
    }

    // readAttributes String all

    @Test
    void readAttributesAll()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();

        final String content = "sample";

        Path memoryFile = Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), content.getBytes());

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryFile, BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "*");

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.get("isDirectory"));
        assertEquals(true, fileAttributes.get("isRegularFile"));
        assertEquals(false, fileAttributes.get("isSymbolicLink"));
        assertEquals(false, fileAttributes.get("isOther"));
        assertEquals((long) content.getBytes().length, fileAttributes.get("size"));
        assertEquals("dir/file", fileAttributes.get("fileKey"));
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.get("lastModifiedTime"));
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.get("creationTime"));
        assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.get("lastAccessTime"));
    }

    @Test
    void readAttributesAllBasic()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();

        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "basic:*");
        Map<String, Object> fileAttributes2 = s3fsProvider.readAttributes(file, "*");

        assertArrayEquals(fileAttributes.values().toArray(new Object[]{}),
                          fileAttributes2.values().toArray(new Object[]{}));
        assertArrayEquals(fileAttributes.keySet().toArray(new String[]{}),
                          fileAttributes2.keySet().toArray(new String[]{}));
    }

    @Test
    void readAttributesOnlyOne()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "isDirectory");

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.get("isDirectory"));
        assertEquals(1, fileAttributes.size());
    }

    @Test
    void readAttributesPartial()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "isDirectory,isRegularFile");

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.get("isDirectory"));
        assertEquals(true, fileAttributes.get("isRegularFile"));
        assertEquals(2, fileAttributes.size());
    }

    @Test
    void readAttributesPartialBasic()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "basic:isOther,basic:creationTime");
        Map<String, Object> fileAttributes2 = s3fsProvider.readAttributes(file, "isOther,creationTime");

        assertArrayEquals(fileAttributes.values().toArray(new Object[]{}),
                          fileAttributes2.values().toArray(new Object[]{}));
        assertArrayEquals(fileAttributes.keySet().toArray(new String[]{}),
                          fileAttributes2.keySet().toArray(new String[]{}));
    }

    @Test
    void readAttributesAllPosix()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "posix:*");

        assertNotNull(fileAttributes.get("permissions"));
    }

    @Test
    void readAttributesPartialPosix()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        Map<String, Object> fileAttributes = s3fsProvider.readAttributes(file, "posix:permissions");

        assertNotNull(fileAttributes.get("permissions"));
    }

    @Test
    void readAttributesNullAttrs()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            // fixtures
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

            FileSystem fs = createNewS3FileSystem();
            Path file = fs.getPath("/bucketA/dir/file");

            s3fsProvider.readAttributes(file, (String) null);
        });

        assertNotNull(exception);
    }

    @Test
    void readAttributesDosNotSupported()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            // fixtures
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

            FileSystem fs = createNewS3FileSystem();
            Path file = fs.getPath("/bucketA/dir/file");

            s3fsProvider.readAttributes(file, "dos:*");
        });

        assertNotNull(exception);
    }

    @Test
    void readAttributesUnknownNotSupported()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            // fixtures
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            Files.write(client.bucket("bucketA").dir("dir").resolve("dir/file"), "sample".getBytes());

            FileSystem fs = createNewS3FileSystem();
            Path file = fs.getPath("/bucketA/dir/file");

            s3fsProvider.readAttributes(file, "lelel:*");
        });

        assertNotNull(exception);
    }

    // setAttribute
    @Test
    void readAttributesObject()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            s3fsProvider.setAttribute(null, "", new Object());
        });

        assertNotNull(exception);
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
     * @throws IOException
     */
    private S3FileSystem createNewS3FileSystem()
            throws IOException
    {
        try
        {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        }
        catch (FileSystemNotFoundException e)
        {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }
    }

}
