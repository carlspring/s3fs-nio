package org.carlspring.cloud.storage.s3fs.path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3PathTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    private S3Path forPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    void createNoPath()
    {
        S3Path path = forPath("/bucket");

        assertEquals("bucket", path.getBucketName());
        assertEquals("", path.getKey());
    }

    @Test
    void createWithTrailingSlash()
    {
        S3Path path = forPath("/bucket/");

        assertEquals(path.getBucketName(), "bucket");
        assertEquals(path.getKey(), "");
    }

    @Test
    void createWithPath()
    {
        S3Path path = forPath("/bucket/path/to/file");

        assertEquals(path.getBucketName(), "bucket");
        assertEquals(path.getKey(), "path/to/file");
    }

    @Test
    void createWithPathAndTrailingSlash()
    {
        S3Path path = forPath("/bucket/path/to/dir/");

        assertEquals("bucket", path.getBucketName());
        assertEquals("path/to/dir/", path.getKey());
    }

    @Test
    void createWithPathAndTrailingSlashDir()
    {
        S3Path path = forPath("/bucket/path/to/dir/");

        assertEquals("bucket", path.getBucketName());
        assertEquals("path/to/dir/", path.getKey());
    }

    @Test
    void createRelative()
    {
        S3Path path = forPath("path/to/file");

        assertNull(path.getFileStore());
        assertEquals(path.getKey(), "path/to/file");
        assertFalse(path.isAbsolute());
    }

    @Test
    void getParent()
    {
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file").getParent());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file/").getParent());
        assertNull(forPath("/bucket/").getParent());
        assertNull(forPath("/bucket").getParent());
    }

    @Test
    void normalize()
    {
        assertEquals(forPath("/bucket"), forPath("/bucket").normalize());
        assertEquals(forPath("/bucket/"), forPath("/bucket/").normalize());
        assertEquals(forPath("/bucket/"), forPath("/bucket/.").normalize());

        // We can't normalize to outside of the bucket
        assertEquals(forPath("/bucket/"), forPath("/bucket/..").normalize());
        assertEquals(forPath("/bucket/path"), forPath("/bucket/../path").normalize());

        // Various different spellings of the same path
        assertEquals(forPath("/bucket/path/to"), forPath("/bucket/path/to").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file/../").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/./").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/./path/to/").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/foo/./../bar/../path/to/").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/foo/bar/../../").normalize());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/././././././foo/./././../././bar/./././../path/./to/././").normalize());

        S3Path path = forPath("../bucket/path/to");
        assertTrue(path == path.normalize());
    }

    @Test
    void nameCount()
    {
        assertEquals(forPath("/bucket/path/to/file").getNameCount(), 3);
        assertEquals(forPath("/bucket/").getNameCount(), 0);
    }

    @Test
    void relativize()
    {
        Path path = forPath("/bucket/path/to/file");
        Path other = forPath("/bucket/path/to/file/hello");

        assertEquals(forPath("hello"), path.relativize(other));

        // another
        assertEquals(forPath("file/hello"),
                     forPath("/bucket/path/to/").relativize(forPath("/bucket/path/to/file/hello")));

        // empty
        assertEquals(forPath(""), forPath("/bucket/path/to/").relativize(forPath("/bucket/path/to/")));
    }

    // register
    @Test
    void registerWatcherShouldThrowException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").register(null);
        });

        assertNotNull(exception);
    }

    /**
     * This covers {@link S3Path#register (WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
     */
    @Test
    void registerWatcherWithEventsAndModifierShouldThrowUnsupportedOperationException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").register(null);
        });

        assertNotNull(exception);
    }

    // to file
    @Test
    void toFile()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").toFile();
        });

        assertNotNull(exception);
    }

    // compares to

    @Test
    void compare()
    {
        assertEquals(0, forPath("file1").compareTo(forPath("file1")));
        assertEquals(0, forPath("/path/file1").compareTo(forPath("/path/file1")));
        assertEquals(-1, forPath("/A/file1").compareTo(forPath("/B/file1")));
        assertEquals(1, forPath("/B/file1").compareTo(forPath("/A/file1")));
        assertTrue(forPath("/AA/file1").compareTo(forPath("/A/file1")) > 0);
        assertTrue(forPath("a").compareTo(forPath("aa")) < 0);
        assertTrue(forPath("ab").compareTo(forPath("aa")) > 0);
    }

    // toRealPath

    @Test
    void toRealPathThrowException()
            throws IOException
    {
        Path path = forPath("/file1");
        Path other = path.toRealPath();

        assertEquals(path, other);
    }

    // toAbsolutePath

    @SuppressWarnings("unused")
    @Test
    void toAbsolutePathRelativePathThrowException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            forPath("file1").toAbsolutePath();
        });

        assertNotNull(exception);
    }

    @Test
    void toAbsolutePath()
    {
        Path path = forPath("/file1");
        Path other = path.toAbsolutePath();

        assertEquals(path, other);
    }

    @Test
    void hashCodeHashMap()
    {
        HashMap<S3Path, String> hashMap = new HashMap<>();
        hashMap.put(forPath("/bucket/a"), "a");
        hashMap.put(forPath("/bucket/a"), "b");

        assertEquals(1, hashMap.size());
        assertEquals("b", hashMap.get(forPath("/bucket/a")));
    }

    @Test
    void preconditions()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);

            new S3Path(fileSystem, "/");
        });

        assertNotNull(exception);
    }

    @Test
    void constructors()
    {
        S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        S3Path path = new S3Path(fileSystem, "/buckname");

        assertEquals("buckname", path.getBucketName());
        assertEquals("", path.getKey());
        assertNull(path.getParent());
        assertEquals("", path.getKey());

        path = new S3Path(fileSystem, "/buckname/");

        assertEquals("buckname", path.getBucketName());
        assertEquals("", path.getKey());

        path = new S3Path(fileSystem, "/buckname/file");

        assertEquals("buckname", path.getBucketName());
        assertEquals("file", path.getKey());

        path = new S3Path(fileSystem, "/buckname/dir/file");

        assertEquals("buckname", path.getBucketName());
        assertEquals("dir/file", path.getKey());

        path = new S3Path(fileSystem, "dir/file");

        assertNull(path.getFileStore());
        assertEquals("dir/file", path.getKey());
        assertEquals("dir/", path.getParent().toString());

        path = new S3Path(fileSystem, "bla");

        assertNull(path.getFileStore());
        assertEquals("bla", path.getKey());
        assertNull(path.getParent());

        path = new S3Path(fileSystem, "");

        assertNull(path.getFileStore());
        assertEquals("", path.getKey());
        assertNull(path.getParent());
    }

    @Test
    void register()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            S3Path path = forPath("/buck/file");
            path.register(null);
        });

        assertNotNull(exception);
    }

    @Test
    void registerWatchService()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            S3Path path = forPath("/buck/file");
            path.register(null, new WatchEvent.Kind<?>[0], new WatchEvent.Modifier[0]);
        });

        assertNotNull(exception);
    }

}
