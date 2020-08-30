package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.HashMap;

import org.junit.jupiter.api.*;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.*;

public class S3PathTest
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
    public void createNoPath()
    {
        S3Path path = forPath("/bucket");

        assertEquals("bucket", path.getFileStore().name());
        assertEquals("", path.getKey());
    }

    @Test
    public void createWithTrailingSlash()
    {
        S3Path path = forPath("/bucket/");

        assertEquals(path.getFileStore().name(), "bucket");
        assertEquals(path.getKey(), "");
    }

    @Test
    public void createWithPath()
    {
        S3Path path = forPath("/bucket/path/to/file");

        assertEquals(path.getFileStore().name(), "bucket");
        assertEquals(path.getKey(), "path/to/file");
    }

    @Test
    public void createWithPathAndTrailingSlash()
    {
        S3Path path = forPath("/bucket/path/to/dir/");

        assertEquals("bucket", path.getFileStore().name());
        assertEquals("path/to/dir/", path.getKey());
    }

    @Test
    public void createWithPathAndTrailingSlashDir()
    {
        S3Path path = forPath("/bucket/path/to/dir/");

        assertEquals("bucket", path.getFileStore().name());
        assertEquals("path/to/dir/", path.getKey());
    }

    @Test
    public void createRelative()
    {
        S3Path path = forPath("path/to/file");

        assertNull(path.getFileStore());
        assertEquals(path.getKey(), "path/to/file");
        assertFalse(path.isAbsolute());
    }

    @Test
    public void getParent()
    {
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file").getParent());
        assertEquals(forPath("/bucket/path/to/"), forPath("/bucket/path/to/file/").getParent());
        assertNull(forPath("/bucket/").getParent());
        assertNull(forPath("/bucket").getParent());
    }

    @Test
    public void nameCount()
    {
        assertEquals(forPath("/bucket/path/to/file").getNameCount(), 3);
        assertEquals(forPath("/bucket/").getNameCount(), 0);
    }

    @Test
    public void relativize()
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
    public void registerWithEventsThrowException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").register(null);
        });

        assertNotNull(exception);
    }

    @Test
    public void registerThrowException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").register(null);
        });

        assertNotNull(exception);
    }

    @Test
    public void registerWithEventsAndModifierThrowException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").register(null);
        });

        assertNotNull(exception);
    }

    // to file
    @Test
    public void toFile()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            forPath("file1").toFile();
        });

        assertNotNull(exception);
    }

    // compares to

    @Test
    public void compare()
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
    public void toRealPathThrowException()
            throws IOException
    {
        Path path = forPath("/file1");
        Path other = path.toRealPath();

        assertEquals(path, other);
    }

    // toAbsolutePath

    @SuppressWarnings("unused")
    @Test
    public void toAbsolutePathRelativePathThrowException()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            forPath("file1").toAbsolutePath();
        });

        assertNotNull(exception);
    }

    @Test
    public void toAbsolutePath()
    {
        Path path = forPath("/file1");
        Path other = path.toAbsolutePath();

        assertEquals(path, other);
    }

    @Test
    public void hashCodeHashMap()
    {
        HashMap<S3Path, String> hashMap = new HashMap<>();
        hashMap.put(forPath("/bucket/a"), "a");
        hashMap.put(forPath("/bucket/a"), "b");

        assertEquals(1, hashMap.size());
        assertEquals("b", hashMap.get(forPath("/bucket/a")));
    }

    @Test
    public void preconditions()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);

            new S3Path(fileSystem, "/");
        });

        assertNotNull(exception);
    }

    @Test
    public void constructors()
    {
        S3FileSystem fileSystem = s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        S3Path path = new S3Path(fileSystem, "/buckname");

        assertEquals("buckname", path.getFileStore().name());
        assertEquals("", path.getKey());
        assertNull(path.getParent());
        assertEquals("", path.getKey());

        path = new S3Path(fileSystem, "/buckname/");

        assertEquals("buckname", path.getFileStore().name());
        assertEquals("", path.getKey());

        path = new S3Path(fileSystem, "/buckname/file");

        assertEquals("buckname", path.getFileStore().name());
        assertEquals("file", path.getKey());

        path = new S3Path(fileSystem, "/buckname/dir/file");

        assertEquals("buckname", path.getFileStore().name());
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
    public void register()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            S3Path path = forPath("/buck/file");
            path.register(null);
        });

        assertNotNull(exception);
    }

    @Test
    public void registerWatchService()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, () -> {
            S3Path path = forPath("/buck/file");
            path.register(null, new WatchEvent.Kind<?>[0], new WatchEvent.Modifier[0]);
        });

        assertNotNull(exception);
    }

}
