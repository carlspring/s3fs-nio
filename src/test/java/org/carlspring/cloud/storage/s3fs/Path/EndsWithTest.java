package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EndsWithTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3_GLOBAL_URI_TEST, null);
    }

    @AfterEach
    public void tearDown()
    {
        s3fsProvider.close((S3FileSystem) fileSystem);
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    public void endsWithAbsoluteRelative()
    {
        assertTrue(getPath("/bucket/file1").endsWith(getPath("file1")));
    }

    @Test
    public void endsWithAbsoluteAbsolute()
    {
        assertTrue(getPath("/bucket/file1").endsWith(getPath("/bucket/file1")));
    }

    @Test
    public void endsWithRelativeRelative()
    {
        assertTrue(getPath("file/file1").endsWith(getPath("file1")));
    }

    @Test
    public void endsWithRelativeAbsolute()
    {
        assertFalse(getPath("file/file1").endsWith(getPath("/bucket")));
    }

    @Test
    public void endsWithDifferenteFileSystem()
    {
        assertFalse(getPath("/bucket/file1").endsWith(Paths.get("/bucket/file1")));
    }

    @Test
    public void endsWithBlankRelativeAbsolute()
    {
        assertFalse(getPath("").endsWith(getPath("/bucket")));
    }

    @Test
    public void endsWithBlankBlank()
    {
        assertTrue(getPath("").endsWith(getPath("")));
    }

    @Test
    public void endsWithRelativeBlankAbsolute()
    {
        assertFalse(getPath("/bucket/file1").endsWith(getPath("")));
    }

    @Test
    public void endsWithRelativeBlankRelative()
    {
        assertFalse(getPath("file1").endsWith(getPath("")));
    }

    @Test
    public void endsWithDifferent()
    {
        assertFalse(getPath("/bucket/dir/dir/file1").endsWith(getPath("fail/dir/file1")));
    }

    @Test
    public void endsWithDifferentProvider()
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path fileLinux = linux.getPath("/file");

            assertFalse(getPath("/bucket/file").endsWith(fileLinux));
        }

        try (FileSystem window = MemoryFileSystemBuilder.newWindows().build("window"))
        {
            Path file = window.getPath("c:/file");

            assertFalse(getPath("/c/file").endsWith(file));
        }
    }

    @Test
    public void endsWithString()
    {
        // endsWithAbsoluteRelative(){
        assertTrue(getPath("/bucket/file1").endsWith("file1"));
        // endsWithAbsoluteAbsolute
        assertTrue(getPath("/bucket/file1").endsWith("/bucket/file1"));
        // endsWithRelativeRelative
        assertTrue(getPath("file/file1").endsWith("file1"));
        // endsWithRelativeAbsolute
        assertFalse(getPath("file/file1").endsWith("/bucket"));
        // endsWithBlankRelativeAbsolute
        assertFalse(getPath("").endsWith("/bucket"));
        // endsWithBlankBlank
        assertTrue(getPath("").endsWith(""));
        // endsWithRelativeBlankAbsolute
        assertFalse(getPath("/bucket/file1").endsWith(""));
        // endsWithRelativeBlankRelative
        assertFalse(getPath("file1").endsWith(""));
    }

}
