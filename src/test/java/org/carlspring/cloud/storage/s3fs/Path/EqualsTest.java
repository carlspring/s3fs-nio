package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class EqualsTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        s3fsProvider.close((S3FileSystem) fileSystem);
        fileSystem.close();
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    public void equals()
    {
        Path path = getPath("/bucketA/dir/file");
        Path path2 = getPath("/bucketA/dir/file");

        assertEquals(path, path2);
    }

    @Test
    public void equalsDir()
    {
        Path path = getPath("/bucketA/dir/");
        Path path2 = getPath("/bucketA/dir/");

        assertEquals(path, path2);
    }

    @Test
    public void equalsBucket()
    {
        Path path = getPath("/bucketA/");
        Path path2 = getPath("/bucketA/");

        assertEquals(path, path2);
    }

    @Test
    public void equalsBucketWithoutEndSlash()
    {
        Path path = getPath("/bucketA/");
        Path path2 = getPath("/bucketA");

        assertNotEquals(path, path2);
    }

    @Test
    public void notEquals()
    {
        Path path = getPath("/bucketA/dir/file");
        Path path2 = getPath("/bucketA/dir/file2");

        assertNotEquals(path, path2);
    }

    @Test
    public void notEqualsDirFile()
    {
        Path path = getPath("/bucketA/dir/asd/");
        Path path2 = getPath("/bucketA/dir/asd");

        assertNotEquals(path, path2);
    }

    @Test
    public void notEqualsNull()
    {
        Path path = getPath("/bucketA/dir/file");

        assertNotEquals(path, null);
    }

    @Test
    public void notEqualsDifferentProvider()
            throws IOException
    {
        Path path = getPath("/c/dir/file");

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path fileLinux = linux.getPath("/dir/file");

            assertNotEquals(path, fileLinux);
        }

        try (FileSystem window = MemoryFileSystemBuilder.newWindows().build("window"))
        {
            Path file = window.getPath("c:/dir/file");

            assertNotEquals(path, file);
        }

        Path pathS3EmptyEndpoint = FileSystems.newFileSystem(URI.create("s3:///"), null).getPath("/bucketA/dir/");
        Path pathS3TestEndpoint = getPath("/bucketA/dir/");

        assertNotEquals(pathS3EmptyEndpoint, pathS3TestEndpoint);
    }

}
