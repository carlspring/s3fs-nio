package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateDirectoryTest
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
    void createDirectory()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA");

        // act
        Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        Files.createDirectory(base);

        // assertions
        assertTrue(Files.exists(base));
        assertTrue(Files.isDirectory(base));
        assertTrue(Files.exists(base));
    }


    @Test
    void createDirectoryInNewBucket()
            throws IOException
    {
        Path root = createNewS3FileSystem().getPath("/newer-bucket");

        Path resolve = root.resolve("folder");
        Path path = Files.createDirectories(resolve);

        // assertions
        assertEquals("/newer-bucket/folder", path.toAbsolutePath().toString());
        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.exists(resolve));
        assertTrue(Files.isDirectory(resolve));
        assertTrue(Files.exists(path));
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void createDirectoryWithSpace()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA");

        // act
        Path base = createNewS3FileSystem().getPath("/bucketA/dir with space/another space");
        Files.createDirectories(base);

        // assertions
        assertTrue(Files.exists(base));
        assertTrue(Files.isDirectory(base));

        // parent
        assertTrue(Files.exists(base.getParent()));
        assertTrue(Files.isDirectory(base.getParent()));
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
     */
    private S3FileSystem createNewS3FileSystem()
    {
        try
        {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        }
        catch (FileSystemNotFoundException e)
        {
            return (S3FileSystem) s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }
    }

}
