package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import java.nio.file.NoSuchFileException;
import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckAccessTest
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

    // check access
    @Test
    void checkAccessRead()
            throws IOException
    {
        // fixtures
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final String bucketName = "bucketA";
        final String directory = "dir";
        final String filePath = directory + "/file";
        client.bucket(bucketName).dir(directory).file(filePath);

        final FileSystem fs = createNewS3FileSystem();
        final String path = String.format("/%s/%s", bucketName, filePath);
        final Path file1 = fs.getPath(path);

        s3fsProvider.checkAccess(file1, AccessMode.READ);
    }


    @Test
    void checkAccessWrite()
            throws IOException
    {
        // fixtures
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final String bucketName = "bucketA";
        final String directory = "dir";
        final String filePath = directory + "/file";
        client.bucket(bucketName).dir(directory).file(filePath);

        final S3FileSystem fs = createNewS3FileSystem();
        final String path = String.format("/%s/%s", bucketName, filePath);
        final S3Path file1 = fs.getPath(path);

        s3fsProvider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test
    void checkAccessMissingFile()
        throws IOException
    {
        // fixtures
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final String bucketName = "bucketA";
        final String directory = "dir";
        client.bucket("bucketA");

        final String path = String.format("/%s/%s", bucketName, directory);
        final Path file1 = createNewS3FileSystem().getPath(path);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NoSuchFileException.class,
            () -> s3fsProvider.checkAccess(file1, AccessMode.WRITE));

        assertNotNull(exception);
    }


    @Test
    void checkAccessExecute()
            throws IOException
    {
        // fixtures
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final String bucketName = "bucketA";
        final String directory = "dir";
        final String filePath = directory + "/file";
        client.bucket(bucketName).dir(directory).file(filePath);

        final String path = String.format("/%s/%s", bucketName, filePath);
        final Path file1 = createNewS3FileSystem().getPath(path);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(AccessDeniedException.class,
                                                 () -> s3fsProvider.checkAccess(file1, AccessMode.EXECUTE));

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
        catch (final FileSystemNotFoundException e)
        {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }
    }

}
