package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.*;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Owner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;

public class CheckAccessTest
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
//!        super.tearDown();

        s3fsProvider.close((S3FileSystem) fileSystem);
        fileSystem.close();
    }

    // check access
    @Test
    public void checkAccessRead()
            throws IOException
    {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file");

        s3fsProvider.checkAccess(file1, AccessMode.READ);
    }

    @Test
    public void checkAccessReadWithoutPermission()
            throws IOException
    {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(AccessDeniedException.class, () -> {
            s3fsProvider.checkAccess(file1, AccessMode.READ);
        });

        // TODO: Assert that the exception message is as expected
        assertNotNull(exception);
    }

    @Test
    public void checkAccessWrite()
            throws IOException
    {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        S3FileSystem fs = createNewS3FileSystem();
        S3Path file1 = fs.getPath("/bucketA/dir/file");

        s3fsProvider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test
    public void checkAccessWriteDifferentUser()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(AccessDeniedException.class, () -> {
            // fixtures
            AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
            client.bucket("bucketA").dir("dir").file("dir/readOnly");

            // return empty list
            doReturn(client.createReadOnly(new Owner("2", "Read Only"))).when(client).getObjectAcl("bucketA",
                                                                                                   "dir/readOnly");

            S3FileSystem fs = createNewS3FileSystem();
            S3Path file1 = fs.getPath("/bucketA/dir/readOnly");

            s3fsProvider.checkAccess(file1, AccessMode.WRITE);
        });

        assertNotNull(exception);
    }

    @Test
    public void checkAccessWriteWithoutPermission()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(AccessDeniedException.class, () -> {
            // fixtures
            AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
            client.bucket("bucketA").dir("dir");

            // return empty list
            doReturn(new AccessControlList()).when(client).getObjectAcl("bucketA", "dir/");

            Path file1 = createNewS3FileSystem().getPath("/bucketA/dir");

            s3fsProvider.checkAccess(file1, AccessMode.WRITE);
        });

        assertNotNull(exception);
    }

    @Test
    public void checkAccessExecute()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(AccessDeniedException.class, () -> {
            // fixtures
            AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
            client.bucket("bucketA").dir("dir").file("dir/file");

            Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file");

            s3fsProvider.checkAccess(file1, AccessMode.EXECUTE);
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
