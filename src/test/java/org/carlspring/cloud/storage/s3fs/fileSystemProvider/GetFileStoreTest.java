package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileStore;
import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class GetFileStoreTest
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
    void shouldReturnS3FileStore()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1");

        // act
        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");

        FileStore fileStore = s3fsProvider.getFileStore(file1);
        assertInstanceOf(S3FileStore.class, fileStore);
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
