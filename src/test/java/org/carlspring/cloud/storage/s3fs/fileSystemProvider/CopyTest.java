package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyTest
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
    void copy()
            throws IOException
    {
        final String content = "content-file-1";

        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir", "dir2").file("dir/file1", content.getBytes());

        // act
        FileSystem fs = createNewS3FileSystem();

        Path file = fs.getPath("/bucketA/dir/file1");
        Path fileDest = fs.getPath("/bucketA", "dir2", "file2");

        s3fsProvider.copy(file, fileDest, StandardCopyOption.REPLACE_EXISTING);

        // assertions
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test
    void copySameFile()
            throws IOException
    {
        final String content = "sample-content";

        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes());

        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");

        Path fileDest = fs.getPath("/bucketA", "dir", "file1");

        s3fsProvider.copy(file, fileDest);

        // assertions
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
        assertEquals(file, fileDest);
    }

    @Test
    void copyAlreadyExistsWithReplace()
            throws IOException
    {
        final String content = "sample-content";

        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes()).file("dir/file2");

        // act
        FileSystem fs = createNewS3FileSystem();

        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");

        s3fsProvider.copy(file, fileDest, StandardCopyOption.REPLACE_EXISTING);

        // assertions
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test
    void copyAlreadyExists()
            throws IOException
    {
        final String content = "sample-content";

        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1", content.getBytes()).file("dir/file2", content.getBytes());

        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");

        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> s3fsProvider.copy(file, fileDest));

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
