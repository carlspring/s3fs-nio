package com.upplication.s3fs.FileSystemProvider;

import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CreateDirectoryTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() {
        s3fsProvider = spy(new S3FileSystemProvider());
        doReturn(false).when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(new Properties()).when(s3fsProvider).loadAmazonProperties();
    }

    @Test
    public void createDirectory() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA");

        // act
        Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        Files.createDirectory(base);
        // assert
        assertTrue(Files.exists(base));
        assertTrue(Files.isDirectory(base));
        assertTrue(Files.exists(base));
    }


    @Test
    public void createDirectoryInNewBucket() throws IOException {
        S3Path root = createNewS3FileSystem().getPath("/newer-bucket");
        Path resolve = root.resolve("folder");
        Path path = Files.createDirectories(resolve);
        assertEquals("s3://s3.test.amazonaws.com/newer-bucket/folder", path.toAbsolutePath().toString());
        // assert
        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.exists(root));
        assertTrue(Files.exists(resolve));
        assertTrue(Files.isDirectory(resolve));
        assertTrue(Files.exists(resolve));
    }

    @Test
    public void createDirectoryWithSpace() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA");

        // act
        Path base = createNewS3FileSystem().getPath("/bucketA/dir with space/another space");
        Files.createDirectories(base);
        // assert
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
     * @throws IOException
     */
    private S3FileSystem createNewS3FileSystem() throws IOException {
        try {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        } catch (FileSystemNotFoundException e) {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }

    }
}