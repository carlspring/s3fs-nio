package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.attribute.S3PosixFileAttributes;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class GetFileAttributeViewTest
        extends S3UnitTestBase
{

    private S3FileSystemProvider s3fsProvider;


    @Before
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void getBasicFileAttributeView()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");
        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        BasicFileAttributeView view = s3fsProvider.getFileAttributeView(file, BasicFileAttributeView.class);

        assertEquals("basic", view.name());
        assertNotNull(view.readAttributes());
        assertTrue(view.readAttributes() instanceof S3BasicFileAttributes);
    }

    @Test
    public void getPosixFileAttributeView()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file");

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        BasicFileAttributeView view = s3fsProvider.getFileAttributeView(file, PosixFileAttributeView.class);

        assertEquals("posix", view.name());
        assertNotNull(view.readAttributes());
        assertTrue(view.readAttributes() instanceof S3PosixFileAttributes);
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
