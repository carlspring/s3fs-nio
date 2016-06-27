package com.upplication.s3fs;


import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;
import com.upplication.s3fs.util.S3Utils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doThrow;

public class S3UtilsTest extends S3UnitTestBase {
    private S3FileSystem fileSystem = null;

    @Before
    public void setup() throws IOException {
        fileSystem = (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucket");
    }

    @Test
    public void getS3ObjectSummary() throws IOException {
        S3Path root = fileSystem.getPath("/bucket");
        S3Path file1 = (S3Path) root.resolve("file1");
        OutputStream outputStream = Files.newOutputStream(file1, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        String contentString = "Some content String";
        outputStream.write(contentString.getBytes());
        outputStream.close();
        S3ObjectSummary file1ObjectSummary = getS3ObjectSummary(file1);
        assertEquals("bucket", file1ObjectSummary.getBucketName());
        assertEquals(null, file1ObjectSummary.getETag());
        assertEquals("file1", file1ObjectSummary.getKey());
        assertNotNull(file1ObjectSummary.getLastModified());
        Owner owner = file1ObjectSummary.getOwner();
        assertNotNull(owner);
        assertEquals("Mock", owner.getDisplayName());
        assertEquals("1", owner.getId());
        assertEquals(19, file1ObjectSummary.getSize());
    }


    @Test(expected = NoSuchFileException.class)
    public void getS3ObjectSummary404() throws IOException {
        S3Path root = fileSystem.getPath("/bucket");
        S3Path file1 = (S3Path) root.resolve("file1");
        getS3ObjectSummary(file1);
    }

    @Test(expected = AmazonS3Exception.class)
    public void getS3ObjectSummary500() throws IOException {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        AmazonS3Exception toBeThrown = new AmazonS3Exception("We messed up");
        toBeThrown.setStatusCode(500);
        doThrow(toBeThrown).when(client).getObjectAcl("bucket", "file2");
        S3Path root = fileSystem.getPath("/bucket");
        S3Path file2 = (S3Path) root.resolve("file2");
        Files.createFile(file2);
        getS3ObjectSummary(file2);
    }


    public S3ObjectSummary getS3ObjectSummary(S3Path s3Path) throws NoSuchFileException {
        return new S3Utils().getS3ObjectSummary(s3Path);
    }


}
