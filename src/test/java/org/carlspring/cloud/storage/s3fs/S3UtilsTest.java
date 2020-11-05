package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.S3Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static software.amazon.awssdk.http.HttpStatusCode.INTERNAL_SERVER_ERROR;

class S3UtilsTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        fileSystem = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);

        final S3ClientMock client = S3MockFactory.getS3ClientMock();

        client.bucket("bucket");
    }

    @Test
    void getS3Object()
            throws IOException
    {
        final S3Path root = (S3Path) fileSystem.getPath("/bucket");
        final S3Path file1 = (S3Path) root.resolve("file1");

        final String contentString = "Some content String";

        final OutputStream outputStream = Files.newOutputStream(file1, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        outputStream.write(contentString.getBytes());
        outputStream.close();

        final S3Object file1Object = getS3Object(file1);

        assertNull(file1Object.eTag());
        assertEquals("file1", file1Object.key());
        assertNotNull(file1Object.lastModified());

        final Owner owner = file1Object.owner();

        assertNotNull(owner);
        assertEquals("Mock", owner.displayName());
        assertEquals("1", owner.id());
        assertEquals(19, file1Object.size());
    }

    @Test
    void getS3Object404()
    {
        final S3Path root = (S3Path) fileSystem.getPath("/bucket");
        final S3Path file1 = (S3Path) root.resolve("file1");

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NoSuchFileException.class, () -> getS3Object(file1));

        assertNotNull(exception);
    }

    @Test
    void getS3Object500()
            throws IOException
    {
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final AwsServiceException toBeThrown = S3Exception.builder().message("We messed up").statusCode(
                INTERNAL_SERVER_ERROR).build();

        GetObjectAclRequest request = GetObjectAclRequest.builder().bucket("bucket").key("file2").build();
        doThrow(toBeThrown).when(client).getObjectAcl(request);

        final S3Path root = (S3Path) fileSystem.getPath("/bucket");
        final S3Path file2 = (S3Path) root.resolve("file2");

        Files.createFile(file2);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(S3Exception.class, () -> getS3Object(file2));

        assertNotNull(exception);
    }

    public S3Object getS3Object(S3Path s3Path)
            throws NoSuchFileException
    {
        return new S3Utils().getS3Object(s3Path);
    }

}
