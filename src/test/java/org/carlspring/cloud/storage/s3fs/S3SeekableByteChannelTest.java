package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.AmazonS3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.EnumSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class S3SeekableByteChannelTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        fileSystem = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void constructor()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = new S3SeekableByteChannel(file1,
                                                                  EnumSet.of(StandardOpenOption.WRITE,
                                                                             StandardOpenOption.READ));

        assertNotNull(channel);

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test
    public void readDontNeedToSyncTempFile()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.READ)));

        assertNotNull(channel);

        channel.close();

        verify(channel, never()).sync();
    }

    @Test
    public void writeNeedToSyncTempFile()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");

        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE)));

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).sync();
    }

    @Test
    public void alreadyExists()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> {
            AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
            client.bucket("buck").file("file1");

            S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST)
                                               .getPath("/buck/file1");
            new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
        });

        assertNotNull(exception);
    }

    @Test
    public void brokenNetwork()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(RuntimeException.class, () -> {
            AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
            doThrow(new RuntimeException("network broken")).when(client).getObject("buck", "file2");

            S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST)
                                               .getPath("/buck/file2");
            S3SeekableByteChannel channel = new S3SeekableByteChannel(file2,
                                                                      EnumSet.of(StandardOpenOption.WRITE,
                                                                                 StandardOpenOption.READ));
            channel.close();
        });

        assertNotNull(exception);
    }

    @Test
    public void tempFileDisappeared()
            throws SecurityException,
                   IllegalArgumentException
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NoSuchFileException.class, () -> {
            S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST)
                                               .getPath("/buck/file2");
            S3SeekableByteChannel channel = new S3SeekableByteChannel(file2,
                                                                      EnumSet.of(StandardOpenOption.WRITE,
                                                                                 StandardOpenOption.READ));

            Field f = channel.getClass().getDeclaredField("tempFile");
            f.setAccessible(true);

            Path tempFile = (Path) f.get(channel);

            Files.delete(tempFile);

            channel.close();
        });

        assertNotNull(exception);
    }

}
