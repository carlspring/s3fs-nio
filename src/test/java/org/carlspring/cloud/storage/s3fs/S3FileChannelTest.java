package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.AmazonS3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.*;
import java.util.EnumSet;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class S3FileChannelTest
        extends S3UnitTestBase
{

    private final AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);

        reset(client);
    }

    @AfterEach
    public void tearDown()
    {
        s3fsProvider.close((S3FileSystem) fileSystem);
    }

    @Test
    public void constructorRead()
            throws IOException
    {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ));

        assertNotNull(channel);

        channel.read(ByteBuffer.allocate(10));
        channel.close();
    }

    @Test
    public void constructorReadButTryToWrite()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NonWritableChannelException.class, () -> {
            client.bucket("buck").file("file1");

            S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
            S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ));

            assertNotNull(channel);

            channel.write(ByteBuffer.wrap("hoi".getBytes()));
            channel.close();
        });

        assertNotNull(exception);
    }

    @Test
    public void constructorWrite()
            throws IOException
    {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE));

        assertNotNull(channel);

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test
    public void constructorWriteButTryToRead()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NonReadableChannelException.class, () -> {
            client.bucket("buck").file("file1");

            S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
            S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE));

            assertNotNull(channel);

            channel.read(ByteBuffer.allocate(10));
            channel.close();
        });

        assertNotNull(exception);
    }

    @Test
    public void readNeedsToCloseChannel()
            throws IOException
    {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3FileChannel channel = spy(new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ)));

        assertNotNull(channel);

        channel.close();

        verify(channel, times(1)).implCloseChannel();
        verify(client, never()).putObject(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void writeNeedsToCloseChannel()
            throws IOException
    {
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");

        S3FileChannel channel = spy(new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE)));

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).implCloseChannel();
        verify(client, times(1)).putObject(eq("buck"), eq("file1"), any(InputStream.class), any(ObjectMetadata.class));
    }

    @Test
    public void alreadyExists()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> {
            client.bucket("buck").file("file1");

            S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");

            new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
        });

        assertNotNull(exception);
    }

    @Test
    public void brokenNetwork()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(RuntimeException.class, () -> {
            doThrow(new RuntimeException("network broken")).when(client).getObject("buck", "file2");

            S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file2");
            S3FileChannel channel = new S3FileChannel(file2, EnumSet.of(StandardOpenOption.READ));

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
            S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file2");
            S3FileChannel channel = new S3FileChannel(file2, EnumSet.of(StandardOpenOption.WRITE));

            Field f = channel.getClass().getDeclaredField("tempFile");
            f.setAccessible(true);

            Path tempFile = (Path) f.get(channel);
            Files.delete(tempFile);

            channel.close();
        });

        assertNotNull(exception);
    }

}
