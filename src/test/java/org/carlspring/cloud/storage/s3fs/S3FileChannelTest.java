package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class S3FileChannelTest
        extends S3UnitTestBase
{

    private final S3ClientMock client = S3MockFactory.getS3ClientMock();


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
    void constructorRead()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ));

        assertNotNull(channel);

        channel.read(ByteBuffer.allocate(10));
        channel.close();
    }

    @Test
    void constructorReadButTryToWrite()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath(
                "/buck/file1");
        final S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ));

        assertNotNull(channel);

        final ByteBuffer wrap = ByteBuffer.wrap("hoi".getBytes());

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NonWritableChannelException.class, () -> channel.write(wrap));

        assertNotNull(exception);
    }

    @Test
    void constructorWrite()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE));

        assertNotNull(channel);

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test
    void constructorWriteButTryToRead()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final S3FileChannel channel = new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE));

        assertNotNull(channel);

        final ByteBuffer byteBuffer = ByteBuffer.allocate(10);

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NonReadableChannelException.class, () -> channel.read(byteBuffer));

        assertNotNull(exception);
    }

    @Test
    void readNeedsToCloseChannel()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final S3FileChannel channel = spy(new S3FileChannel(file1, EnumSet.of(StandardOpenOption.READ)));

        assertNotNull(channel);

        channel.close();

        verify(channel, times(1)).implCloseChannel();
        verify(client, never()).putObject(isA(PutObjectRequest.class), isA(RequestBody.class));
    }

    @Test
    void writeNeedsToCloseChannel()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");

        final S3FileChannel channel = spy(new S3FileChannel(file1, EnumSet.of(StandardOpenOption.WRITE)));

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).implCloseChannel();
        verify(client, times(1)).putObject(isA(PutObjectRequest.class), isA(RequestBody.class));
    }

    @Test
    void alreadyExists()
            throws IOException
    {
        client.bucket("buck").file("file1");

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(FileAlreadyExistsException.class,
                                                 () -> new S3FileChannel(file1, EnumSet.of(
                                                         StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)));

        assertNotNull(exception);
    }

    @Test
    void brokenNetwork()
    {
        final GetObjectRequest request = GetObjectRequest.builder().bucket("buck").key("file2").build();
        doThrow(new RuntimeException("network broken")).when(client).getObject(request);

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file2 = (S3Path) fileSystem.getPath("/buck/file2");

        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(RuntimeException.class, () -> new S3FileChannel(file2, readOption));

        assertNotNull(exception);
    }

    @Test
    void shouldNotCreateChannelWithWriteWhenTargetDoesNotExist()
            throws SecurityException, IllegalArgumentException
    {
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file2 = (S3Path) fileSystem.getPath("/buck/file2");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NoSuchFileException.class, () -> new S3FileChannel(file2, writeOption));

        assertNotNull(exception);
    }

}
