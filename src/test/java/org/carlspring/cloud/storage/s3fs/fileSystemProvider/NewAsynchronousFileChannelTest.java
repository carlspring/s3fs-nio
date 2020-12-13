package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NewAsynchronousFileChannelTest
        extends S3UnitTestBase
{

    private final S3ClientMock client = S3MockFactory.getS3ClientMock();

    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    void fileChannelWriteAndRead()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        client.bucket("bucketA").dir("dir").file("dir/file", "".getBytes());

        final Path base = createNewS3FileSystem().getPath("/bucketA", "dir");
        final Path file = base.resolve("file");
        final String content = "content";
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);
        final ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
        final ByteBuffer bufferRead = ByteBuffer.allocate(7);

        //when
        try (final AsynchronousFileChannel channel = s3fsProvider.newAsynchronousFileChannel(file, options, null))
        {
            final Future<Integer> writeResult = channel.write(buffer, 0);
            writeResult.get(1, TimeUnit.SECONDS);
            assertTrue(writeResult.isDone());

            final Future<Integer> readResult = channel.read(bufferRead, 0);
            readResult.get(1, TimeUnit.SECONDS);
            assertTrue(readResult.isDone());

            //then
            assertArrayEquals(bufferRead.array(), buffer.array());
        }
    }

    @Test
    void fileChannelSize()
            throws IOException
    {
        //given
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);

        //when
        final long size;
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, options, null))
        {
            size = fileChannel.size();
        }

        //then
        assertEquals(content.length(), size);
    }

    @Test
    void fileChannelAnotherSize()
            throws IOException
    {
        //given
        final String content = "content-more-large";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);

        //when
        final long size;
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, options, null))
        {
            size = fileChannel.size();
        }

        //then
        assertEquals(content.length(), size);
    }

    @Test
    void fileChannelPositionRead()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final String content = "content-more-larger";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final ByteBuffer copy = ByteBuffer.allocate(3);
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> option = EnumSet.of(StandardOpenOption.READ);
        final int expected = 3;

        //when
        final int actual;
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, option, null))
        {
            final Future<Integer> readResult = fileChannel.read(copy, 0);
            actual = readResult.get(1, TimeUnit.SECONDS);
            assertTrue(readResult.isDone());
        }

        //then
        assertEquals(expected, actual);

    }

    @Test
    void fileChannelPositionWrite()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final String content = "content-more-larger";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final ByteBuffer copy = ByteBuffer.allocate(5);
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> option = EnumSet.of(StandardOpenOption.WRITE);
        int expected = 5;

        //when
        final int actual;
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, option, null))
        {
            final Future<Integer> readResult = fileChannel.write(copy, 0);
            actual = readResult.get(1, TimeUnit.SECONDS);
            assertTrue(readResult.isDone());
        }

        //then
        assertEquals(expected, actual);
    }

    @Test
    void fileChannelIsOpen()
            throws IOException
    {
        //given
        client.bucket("bucketA").dir("dir").file("dir/file");

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, writeOption, null))
        {
            //then
            assertTrue(fileChannel.isOpen());
        }

        //when
        final AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, readOption, null);

        //then
        assertTrue(fileChannel.isOpen());
        fileChannel.close();

        assertFalse(fileChannel.isOpen());
    }

    @Test
    void fileChannelRead()
            throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        //given
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        final ByteBuffer bufferRead = ByteBuffer.allocate(7);
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, readOption, null))
        {
            final Future<Integer> readResult = fileChannel.read(bufferRead, 0);
            readResult.get(1, TimeUnit.SECONDS);
            assertTrue(readResult.isDone());
        }

        //then
        assertArrayEquals(bufferRead.array(), content.getBytes());
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    void fileChannelReadPartialContent()
            throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        //given
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        final ByteBuffer bufferRead = ByteBuffer.allocate(4);
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, readOption, null))
        {
            final Future<Integer> readResult = fileChannel.read(bufferRead, 3);
            readResult.get(1, TimeUnit.SECONDS);
            assertTrue(readResult.isDone());
        }

        //then
        assertArrayEquals("tent".getBytes(), bufferRead.array());
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    void fileChannelTruncate()
            throws IOException
    {
        //given
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);
        int expected = 1;

        //when
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, options, null))
        {
            // discard all content except the first c.
            fileChannel.truncate(1);

            //then
            assertEquals(expected, fileChannel.size());
        }
    }

    @Test
    void fileChannelAnotherTruncate()
            throws IOException
    {
        //given
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);
        final int expected = 3;

        //when
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, options, null))
        {
            assertEquals(7, fileChannel.size());

            // discard all content except the first three chars 'con'
            fileChannel.truncate(3);

            //then
            assertEquals(expected, fileChannel.size());
        }

    }

    @Test
    void fileChannelTruncateGreaterThanSize()
            throws IOException
    {
        //given
        final String content = "content";
        client.bucket("bucketA").dir("dir").file("dir/file", content.getBytes());

        final Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, options, null))
        {
            fileChannel.truncate(10);
        }

        //then
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    void fileChannelCreateEmpty()
            throws IOException
    {
        //given
        client.bucket("bucketA").dir("dir").file("dir/file", "".getBytes());

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> createOption = EnumSet.of(StandardOpenOption.CREATE);

        //when
        try (AsynchronousFileChannel ignored = s3fsProvider.newAsynchronousFileChannel(file, createOption, null))
        {
            // Do nothing
        }

        //then
        assertTrue(Files.exists(file));
        assertArrayEquals("".getBytes(), Files.readAllBytes(file));
    }

    @Test
    void fileChannelCloseTwice()
            throws IOException
    {
        //given
        client.bucket("bucketA").dir("dir");

        final Path base = createNewS3FileSystem().getPath("/bucketA/dir");
        final Path file = Files.createFile(base.resolve("file"));
        final EnumSet<StandardOpenOption> noOptions = EnumSet.noneOf(StandardOpenOption.class);

        //when
        final AsynchronousFileChannel fileChannel = s3fsProvider.newAsynchronousFileChannel(file, noOptions, null);
        fileChannel.close();
        fileChannel.close();

        //then
        assertTrue(Files.exists(file));
    }

    @Test
    void fileChannelNotExists()
            throws IOException
    {
        //given
        client.bucket("bucketA").dir("dir");

        final Path base = createNewS3FileSystem().getPath("/bucketA", "dir");
        final Path file = base.resolve("file");
        final EnumSet<StandardOpenOption> noOptions = EnumSet.noneOf(StandardOpenOption.class);

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NoSuchFileException.class,
                                                 () -> s3fsProvider.newAsynchronousFileChannel(file, noOptions, null));

        //then
        assertNotNull(exception);
    }

    /**
     * Creates a new file system for S3 scheme with fake credentials and global endpoint.
     *
     * @return S3FileSystem
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
