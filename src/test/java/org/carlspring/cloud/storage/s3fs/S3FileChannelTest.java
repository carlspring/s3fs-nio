package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private static Stream<Arguments> positionAndBytesReadForReadProvider()
    {
        return Stream.of(Arguments.of(0, 10),
                         Arguments.of(5, 9),
                         Arguments.of(15, -1));
    }

    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);

        reset(client);

        client.bucket("buck").file("file1");
    }

    @AfterEach
    public void tearDown()
    {
        s3fsProvider.close((S3FileSystem) fileSystem);
    }

    @ParameterizedTest(name = "{index} ==> position={0}, bytesRead={1}")
    @MethodSource(value = "positionAndBytesReadForReadProvider")
    void constructorReadFromPosition(final int position,
                                     final int expected)
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);
        final ByteBuffer buffer = ByteBuffer.allocate(10);

        //when
        int actual;
        try (final S3FileChannel channel = new S3FileChannel(file1, readOption, null, true))
        {
            final Future<Integer> actualFuture = channel.read(buffer, position);

            actual = actualFuture.get(1, TimeUnit.SECONDS);
            assertTrue(actualFuture.isDone());
        }

        //then
        assertEquals(expected, actual);
    }

    @Test
    void whenChannelNotOpenedThenReadShouldReturnZero()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);
        final ByteBuffer buffer = ByteBuffer.allocate(10);
        int expected = 0;

        //when
        final S3FileChannel channel = new S3FileChannel(file1, readOption, null, true);
        channel.close();
        final Future<Integer> actualFuture = channel.read(buffer, 0);
        final int actual = actualFuture.get(1, TimeUnit.SECONDS);

        //then
        assertFalse(channel.isOpen());
        assertTrue(actualFuture.isDone());
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "{index} ==> position={0}, bytesRead={1}")
    @MethodSource(value = "positionAndBytesReadForReadProvider")
    void constructorReadWithHandlerFromPosition(final int position,
                                                final int expected)
            throws IOException, InterruptedException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        final ByteBuffer buffer = ByteBuffer.allocate(10);
        final ByteBuffer attachment = ByteBuffer.allocate(10);
        final int[] actual = { 0 };
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final CompletionHandler<Integer, ByteBuffer> handler = new CompletionHandler<Integer, ByteBuffer>()
        {
            @Override
            public void completed(Integer bytesRead,
                                  ByteBuffer attachment)
            {
                actual[0] = bytesRead;
                System.out.println("Response from server: " + bytesRead);
                countDownLatch.countDown();
            }

            @Override
            public void failed(Throwable exc,
                               ByteBuffer attachment)
            {
            }
        };

        //when
        try (final S3FileChannel channel = new S3FileChannel(file1, readOption, null, true))
        {
            channel.read(buffer, position, attachment, handler);

            final boolean awaitFinished = countDownLatch.await(1, TimeUnit.SECONDS);

            //then
            assertTrue(awaitFinished);
            assertEquals(expected, actual[0]);
        }
    }

    @Test
    void whenChannelNotOpenedThenReadWithHandlerShouldDoNothing()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        final ByteBuffer buffer = ByteBuffer.allocate(10);
        final ByteBuffer attachment = ByteBuffer.allocate(10);
        final CompletionHandler<Integer, ByteBuffer> handler = new CompletionHandler<Integer, ByteBuffer>()
        {
            @Override
            public void completed(Integer bytesRead,
                                  ByteBuffer attachment)
            {
            }

            @Override
            public void failed(Throwable exc,
                               ByteBuffer attachment)
            {
            }
        };

        //when
        final S3FileChannel channel = spy(new S3FileChannel(file1, readOption, null, true));
        channel.close();
        channel.read(buffer, 0, attachment, handler);

        //then
        assertFalse(channel.isOpen());
    }

    @Test
    void constructorReadButTryToWrite()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);
        final String content = "hello";
        final ByteBuffer wrap = ByteBuffer.wrap(content.getBytes());

        //when
        final S3FileChannel channel = new S3FileChannel(file1, readOption, null, true);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NonWritableChannelException.class, () -> channel.write(wrap, 0));

        //then
        assertNotNull(exception);
    }

    @ParameterizedTest(name = "{index} ==> position={0}")
    @ValueSource(ints = { 0,
                          2,
                          10 })
    void constructorWriteFromPosition(final int position)
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);
        final String content = "hello";
        int expected = content.length();
        final ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());

        //when
        int actual;
        try (final S3FileChannel channel = new S3FileChannel(file1, writeOption, null, true))
        {
            final Future<Integer> actualFuture = channel.write(byteBuffer, position);

            actual = actualFuture.get(1, TimeUnit.SECONDS);
            assertTrue(actualFuture.isDone());
        }

        //then
        assertEquals(expected, actual);
    }

    @Test
    void whenChannelNotOpenedThenWriteShouldReturnZero()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);
        int expected = 0;

        //when
        final S3FileChannel channel = new S3FileChannel(file1, writeOption, null, true);
        channel.close();
        final Future<Integer> actualFuture = channel.write(ByteBuffer.allocate(10), 0);
        final int actual = actualFuture.get(1, TimeUnit.SECONDS);

        //then
        assertFalse(channel.isOpen());
        assertTrue(actualFuture.isDone());
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "{index} ==> position={0}")
    @ValueSource(ints = { 0,
                          2,
                          8 })
    void constructorWriteWithHandlerFromPosition(final int position)
            throws IOException, InterruptedException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        final int expected = 10;
        final int[] actual = { 0 };
        final ByteBuffer buffer = ByteBuffer.allocate(expected);
        final ByteBuffer attachment = ByteBuffer.allocate(expected);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final CompletionHandler<Integer, ByteBuffer> handler = new CompletionHandler<Integer, ByteBuffer>()
        {
            @Override
            public void completed(Integer bytesWritten,
                                  ByteBuffer attachment)
            {
                actual[0] = bytesWritten;
                System.out.println("bytes written: " + bytesWritten);
                countDownLatch.countDown();
            }

            @Override
            public void failed(Throwable exc,
                               ByteBuffer attachment)
            {
                System.out.println("Write failed");
                exc.printStackTrace();
            }
        };

        //when
        try (final S3FileChannel channel = new S3FileChannel(file1, writeOption, null, true))
        {
            channel.write(buffer, position, attachment, handler);

            final boolean awaitFinished = countDownLatch.await(1, TimeUnit.SECONDS);

            //then
            assertTrue(awaitFinished);
            assertEquals(expected, actual[0]);
        }
    }

    @Test
    void whenChannelNotOpenedThenWriteWithHandlerShouldDoNothing()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        final ByteBuffer buffer = ByteBuffer.allocate(10);
        final ByteBuffer attachment = ByteBuffer.allocate(10);
        final CompletionHandler<Integer, ByteBuffer> handler = new CompletionHandler<Integer, ByteBuffer>()
        {
            @Override
            public void completed(Integer bytesWritten,
                                  ByteBuffer attachment)
            {
            }

            @Override
            public void failed(Throwable exc,
                               ByteBuffer attachment)
            {
            }
        };

        //when
        final S3FileChannel channel = spy(new S3FileChannel(file1, writeOption, null, true));
        channel.close();
        channel.write(buffer, 0, attachment, handler);

        //then
        assertFalse(channel.isOpen());
    }

    @Test
    void constructorWriteButTryToRead()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(10);

        //when
        final S3FileChannel channel = new S3FileChannel(file1, writeOption, null, true);

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NonReadableChannelException.class, () -> channel.read(byteBuffer, 0));

        //then
        assertNotNull(exception);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true,
                              false })
    void readNeedsToCloseChannel(final boolean tempFileRequired)
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        final S3FileChannel channel = spy(new S3FileChannel(file1, readOption, null, tempFileRequired));
        channel.close();

        //then
        verify(channel, times(1)).close();
        verify(client, never()).putObject(isA(PutObjectRequest.class), isA(RequestBody.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true,
                              false })
    void writeNeedsToCloseChannel(final boolean tempFileRequired)
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final String content = "hello";
        final ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        //when
        final S3FileChannel channel = spy(new S3FileChannel(file1, writeOption, null, tempFileRequired));
        channel.write(buffer, 0);
        channel.close();

        //then
        verify(channel, times(1)).close();
        verify(client, times(1)).putObject(isA(PutObjectRequest.class), isA(RequestBody.class));
    }

    @Test
    void alreadyExists()
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(FileAlreadyExistsException.class,
                                                 () -> new S3FileChannel(file1, options, null, true));

        //then
        assertNotNull(exception);
    }

    @Test
    void brokenNetwork()
    {
        //given
        final GetObjectRequest request = GetObjectRequest.builder().bucket("buck").key("file2").build();
        doThrow(new RuntimeException("network broken")).when(client).getObject(request);

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file2 = (S3Path) fileSystem.getPath("/buck/file2");

        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(RuntimeException.class,
                                                 () -> new S3FileChannel(file2, readOption, null, true));

        //then
        assertNotNull(exception);
    }

    @Test
    void shouldNotCreateChannelWithWriteWhenTargetDoesNotExist()
            throws SecurityException, IllegalArgumentException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file2 = (S3Path) fileSystem.getPath("/buck/file2");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NoSuchFileException.class,
                                                 () -> new S3FileChannel(file2, writeOption, null, true));

        //then
        assertNotNull(exception);
    }

    @Test
    void shouldRemoveTempFileWhenRequiredButTargetDoesNotExist()
            throws SecurityException, IllegalArgumentException, IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file2 = (S3Path) fileSystem.getPath("/buck/file2");
        final EnumSet<StandardOpenOption> createNewOption = EnumSet.of(StandardOpenOption.CREATE_NEW);

        //when
        final S3FileChannel channel = spy(new S3FileChannel(file2, createNewOption, null, true));
        channel.close();

        //then
        verify(channel, times(1)).close();
        verify(client, never()).putObject(isA(PutObjectRequest.class), isA(RequestBody.class));
    }

    @Test
    void lockAcquiresALockOfChannelFile()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        final S3FileChannel channel = new S3FileChannel(file1, readOption, null, false);
        Future<FileLock> lock = channel.lock(0, Long.MAX_VALUE, true);

        final FileLock fileLock = lock.get(1, TimeUnit.SECONDS);
        assertTrue(lock.isDone());

        final ByteBuffer buffer = ByteBuffer.allocate(20);
        Future<Integer> noOfBytesRead = channel.read(buffer, 0);

        while (noOfBytesRead.get(1, TimeUnit.SECONDS) != -1)
        {

            buffer.flip();
            System.out.print("    ");

            while (buffer.hasRemaining())
            {
                System.out.print((char) buffer.get());
            }

            System.out.println(" ");

            buffer.clear();
            noOfBytesRead = channel.read(buffer, noOfBytesRead.get(1, TimeUnit.SECONDS));
        }

        //then
        assertTrue(fileLock.isValid());
        assertTrue(fileLock.isShared());

        channel.close(); // also releases the lock

        assertFalse(fileLock.isValid());
    }

    @Test
    void lockWithHandlerAcquiresALockOfChannelFile()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        final ByteBuffer attachment = ByteBuffer.allocate(10);
        final FileLock[] fileLock = { null };
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final CompletionHandler<FileLock, ByteBuffer> handler = new CompletionHandler<FileLock, ByteBuffer>()
        {
            @Override
            public void completed(FileLock fileLockResult,
                                  ByteBuffer attachment)
            {
                fileLock[0] = fileLockResult;
                System.out.println("resulting FileLock: " + fileLockResult);
                countDownLatch.countDown();
            }

            @Override
            public void failed(Throwable exc,
                               ByteBuffer attachment)
            {
                System.out.println("Lock failed");
                exc.printStackTrace();
            }
        };

        //when
        final S3FileChannel channel = new S3FileChannel(file1, readOption, null, false);
        channel.lock(0, Long.MAX_VALUE, true, attachment, handler);
        final boolean awaitFinished = countDownLatch.await(1, TimeUnit.SECONDS);

        final ByteBuffer buffer = ByteBuffer.allocate(20);
        Future<Integer> noOfBytesRead = channel.read(buffer, 0);

        while (noOfBytesRead.get(1, TimeUnit.SECONDS) != -1)
        {

            buffer.flip();
            System.out.print("    ");

            while (buffer.hasRemaining())
            {
                System.out.print((char) buffer.get());
            }

            System.out.println(" ");

            buffer.clear();
            noOfBytesRead = channel.read(buffer, noOfBytesRead.get(1, TimeUnit.SECONDS));
        }

        //then
        assertTrue(awaitFinished);
        assertNotNull(fileLock[0]);
        assertTrue(fileLock[0].isValid());
        assertTrue(fileLock[0].isShared());

        channel.close(); // also releases the lock

        assertFalse(fileLock[0].isValid());
    }

    @Test
    void tryLockOverAnAcquiredLockThrowsAnException()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        final S3FileChannel channel = new S3FileChannel(file1, readOption, null, false);
        Future<FileLock> lock = channel.lock(0, Long.MAX_VALUE, true);

        final FileLock fileLock = lock.get(1, TimeUnit.SECONDS);
        assertTrue(lock.isDone());

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(OverlappingFileLockException.class,
                                                 () -> channel.tryLock(0, Long.MAX_VALUE, true));

        //then
        assertNotNull(exception);

        channel.close(); // also releases the lock

        assertFalse(fileLock.isValid());
    }

    @Test
    void tryLockShouldAcquireLockOverANotAcquiredLock()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        final S3FileChannel channel = new S3FileChannel(file1, readOption, null, false);
        final FileLock fileLock = channel.tryLock(0, Long.MAX_VALUE, true);

        final ByteBuffer buffer = ByteBuffer.allocate(20);
        Future<Integer> noOfBytesRead = channel.read(buffer, 0);

        while (noOfBytesRead.get(1, TimeUnit.SECONDS) != -1)
        {

            buffer.flip();
            System.out.print("    ");

            while (buffer.hasRemaining())
            {
                System.out.print((char) buffer.get());
            }

            System.out.println(" ");

            buffer.clear();
            noOfBytesRead = channel.read(buffer, noOfBytesRead.get(1, TimeUnit.SECONDS));
        }

        //then
        assertTrue(fileLock.isValid());
        assertTrue(fileLock.isShared());

        channel.close(); // also releases the lock

        assertFalse(fileLock.isValid());
    }

    @Test
    void isOpenShouldReturnTrueWhenChannelIsOpened()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> createOption = EnumSet.of(StandardOpenOption.CREATE);

        //when
        final boolean actual;
        try (final S3FileChannel channel = new S3FileChannel(file1, createOption, null, true))
        {
            actual = channel.isOpen();
        }

        //then
        assertTrue(actual);
    }

    @Test
    void isOpenShouldReturnFalseWhenChannelIsClosed()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> createOption = EnumSet.of(StandardOpenOption.CREATE);

        //when
        final S3FileChannel channel = new S3FileChannel(file1, createOption, null, true);
        channel.close();
        final boolean actual = channel.isOpen();

        //then
        assertFalse(actual);
    }

    @Test
    void sizeShouldReturnChannelFileSizeInBytes()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final long expected = Files.size(file1);
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        //when
        final long actual;
        try (final S3FileChannel channel = new S3FileChannel(file1, writeOption, null, false))
        {
            actual = channel.size();
        }

        //then
        assertEquals(expected, actual);
    }

    @Test
    void truncateShouldSetChannelFileToGivenSize()
            throws IOException
    {
        //given
        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file1 = (S3Path) fileSystem.getPath("/buck/file1");
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);
        long expected = 1;

        //when
        final long actual;
        try (final S3FileChannel channel = new S3FileChannel(file1, writeOption, null, false))
        {
            channel.truncate(expected);
            actual = channel.size();
        }

        //then
        assertEquals(expected, actual);
    }

}
