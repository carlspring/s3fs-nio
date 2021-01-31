package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.CopyDirVisitor;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@S3IntegrationTest
class NewAsynchronousFileChannelTestIT
        extends S3UnitTestBase
{

    private static final String bucket = EnvironmentBuilder.getEnvironmentConfiguration().getBucketName();

    // TODO: Replace (with BaseIntegrationTest:getGlobalUrl) or delete when https://github.com/carlspring/s3fs-nio/issues/187 is closed.
    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private FileSystem fileSystemAmazon;

    private FileSystemProvider provider;

    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);

        fileSystemAmazon = build();
        provider = fileSystemAmazon.provider();
    }

    private static FileSystem build()
            throws IOException
    {
        try
        {
            FileSystems.getFileSystem(uriGlobal).close();

            return createNewFileSystem();
        }
        catch (FileSystemNotFoundException e)
        {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem()
            throws IOException
    {
        return FileSystems.newFileSystem(uriGlobal, EnvironmentBuilder.getEnvironmentConfiguration().asMap());
    }

    @Test
    void newAsynchronousFileChannelCreateAndWrite()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final String content = "sample content";
        final ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
        final Path file = uploadDirectory().resolve("file");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE,
                                                               StandardOpenOption.READ,
                                                               StandardOpenOption.CREATE_NEW);

        //when
        try (AsynchronousFileChannel fileChannel = provider.newAsynchronousFileChannel(file, options, null))
        {
            final Future<Integer> writeResult = fileChannel.write(buffer, 0);

            //then
            writeResult.get(1, TimeUnit.SECONDS);
            assertTrue(writeResult.isDone());

            assertArrayEquals(content.getBytes(), new String(buffer.array()).getBytes());
        }
    }

    @Test
    void newAsynchronousFileChannelWrite()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        //given
        final String content = "sample content";
        final ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
        final Path file = uploadSingleFile("");
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = provider.newAsynchronousFileChannel(file, options, null))
        {
            final Future<Integer> writeResult = fileChannel.write(buffer, 0);

            //then
            writeResult.get(1, TimeUnit.SECONDS);
            assertTrue(writeResult.isDone());

            assertArrayEquals(content.getBytes(), new String(buffer.array()).getBytes());
        }
    }

    @Test
    void newAsynchronousFileChannelWriteWithoutPermission()
            throws IOException
    {
        //given
        final String content = "sample content";
        final byte[] bytes = content.getBytes();
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        final Path file = uploadSingleFile("");
        final EnumSet<StandardOpenOption> readOption = EnumSet.of(StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = provider.newAsynchronousFileChannel(file, readOption, null))
        {

            final Exception exception = assertThrows(NonWritableChannelException.class,
                                                     () -> fileChannel.write(byteBuffer, 0));

            //then
            assertNotNull(exception);
        }
    }

    @Test
    void newAsynchronousFileChannelRead()
            throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        //given
        final String content = "sample content";
        final ByteBuffer bufferRead = ByteBuffer.allocate(content.length());
        final Path file = uploadSingleFile(content);
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE,
                                                               StandardOpenOption.READ);

        //when
        try (AsynchronousFileChannel fileChannel = provider.newAsynchronousFileChannel(file, options, null))
        {
            final Future<Integer> readResult = fileChannel.read(bufferRead, 0);
            readResult.get(1, TimeUnit.SECONDS);
            assertTrue(readResult.isDone());
        }

        //then
        assertArrayEquals(new String(bufferRead.array()).getBytes(), Files.readAllBytes(file));
        assertArrayEquals(new String(bufferRead.array()).getBytes(), content.getBytes());
    }

    @Test
    void newAsynchronousFileChannelReadWithoutPermission()
            throws IOException
    {
        //given
        final String content = "sample content";
        final int length = content.length();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        final Path file = uploadSingleFile(content);
        final EnumSet<StandardOpenOption> writeOption = EnumSet.of(StandardOpenOption.WRITE);

        //then
        try (AsynchronousFileChannel fileChannel = provider.newAsynchronousFileChannel(file, writeOption, null))
        {
            final Exception exception = assertThrows(NonReadableChannelException.class,
                                                     () -> fileChannel.read(byteBuffer, 0));

            //then
            assertNotNull(exception);
        }
    }

    private Path uploadSingleFile(String content)
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            final Path file = Files.createFile(linux.getPath(UUID.randomUUID().toString()));

            Files.write(file, content.getBytes());

            final Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

            Files.copy(file, result);

            return result;
        }
    }

    private Path uploadDirectory()
            throws IOException
    {
        try (final FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            final Path assets = Files.createDirectories(linux.getPath("/upload/assets1"));
            final Path dir = fileSystemAmazon.getPath(bucket, "0000example" + UUID.randomUUID().toString() + "/");

            Files.walkFileTree(assets.getParent(), new CopyDirVisitor(assets.getParent(), dir));

            return dir;
        }
    }

}
