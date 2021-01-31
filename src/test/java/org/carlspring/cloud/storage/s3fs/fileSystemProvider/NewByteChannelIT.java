package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.CopyDirVisitor;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.UUID;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@S3IntegrationTest
class NewByteChannelIT
        extends S3UnitTestBase
{

    private static final String bucket = EnvironmentBuilder.getEnvironmentConfiguration().getBucketName();

    // TODO: Replace (with BaseIntegrationTest:getGlobalUrl) or delete when https://github.com/carlspring/s3fs-nio/issues/187 is closed.
    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private FileSystem fileSystemAmazon;


    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);

        fileSystemAmazon = build();
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
    void newByteChannelCreateAndWrite()
            throws IOException
    {
        final String content = "sample content";
        Path file = uploadDir().resolve("file");

        try (SeekableByteChannel seek = fileSystemAmazon.provider().newByteChannel(file,
                                                                                   EnumSet.of(StandardOpenOption.WRITE,
                                                                                              StandardOpenOption.READ,
                                                                                              StandardOpenOption.CREATE_NEW)))
        {
            ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
            seek.write(buffer);
        }

        assertTrue(Files.exists(file));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    void newByteChannelWrite()
            throws IOException
    {
        final String content = "sample content";
        Path file = uploadSingleFile("");

        try (SeekableByteChannel seek = fileSystemAmazon.provider().newByteChannel(file,
                                                                                   EnumSet.of(StandardOpenOption.WRITE,
                                                                                              StandardOpenOption.READ)))
        {
            seek.write(ByteBuffer.wrap(content.getBytes()));
        }

        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    void newByteChannelWriteWithoutPermission()
            throws IOException
    {
        final String content = "sample content";

        Path file = uploadSingleFile("");

        try (SeekableByteChannel seek = fileSystemAmazon.provider().newByteChannel(file,
                                                                                   EnumSet.of(StandardOpenOption.READ)))
        {
            byte[] bytes = content.getBytes();
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            Exception exception = assertThrows(NonWritableChannelException.class,
                                               () -> seek.write(byteBuffer));

            assertNotNull(exception);
        }
    }

    @Test
    void newByteChannelRead()
            throws IOException
    {
        final String content = "sample content";

        Path file = uploadSingleFile(content);

        ByteBuffer bufferRead = ByteBuffer.allocate(content.length());

        try (SeekableByteChannel seek = fileSystemAmazon.provider().newByteChannel(file,
                                                                                   EnumSet.of(StandardOpenOption.WRITE,
                                                                                              StandardOpenOption.READ)))
        {
            seek.read(bufferRead);
        }

        assertArrayEquals(new String(bufferRead.array()).getBytes(), Files.readAllBytes(file));
        assertArrayEquals(new String(bufferRead.array()).getBytes(), content.getBytes());
    }

    @Test
    void newByteChannelReadWithoutPermission()
            throws IOException
    {
        final String content = "sample content";
        Path file = uploadSingleFile(content);

        try (SeekableByteChannel seek = fileSystemAmazon.provider().newByteChannel(file,
                                                                                   EnumSet.of(
                                                                                           StandardOpenOption.WRITE)))
        {
            int length = content.length();
            ByteBuffer byteBuffer = ByteBuffer.allocate(length);
            Exception exception = assertThrows(NonReadableChannelException.class,
                                               () -> seek.read(byteBuffer));

            assertNotNull(exception);
        }
    }

    private Path uploadSingleFile(String content)
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path file = Files.createFile(linux.getPath(UUID.randomUUID().toString()));

            Files.write(file, content.getBytes());

            Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

            Files.copy(file, result);

            return result;
        }
    }

    private Path uploadDir()
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path assets = Files.createDirectories(linux.getPath("/upload/assets1"));
            Path dir = fileSystemAmazon.getPath(bucket, "0000example" + UUID.randomUUID().toString() + "/");

            Files.walkFileTree(assets.getParent(), new CopyDirVisitor(assets.getParent(), dir));

            return dir;
        }
    }

}
