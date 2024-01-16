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

    private static final String bucket = EnvironmentBuilder.getBucket();

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    @Test
    void newByteChannelCreateAndWrite()
            throws IOException
    {
        final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);
        final String content = "sample content";
        Path file = uploadDir().resolve("file");

        try (SeekableByteChannel seek = provisionedFileSystem.provider().newByteChannel(file,
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
        final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);
        final String content = "sample content";
        Path file = uploadSingleFile("");

        try (SeekableByteChannel seek = provisionedFileSystem.provider().newByteChannel(file,
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
        final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);
        final String content = "sample content";

        Path file = uploadSingleFile("");

        try (SeekableByteChannel seek = provisionedFileSystem.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.READ)))
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
        final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);

        final String content = "sample content";

        Path file = uploadSingleFile(content);

        ByteBuffer bufferRead = ByteBuffer.allocate(content.length());

        try (SeekableByteChannel seek = provisionedFileSystem.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE,
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
        final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);

        final String content = "sample content";
        Path file = uploadSingleFile(content);

        try (SeekableByteChannel seek = provisionedFileSystem.provider().newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE)))
        {
            int length = content.length();
            ByteBuffer byteBuffer = ByteBuffer.allocate(length);
            Exception exception = assertThrows(NonReadableChannelException.class,
                                               () -> seek.read(byteBuffer));

            assertNotNull(exception);
        }
    }

    public Path uploadSingleFile(String content)
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build(getTestBasePathWithUUID().replaceAll("/", "_")))
        {
            Path file = Files.createFile(linux.getPath(UUID.randomUUID().toString()));

            Files.write(file, content.getBytes());

            final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);
            Path result = provisionedFileSystem.getPath(bucket, getTestBasePathWithUUID());

            Files.copy(file, result);

            return result;
        }
    }

    private Path uploadDir()
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build(getTestBasePathWithUUID().replaceAll("/", "_")))
        {
            Path assets = Files.createDirectories(linux.getPath("/upload/assets1"));
            final FileSystem provisionedFileSystem = provisionFilesystem(uriGlobal);
            Path dir = provisionedFileSystem.getPath(bucket, getTestBasePathWithUUID() + "/");

            Files.walkFileTree(assets.getParent(), new CopyDirVisitor(assets.getParent(), dir));

            return dir;
        }
    }

}
