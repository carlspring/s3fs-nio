package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NewOutputStreamTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        super.tearDown();

    }

    @Test
    void outputStreamFileExists()
            throws IOException
    {
        final Path base = getS3Directory();

        final Path file = base.resolve("file1");
        Files.createFile(file);
        final String content = "sample content";

        try (final OutputStream stream = s3fsProvider.newOutputStream(file))
        {
            stream.write(content.getBytes());
            stream.flush();
        }

        // get the input
        final byte[] buffer = Files.readAllBytes(file);

        // check
        assertArrayEquals(content.getBytes(), buffer);
    }

    @Test
    void outputStreamFileNotExists()
            throws IOException
    {
        final Path base = getS3Directory();

        final Path file = base.resolve("file1");
        final String content = "sample content";

        try (final OutputStream stream = s3fsProvider.newOutputStream(file))
        {
            stream.write(content.getBytes());
            stream.flush();
        }

        // get the input
        final byte[] buffer = Files.readAllBytes(file);

        // check
        assertArrayEquals(content.getBytes(), buffer);
    }

    @Test
    void outputStreamWithCreateNew()
            throws IOException
    {
        Path base = getS3Directory();

        Path file = base.resolve("file1");
        final String content = "sample content";

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW))
        {
            stream.write(content.getBytes());
            stream.flush();
        }

        // get the input
        byte[] buffer = Files.readAllBytes(file);

        // check
        assertArrayEquals(content.getBytes(), buffer);
    }

    @Test
    void outputStreamWithTruncate()
            throws IOException
    {
        String initialContent = "Content line 1\n" + "Content line 2\n" + "Content line 3\n" + "Content line 4";

        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1", initialContent.getBytes());

        Path file = createNewS3FileSystem().getPath("/bucketA/file1");

        String res = "only one line";

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING))
        {
            stream.write(res.getBytes());
            stream.flush();
        }

        // get the input
        byte[] buffer = Files.readAllBytes(file);

        // check
        assertArrayEquals(res.getBytes(), buffer);
    }

    @Test
    void outputStreamWithCreateNewAndFileExists()
            throws IOException
    {
        final Path base = getS3Directory();
        final Path file = Files.createFile(base.resolve("file1"));

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(FileAlreadyExistsException.class,
                                                 () -> s3fsProvider.newOutputStream(file,
                                                                                    StandardOpenOption.CREATE_NEW));

        assertNotNull(exception);
    }

    @Test
    void outputStreamWithCreateAndFileExists()
            throws IOException
    {
        final Path base = getS3Directory();
        final Path file = Files.createFile(base.resolve("file1"));

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(FileAlreadyExistsException.class,
                                                 () -> s3fsProvider.newOutputStream(file,
                                                                                    StandardOpenOption.CREATE));

        assertNotNull(exception);
    }

    @Test
    void outputStreamWithCreateAndFileNotExists()
            throws IOException
    {
        Path base = getS3Directory();

        Path file = base.resolve("file1");

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE))
        {
            stream.write("sample content".getBytes());
            stream.flush();
        }

        // get the input
        byte[] buffer = Files.readAllBytes(file);

        // check
        assertArrayEquals("sample content".getBytes(), buffer);
    }

    @Test
    void anotherOutputStream()
            throws IOException
    {
        Path base = getS3Directory();
        Path file = base.resolve("file1");

        final String content = "heyyyyyy";

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW))
        {
            stream.write(content.getBytes());
            stream.flush();
        }

        // get the input
        byte[] buffer = Files.readAllBytes(file);

        // check
        assertArrayEquals(content.getBytes(), buffer);
    }

    private Path getS3Directory()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir");

        final URI uri = URI.create("s3://endpoint1/");
        final Map<String, Object> envMap = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access_key")
                                                                                 .put(SECRET_KEY, "secret_key")
                                                                                 .build();
        final FileSystem fileSystem = s3fsProvider.newFileSystem(uri, envMap);
        return fileSystem.getPath("/bucketA/dir");
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
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
