package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.*;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.*;

public class NewOutputStreamTest
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
    public void outputStreamWithCreateNew()
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
    public void outputStreamWithTruncate()
            throws IOException
    {
        String initialContent = "Content line 1\n" + "Content line 2\n" + "Content line 3\n" + "Content line 4";

        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
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
    public void outputStreamWithCreateNewAndFileExists()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> {
            Path base = getS3Directory();
            Path file = Files.createFile(base.resolve("file1"));

            s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW);
        });

        assertNotNull(exception);
    }

    @Test
    public void outputStreamWithCreateAndFileExists()
            throws IOException
    {
        Path base = getS3Directory();

        Path file = base.resolve("file1");
        Files.createFile(file);

        final String content = "sample content";

        try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE))
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
    public void outputStreamWithCreateAndFileNotExists()
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
    public void anotherOutputStream()
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
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        return s3fsProvider.newFileSystem(URI.create("s3://endpoint1/"),
                                          ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access_key")
                                                                                .put(SECRET_KEY, "secret_key")
                                                                                .build())
                           .getPath("/bucketA/dir");
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
