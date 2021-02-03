package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.IOUtils;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class NewInputStreamTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    void inputStreamFile()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1", "content".getBytes());

        Path file = createNewS3FileSystem().getPath("/bucketA/file1");
        try (InputStream inputStream = s3fsProvider.newInputStream(file))
        {
            byte[] buffer = IOUtils.toByteArray(inputStream);

            // check
            assertArrayEquals("content".getBytes(), buffer);
        }
    }

    @Test
    void newInputStreamFileNotExists()
            throws IOException
    {
        // fixtures
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir");

        // act
        final S3FileSystem fileSystem = createNewS3FileSystem();

        final Path file = fileSystem.getPath("/bucketA/dir/file1");

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(NoSuchFileException.class, () -> {
            try (InputStream ignored = s3fsProvider.newInputStream(file))
            {
                fail("The file does not exist");
            }
        });

        assertNotNull(exception);
    }

    @Test
    void inputStreamDirectory()
            throws IOException
    {
        // fixtures
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir");

        final URI uri = URI.create("s3://endpoint1/");
        final Map<String, ?> envMap = buildFakeEnv();
        final FileSystem fileSystem = s3fsProvider.newFileSystem(uri, envMap);
        Path result = fileSystem.getPath("/bucketA/dir");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IOException.class, () -> s3fsProvider.newInputStream(result));

        assertNotNull(exception);
    }

    private Map<String, ?> buildFakeEnv()
    {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access-key")
                                                     .put(SECRET_KEY, "secret-key")
                                                     .build();
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
