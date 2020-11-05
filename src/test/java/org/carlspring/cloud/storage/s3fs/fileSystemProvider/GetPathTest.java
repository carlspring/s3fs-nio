package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GetPathTest
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
    void getPathWithEmptyEndpoint()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
        Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

        assertEquals(path, fs.getPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }

    @Test
    void getPath()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), null);
        Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

        assertEquals(path, fs.getPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }

    @Test
    void getAnotherPath()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), ImmutableMap.<String, Object>of());
        Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

        assertEquals(path, fs.getPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }

    @Test
    void getPathWithEndpointAndWithoutBucket()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), null);
        URI uri = URI.create("s3://endpoint1//missed-bucket");
        FileSystemProvider provider = fs.provider();

        Exception exception = assertThrows(IllegalArgumentException.class,
                                           () -> provider.getPath(uri));

        assertNotNull(exception);
    }

    @Test
    void getPathWithDefaultEndpointAndWithoutBucket()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
        FileSystemProvider provider = fs.provider();
        URI uri = URI.create("s3:////missed-bucket");

        Exception exception = assertThrows(IllegalArgumentException.class,
                                           () -> provider.getPath(uri));

        assertNotNull(exception);
    }

}
