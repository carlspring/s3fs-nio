package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class GetPathTest
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
    public void getPathWithEmptyEndpoint()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
        Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

        assertEquals(path, fs.getPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }

    @Test
    public void getPath()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), null);
        Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

        assertEquals(path, fs.getPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }

    @Test
    public void getAnotherPath()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), ImmutableMap.<String, Object>of());
        Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

        assertEquals(path, fs.getPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }

    @Test
    public void getPathWithEndpointAndWithoutBucket()
    {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), null);
            fs.provider().getPath(URI.create("s3://endpoint1//missed-bucket"));
        });

        assertNotNull(exception);
    }

    @Test
    public void getPathWithDefaultEndpointAndWithoutBucket()
    {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
            fs.provider().getPath(URI.create("s3:////missed-bucket"));
        });

        assertNotNull(exception);
    }

}
