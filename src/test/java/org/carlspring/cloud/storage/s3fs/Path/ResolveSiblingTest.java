package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.FileSystem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ResolveSiblingTest
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
        s3fsProvider.close((S3FileSystem) fileSystem);
        fileSystem.close();
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    public void resolveSibling()
    {
        // absolute (non-root) vs...
        assertEquals(getPath("/bucket/path/to/file").resolveSibling(getPath("other/child")),
                     getPath("/bucket/path/to/other/child"));
        assertEquals(getPath("/bucket/path/to/file").resolveSibling(getPath("/bucket2/other/child")),
                     getPath("/bucket2/other/child"));
        assertEquals(getPath("/bucket/path/to/file").resolveSibling(getPath("")), getPath("/bucket/path/to/"));

        // absolute (root) vs ...
        assertEquals(getPath("/bucket").resolveSibling(getPath("other/child")), getPath("other/child"));
        assertEquals(getPath("/bucket").resolveSibling(getPath("/bucket2/other/child")),
                     getPath("/bucket2/other/child"));
        assertEquals(getPath("/bucket").resolveSibling(getPath("")), getPath(""));

        // relative (empty) vs ...
        assertEquals(getPath("").resolveSibling(getPath("other/child")), getPath("other/child"));
        assertEquals(getPath("").resolveSibling(getPath("/bucket2/other/child")), getPath("/bucket2/other/child"));
        assertEquals(getPath("").resolveSibling(getPath("")), getPath(""));

        // relative (non-empty) vs ...
        assertEquals(getPath("path/to/file").resolveSibling(getPath("other/child")), getPath("path/to/other/child"));
        assertEquals(getPath("path/to/file").resolveSibling(getPath("/bucket2/other/child")),
                     getPath("/bucket2/other/child"));
        assertEquals(getPath("path/to/file").resolveSibling(getPath("")), getPath("path/to/"));
    }

    @Test
    public void resolveSiblingString()
    {
        // absolute (non-root) vs...
        assertEquals(getPath("/bucket/path/to/file").resolveSibling("other/child"),
                     getPath("/bucket/path/to/other/child"));
        assertEquals(getPath("/bucket/path/to/file").resolveSibling("/bucket2/other/child"),
                     getPath("/bucket2/other/child"));
        assertEquals(getPath("/bucket/path/to/file").resolveSibling(""), getPath("/bucket/path/to/"));

        // absolute (root) vs ...
        assertEquals(getPath("/bucket").resolveSibling("other/child"), getPath("other/child"));
        assertEquals(getPath("/bucket").resolveSibling("/bucket2/other/child"), getPath("/bucket2/other/child"));
        assertEquals(getPath("/bucket").resolveSibling(""), getPath(""));

        // relative (empty) vs ...
        assertEquals(getPath("").resolveSibling("other/child"), getPath("other/child"));
        assertEquals(getPath("").resolveSibling("/bucket2/other/child"), getPath("/bucket2/other/child"));
        assertEquals(getPath("").resolveSibling(""), getPath(""));

        // relative (non-empty) vs ...
        assertEquals(getPath("path/to/file").resolveSibling("other/child"), getPath("path/to/other/child"));
        assertEquals(getPath("path/to/file").resolveSibling("/bucket2/other/child"), getPath("/bucket2/other/child"));
        assertEquals(getPath("path/to/file").resolveSibling(""), getPath("path/to/"));
    }

}
