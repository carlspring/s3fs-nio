package org.carlspring.cloud.storage.s3fs.path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResolveTest
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
    void resolve()
    {
        assertEquals(getPath("/bucket/path/to/dir/child/xyz"),
                     getPath("/bucket/path/to/dir/").resolve(getPath("child/xyz")));
        assertEquals(getPath("/bucket/path/to/dir/child/xyz"), getPath("/bucket/path/to/dir/").resolve("child/xyz"));

        assertEquals(getPath("/bucket/path/to/dir/child/xyz"),
                     getPath("/bucket/path/to/dir").resolve(getPath("child/xyz")));
        assertEquals(getPath("/bucket/path/to/dir/child/xyz"), getPath("/bucket/path/to/dir").resolve("child/xyz"));

        assertEquals(getPath("/bucket/path/to/file"), getPath("/bucket/path/to/file").resolve(getPath("")));
        assertEquals(getPath("/bucket/path/to/file"), getPath("/bucket/path/to/file").resolve(""));

        assertEquals(getPath("path/to/file/child/xyz"), getPath("path/to/file").resolve(getPath("child/xyz")));
        assertEquals(getPath("path/to/file/child/xyz"), getPath("path/to/file").resolve("child/xyz"));

        assertEquals(getPath("path/to/file"), getPath("path/to/file").resolve(getPath("")));
        assertEquals(getPath("path/to/file"), getPath("path/to/file").resolve(""));

        assertEquals(getPath("/bucket2/other/child"),
                     getPath("/bucket/path/to/file").resolve(getPath("/bucket2/other/child")));
        assertEquals(getPath("/bucket2/other/child"), getPath("/bucket/path/to/file").resolve("/bucket2/other/child"));
    }


    @Test
    void nonS3Paths()
    {
        S3Path parent = getPath("/bucket");
        S3Path child = getPath("/bucket/rabbit");
        S3Path resolved = (S3Path) parent.resolve(child);

        assertEquals(child, resolved);

        resolved = (S3Path) parent.resolve("rabbit");
        assertEquals(child, resolved);

        resolved = (S3Path) parent.resolve(Paths.get("rabbit")); //unixPath
        assertEquals(child, resolved);

        resolved = (S3Path) parent.resolve(Paths.get("./rabbit")); //unixPath
        assertEquals("s3://s3.test.amazonaws.com/bucket/./rabbit", resolved.toString());

        resolved = (S3Path) parent.resolve(Paths.get("./rabbit in space")); //unixPath
        assertEquals("s3://s3.test.amazonaws.com/bucket/./rabbit%20in%20space", resolved.toString());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                parent.resolve(Paths.get("/tmp")));
        assertEquals("other must be an instance of org.carlspring.cloud.storage.s3fs.S3Path or a relative Path", e.getMessage());
    }
}
