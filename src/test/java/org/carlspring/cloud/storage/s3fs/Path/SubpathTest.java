package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;

public class SubpathTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    private S3Path getPath(String path) {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void subPath0() {
        assertEquals(getPath("/bucket/path/"), getPath("/bucket/path/to/file").subpath(0, 1));
    }

    @Test
    public void subPath() {
        assertEquals(getPath("/bucket/path/to/"), getPath("/bucket/path/to/file").subpath(0, 2));
        assertEquals(getPath("/bucket/path/to/file"), getPath("/bucket/path/to/file").subpath(0, 3));
        assertEquals(getPath("to/"), getPath("/bucket/path/to/file").subpath(1, 2));
        assertEquals(getPath("to/file"), getPath("/bucket/path/to/file").subpath(1, 3));
        assertEquals(getPath("file"), getPath("/bucket/path/to/file").subpath(2, 3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void subPathOutOfRange() {
        getPath("/bucket/path/to/file").subpath(0, 4);
    }
}
