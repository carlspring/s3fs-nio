package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetFileNameTest
        extends S3UnitTestBase
{

    private S3FileSystemProvider s3fsProvider;

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void getFileName()
    {
        Path path = getPath("/bucketA/file");
        Path name = path.getFileName();

        assertEquals(getPath("file"), name);
    }

    @Test
    public void getAnotherFileName()
    {
        Path path = getPath("/bucketA/dir/another-file");
        Path fileName = path.getFileName();
        Path dirName = path.getParent().getFileName();

        assertEquals(getPath("another-file"), fileName);
        assertEquals(getPath("dir"), dirName);
    }

    @Test
    public void getFileNameBucket()
    {
        Path path = getPath("/bucket");
        Path name = path.getFileName();

        assertNull(name);
    }

}
