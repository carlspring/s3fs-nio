package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class IteratorTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    public void iterator()
    {
        Iterator<Path> iterator = getPath("/bucket/path/to/file").iterator();

        assertEquals(getPath("/bucket/"), iterator.next());
        assertEquals(getPath("path/"), iterator.next());
        assertEquals(getPath("to/"), iterator.next());
        assertEquals(getPath("file"), iterator.next());
    }

    @Test
    public void iteratorEmtpy()
    {
        Iterator<Path> iterator = getPath("").iterator();
        assertFalse(iterator.hasNext());
    }

}
