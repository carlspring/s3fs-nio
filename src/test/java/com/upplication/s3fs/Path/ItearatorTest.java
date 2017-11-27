package com.upplication.s3fs.Path;

import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Iterator;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;

public class ItearatorTest extends S3UnitTestBase {

    private static S3Path getPath(String path) {
        return (S3Path) FileSystems.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        FileSystems
                .newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void iterator() {
        Iterator<Path> iterator = getPath("/bucket/path/to/file").iterator();

        assertEquals(getPath("/bucket/"), iterator.next());
        assertEquals(getPath("path/"), iterator.next());
        assertEquals(getPath("to/"), iterator.next());
        assertEquals(getPath("file"), iterator.next());
    }


}
