package com.upplication.s3fs.Path;

import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.S3EndpointConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;

import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetRootTest extends S3UnitTestBase {

    private static S3Path getPath(String path) {
        return (S3Path) FileSystems.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Before
    public void setup() throws IOException {
        FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void getRootReturnBucket() {
        assertEquals(getPath("/bucketA/"), getPath("/bucketA/dir/file").getRoot());
    }

    @Test
    public void getRootRelativeReturnNull() {
        assertNull(getPath("dir/file").getRoot());
    }

}
