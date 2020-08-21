package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;

import org.junit.Before;
import org.junit.Test;

public class SetAttributeTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() throws IOException {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }


    @Test(expected = UnsupportedOperationException.class)
    public void readAttributesObject() {
        s3fsProvider.setAttribute(null, "", new Object());
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
     * @throws IOException
     */
    private S3FileSystem createNewS3FileSystem() throws IOException {
        try {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        } catch (FileSystemNotFoundException e) {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }

    }
}
