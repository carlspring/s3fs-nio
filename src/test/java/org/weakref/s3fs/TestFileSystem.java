package org.weakref.s3fs;

import com.amazonaws.services.s3.AmazonS3Client;
import org.testng.annotations.Test;

import java.nio.file.FileSystem;

import static org.testng.Assert.assertEquals;
import static org.weakref.s3fs.S3Path.forPath;

public class TestFileSystem
{
    @Test
    public void testGetPath()
    {
        FileSystem fs = new S3FileSystem(new S3FileSystemProvider(), new AmazonS3Client());

        assertEquals(fs.getPath("/bucket/path/to/file"), forPath("/bucket/path/to/file"));
        assertEquals(fs.getPath("/bucket", "path", "to", "file"), forPath("/bucket/path/to/file"));
        assertEquals(fs.getPath("bucket", "path", "to", "file"), forPath("/bucket/path/to/file"));
        assertEquals(fs.getPath("bucket", "path", "to", "dir/"), forPath("/bucket/path/to/dir/"));
        assertEquals(fs.getPath("bucket", "path/", "to/", "dir/"), forPath("/bucket/path/to/dir/"));
    }
}
