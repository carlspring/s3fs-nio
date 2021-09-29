package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.MockBucket;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NewDirectoryStreamTest
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
    void createStreamDirectoryReader()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1", "file2");

        // act
        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        // assert
        assertNewDirectoryStream(bucket, "file1", "file2");
    }

    @Test
    void createWithDirStreamDirectoryReader()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir1").file("file1");

        // act
        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        // assert
        assertNewDirectoryStream(bucket, "file1", "dir1");
    }

    @Test
    void createStreamDirectoryFromDirectoryReader()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir", "dir/file2").file("dir/file1");

        // act
        Path dir = createNewS3FileSystem().getPath("/bucketA", "dir");

        // assert
        assertNewDirectoryStream(dir, "file1", "file2");
    }

    @Test
    void removeIteratorStreamDirectoryReader()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir1").file("dir1/file1", "content".getBytes());

        // act
        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        try (DirectoryStream<Path> dir = Files.newDirectoryStream(bucket))
        {
            Iterator<Path> iterator = dir.iterator();

            // We're expecting an exception here to be thrown
            Exception exception = assertThrows(UnsupportedOperationException.class, iterator::remove);
            assertNotNull(exception);
        }
    }

    @Test
    public void directoryStreamWithFilter()
        throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA")
              .dir("dir1") //
              .file("dir/file1.txt", "content".getBytes()) //
              .file("dir/file2.sql", "content".getBytes()) //
              .file("dir/file3.txt", "content".getBytes()) //
              .file("dir/file4.sql", "content".getBytes()) //
              .file("dir/tmp_file.txt", "content".getBytes());

        // act
        Path base = createNewS3FileSystem().getPath("/bucketA", "dir");
        DirectoryStream.Filter<Path> filter = entry -> {
            String filename = entry.getFileName().toString();
            return filename.startsWith("tmp_") || filename.endsWith(".sql");
        };

        // act
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(base, filter))
        {
            // assert
            assertDirectoryStream(dirStream, "file2.sql", "file4.sql", "tmp_file.txt");
        }
    }

    @Test
    void list999Paths()
            throws IOException
    {
        // fixtures
        S3ClientMock client = S3MockFactory.getS3ClientMock();

        MockBucket bucketA = client.bucket("bucketA");

        final int count999 = 999;
        for (int i = 0; i < count999; i++)
        {
            bucketA.file(i + "file");
        }

        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        int count = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(bucket))
        {
            for (Path ignored : files)
            {
                count++;
            }
        }

        assertEquals(count999, count);
    }

    /**
     * check if the directory path contains all the files name
     *
     * @param base  Path
     * @param files String array of file names
     * @throws IOException
     */
    private void assertNewDirectoryStream(Path base,
                                          final String... files)
            throws IOException
    {
        try (DirectoryStream<Path> dir = Files.newDirectoryStream(base))
        {
            assertDirectoryStream(dir, files);
        }
    }

    private void assertDirectoryStream(DirectoryStream<Path> dir,
                                       final String... files)
    {
        assertNotNull(dir);
        assertNotNull(dir.iterator());
        assertTrue(dir.iterator().hasNext());

        Set<String> filesNamesExpected = new HashSet<>(Arrays.asList(files));
        Set<String> filesNamesActual = new HashSet<>();

        for (Path path : dir)
        {
            String fileName = path.getFileName().toString();
            filesNamesActual.add(fileName);
        }

        assertEquals(filesNamesExpected, filesNamesActual);
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
     * @throws IOException
     */
    private S3FileSystem createNewS3FileSystem()
            throws IOException
    {
        try
        {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        }
        catch (FileSystemNotFoundException e)
        {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }
    }

}
