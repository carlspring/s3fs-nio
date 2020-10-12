package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.MockBucket;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class S3IteratorTest
        extends S3UnitTestBase
{

    S3FileSystemProvider provider;

    private static final URI endpoint = URI.create("s3://s3iteratortest.test");


    @BeforeEach
    public void prepare()
            throws IOException
    {
        provider = spy(new S3FileSystemProvider());

        doReturn(new Properties()).when(provider).loadAmazonProperties();
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        FileSystems.newFileSystem(endpoint, null);

        reset(S3MockFactory.getS3ClientMock());
    }

    @Test
    void iteratorDirectory()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1");
    }

    @Test
    void iteratorAnotherDirectory()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir2").file("dir2/file1", "dir2/file2");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1", "file2");
    }

    @Test
    void iteratorWithFileContainsDirectoryName()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir2").file("dir2/dir2-file", "dir2-file2");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "dir2-file");
    }

    @Test
    void iteratorWithSubFolderAndSubFiles()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir", "dir/dir", "dir/dir2", "dir/dir2/dir3").file("dir/file",
                                                                                         "dir/file2",
                                                                                         "dir/dir/file",
                                                                                         "dir/dir2/file",
                                                                                         "dir/dir2/dir3/file");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "dir", "dir2", "file", "file2");
    }

    @Test
    void iteratorWithSubFolderAndSubFilesAtBucketLevel()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file", "file2", "dir/file3", "dir2/file4", "dir2/dir3/file3").dir("dir",
                                                                                                         "dir2",
                                                                                                         "dir2/dir3",
                                                                                                         "dir4");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "dir", "dir2", "dir4", "file", "file2");
    }

    @Test
    void iteratorFileReturnEmpty()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "file1");
        S3Iterator iterator = new S3Iterator(path);

        assertFalse(iterator.hasNext());
    }

    @Test
    void iteratorEmptyDirectory()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");
        S3Iterator iterator = new S3Iterator(path);

        assertFalse(iterator.hasNext());
    }

    @Test
    void iteratorBucket()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1", "file2", "file3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1", "file2", "file3");
    }

    @Test
    void iteratorExhausted()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1", "file2", "file3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA");
        S3Iterator iterator = new S3Iterator(path);

        while (iterator.hasNext())
        {
            iterator.next();
        }

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NoSuchElementException.class, iterator::next);

        assertNotNull(exception);
    }

    @Test
    void iteratorDirs()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("file1",
                                      "file2",
                                      "file3",
                                      "directory1/file1.1",
                                      "directory1/file1.2",
                                      "directory1/file1.3").dir("directory1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "directory1", "file1", "file2", "file3");
    }

    @Test
    void virtualDirs()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("directory1/file1.1", "directory1/file1.2", "directory1/file1.3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA/directory1");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1.1", "file1.2", "file1.3");

        path = s3FileSystem.getPath("/bucketA");
        iterator = new S3Iterator(path);

        assertIterator(iterator, "directory1");
    }

    @Test
    void incrementalVirtualDirs()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").file("dir/subdir/subberdir/file1.1",
                                      "dir/subdir/subberdir/file1.2",
                                      "dir/subdir/subberdir/file1.3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA/dir/subdir");
        S3Iterator iterator = new S3Iterator(path, true);

        assertIterator(iterator, "subdir", "subberdir", "file1.1", "file1.2", "file1.3");
    }

    @Test
    void iteratorMoreThanS3ClientLimit()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        MockBucket mockBucket = client.bucket("bucketD");

        String[] filesNameExpected = new String[1050];
        for (int i = 0; i < 1050; i++)
        {
            StringBuilder name = new StringBuilder("file-");

            if (i < 1000)
            {
                name.append("0");
            }

            if (i < 100)
            {
                name.append("0");
            }

            if (i < 10)
            {
                name.append("0");
            }

            name.append(i);
            mockBucket.file(name.toString());
            filesNameExpected[i] = name.toString();
        }

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketD");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, filesNameExpected);

        verify(client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void remove()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");

        S3Iterator iterator = new S3Iterator(path);

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(UnsupportedOperationException.class, iterator::remove);

        assertNotNull(exception);
    }

    private void assertIterator(Iterator<Path> iterator, final String... files)
    {
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        List<String> filesNamesExpected = Arrays.asList(files);
        List<String> filesNamesActual = new ArrayList<>();

        while (iterator.hasNext())
        {
            Path path = iterator.next();

            String fileName = path.getFileName().toString();

            filesNamesActual.add(fileName);
        }

        assertEquals(filesNamesExpected, filesNamesActual);
    }

}
