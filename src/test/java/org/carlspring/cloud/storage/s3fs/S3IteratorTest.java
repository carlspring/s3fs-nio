package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.AmazonS3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.MockBucket;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

import com.amazonaws.services.s3.model.ObjectListing;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class S3IteratorTest
        extends S3UnitTestBase
{

    S3FileSystemProvider provider;

    private static URI endpoint = URI.create("s3://s3iteratortest.test");


    @Before
    public void prepare()
            throws IOException
    {
        provider = spy(new S3FileSystemProvider());

        doReturn(new Properties()).when(provider).loadAmazonProperties();
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        FileSystems.newFileSystem(endpoint, null);

        reset(AmazonS3MockFactory.getAmazonClientMock());
    }

    @Test
    public void iteratorDirectory()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1");
    }

    @Test
    public void iteratorAnotherDirectory()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir2").file("dir2/file1", "dir2/file2");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1", "file2");
    }

    @Test
    public void iteratorWithFileContainsDirectoryName()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir2").file("dir2/dir2-file", "dir2-file2");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "dir2-file");
    }

    @Test
    public void iteratorWithSubFolderAndSubFiles()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
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
    public void iteratorWithSubFolderAndSubFilesAtBucketLevel()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
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
    public void iteratorFileReturnEmpty()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").file("file1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "file1");
        S3Iterator iterator = new S3Iterator(path);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorEmptyDirectory()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");
        S3Iterator iterator = new S3Iterator(path);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorBucket()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").file("file1", "file2", "file3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, "file1", "file2", "file3");
    }

    @Test(expected = NoSuchElementException.class)
    public void iteratorExhausted()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").file("file1", "file2", "file3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA");
        S3Iterator iterator = new S3Iterator(path);

        while (iterator.hasNext())
        {
            iterator.next();
        }

        iterator.next();
    }

    @Test
    public void iteratorDirs()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
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
    public void virtualDirs()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
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
    public void incrementalVirtualDirs()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").file("dir/subdir/subberdir/file1.1",
                                      "dir/subdir/subberdir/file1.2",
                                      "dir/subdir/subberdir/file1.3");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA/dir/subdir");
        S3Iterator iterator = new S3Iterator(path, true);

        assertIterator(iterator, "subdir", "subberdir", "file1.1", "file1.2", "file1.3");
    }

    @Test
    public void iteratorMoreThanAmazonS3ClientLimit()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        MockBucket mockBucket = client.bucket("bucketD");

        String fileNamesExpected[] = new String[1050];
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

            fileNamesExpected[i] = name.toString();
        }

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketD");
        S3Iterator iterator = new S3Iterator(path);

        assertIterator(iterator, fileNamesExpected);

        verify(client, times(1)).listNextBatchOfObjects(any(ObjectListing.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove()
            throws IOException
    {
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1");

        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(endpoint);
        S3Path path = s3FileSystem.getPath("/bucketA", "dir");

        S3Iterator iterator = new S3Iterator(path);
        iterator.remove();
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
