package com.upplication.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;

public class S3IteratorTest extends S3UnitTest {
	S3FileSystemProvider provider;

	@Before
	public void prepare() {
		provider = spy(new S3FileSystemProvider());
		doReturn(new Properties()).when(provider).loadAmazonProperties();
	}

	@Test
	public void iteratorDirectory() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path dir = client.addDirectory(mocket, "dir");
		client.addFile(dir, "file1");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);
		assertIterator(iterator, "file1");
	}

	@Test
	public void iteratorAnotherDirectory() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path dir = client.addDirectory(mocket, "dir2");
		client.addFile(dir, "file1");
		client.addFile(dir, "file2");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file1", "file2");
	}

	@Test
	public void iteratorWithFileContainsDirectoryName() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path dir = client.addDirectory(mocket, "dir2");
		client.addFile(dir, "dir2-file");
		client.addFile(mocket, "dir2-file2");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "dir2-file");
	}

	@Test
	public void iteratorWithSubFolderAndSubFiles() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path dir = client.addDirectory(mocket, "dir");
		client.addFile(dir, "file");
		client.addFile(dir, "file2");
		Path subdir = client.addDirectory(dir, "dir");
		client.addFile(subdir, "file");
		Path subdir2 = client.addDirectory(dir, "dir2");
		client.addFile(subdir2, "file");
		Path subdir3 = client.addDirectory(subdir2, "dir3");
		client.addFile(subdir3, "file3");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file", "file2", "dir", "dir2");
	}

	@Test
	public void iteratorWithSubFolderAndSubFilesAtBucketLevel() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addFile(mocket, "file");
		client.addFile(mocket, "file2");
		Path dir = client.addDirectory(mocket, "dir");
		client.addFile(dir, "file");
		Path subdir2 = client.addDirectory(mocket, "dir2");
		client.addFile(subdir2, "file");
		Path subdir3 = client.addDirectory(subdir2, "dir3");
		client.addFile(subdir3, "file3");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file", "file2", "dir", "dir2");
	}

	//    @Test(expected = IllegalArgumentException.class)
	//    public void iteratorKeyNotEndSlash() throws IOException {
	//		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
	//		Path mocket = client.addBucket("bucketA");
	//		Path subdir2 = client.addDirectory(mocket, "dir2");
	//		client.addFile(subdir2, "dir2-file");
	//        S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
	//        S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
	//        new S3Iterator(path);
	//    }

	@Test
	public void iteratorFileReturnEmpty() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addFile(mocket, "file1");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "file1");
		S3Iterator iterator = new S3Iterator(path);

		assertFalse(iterator.hasNext());
	}

	@Test
	public void iteratorEmptyDirectory() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);

		assertFalse(iterator.hasNext());
	}

	@Test
	public void iteratorBucket() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addFile(mocket, "file1");
		client.addFile(mocket, "file2");
		client.addFile(mocket, "file3");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file1", "file2", "file3");
	}

	@Test
	public void iteratorMoreThanAmazonS3ClientLimit() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");

		String filesNameExpected[] = new String[1050];
		for (int i = 0; i < 1050; i++) {
			final String name = "file-" + i;
			client.addFile(mocket, name);
			filesNameExpected[i] = name;
		}

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, filesNameExpected);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void remove() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path dir = client.addDirectory(mocket, "dir");
		client.addFile(dir, "file1");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);
		iterator.remove();
	}

	private void assertIterator(Iterator<Path> iterator, final String... files) {

		assertNotNull(iterator);
		assertTrue(iterator.hasNext());

		Set<String> filesNamesExpected = new HashSet<>(Arrays.asList(files));
		Set<String> filesNamesActual = new HashSet<>();

		while (iterator.hasNext()) {
			Path path = iterator.next();
			String fileName = path.getFileName().toString();
			filesNamesActual.add(fileName);
		}

		assertEquals(filesNamesExpected, filesNamesActual);
	}
}
