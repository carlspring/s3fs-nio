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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.MockBucket;

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
		client.bucket("bucketA").dir("dir").file("dir/file1");
		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);
		assertIterator(iterator, "file1");
	}

	@Test
	public void iteratorAnotherDirectory() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir2").file("dir2/file1","dir2/file2");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file1", "file2");
	}

	@Test
	public void iteratorWithFileContainsDirectoryName() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir2").file("dir2/dir2-file", "dir2-file2");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir2");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "dir2-file");
	}

	@Test
	public void iteratorWithSubFolderAndSubFiles() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir", "dir/dir", "dir/dir2", "dir/dir2/dir3").file("dir/file", "dir/file2", "dir/dir/file", "dir/dir2/file", "dir/dir2/dir3/file");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "dir", "dir2", "file", "file2");
	}

	@Test
	public void iteratorWithSubFolderAndSubFilesAtBucketLevel() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("file", "file2", "dir/file3", "dir2/file4", "dir2/dir3/file3").dir("dir", "dir2", "dir2/dir3", "dir4");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "dir", "dir2", "dir4", "file", "file2");
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
		client.bucket("bucketA").file("file1");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "file1");
		S3Iterator iterator = new S3Iterator(path);

		assertFalse(iterator.hasNext());
	}

	@Test
	public void iteratorEmptyDirectory() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").dir("dir");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);

		assertFalse(iterator.hasNext());
	}

	@Test
	public void iteratorBucket() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("file1", "file2", "file3");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file1", "file2", "file3");
	}

	@Test
	public void iteratorDirs() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("file1", "file2", "file3", "directory1/file1.1", "directory1/file1.2", "directory1/file1.3").dir("directory1");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "directory1", "file1", "file2", "file3");
	}

	@Test
	public void virtualDirs() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("bucketA").file("directory1/file1.1", "directory1/file1.2", "directory1/file1.3");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA/directory1");
		S3Iterator iterator = new S3Iterator(path);

		assertIterator(iterator, "file1.1", "file1.2", "file1.3");
		path = s3FileSystem.getPath("/bucketA");
		iterator = new S3Iterator(path);
		assertIterator(iterator, "directory1");
	}

	@Test
	public void iteratorMoreThanAmazonS3ClientLimit() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		MockBucket mocket = client.bucket("bucketA");

		String filesNameExpected[] = new String[1050];
		for (int i = 0; i < 1050; i++) {
			final String name = "file-" + i;
			mocket.file(name);
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
		client.bucket("bucketA").dir("dir").file("dir/file1");

		S3FileSystem s3FileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path path = s3FileSystem.getPath("/bucketA", "dir");
		S3Iterator iterator = new S3Iterator(path);
		iterator.remove();
	}

	private void assertIterator(Iterator<Path> iterator, final String... files) {
		assertNotNull(iterator);
		assertTrue(iterator.hasNext());
		List<String> filesNamesExpected = Arrays.asList(files);
		Collections.sort(filesNamesExpected);
		List<String> filesNamesActual = new ArrayList<>();
		while (iterator.hasNext()) {
			Path path = iterator.next();
			String fileName = path.getFileName().toString();
			filesNamesActual.add(fileName);
		}
		assertEquals(filesNamesExpected, filesNamesActual);
	}
}