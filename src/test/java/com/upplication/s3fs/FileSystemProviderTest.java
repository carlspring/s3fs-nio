package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AccessControlList;
import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;

public class FileSystemProviderTest extends S3UnitTest {
	S3FileSystemProvider s3fsProvider;

	@Before
	public void cleanup() {
		s3fsProvider = spy(new S3FileSystemProvider());
		doReturn(new Properties()).when(s3fsProvider).loadAmazonProperties();
	}

	@Test(expected = S3FileSystemConfigurationException.class)
	public void missconfigure() {
		Properties props = new Properties();
		props.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.BrokenAmazonS3Factory");
		s3fsProvider.createFileSystem(S3UnitTest.S3_GLOBAL_URI, props);
	}

	@Test
	public void createsAuthenticatedByEnv() throws IOException {

		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = s3fsProvider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, env);

		assertNotNull(fileSystem);
		verify(s3fsProvider).createFileSystem(eq(S3UnitTest.S3_GLOBAL_URI), eq(buildFakeProps((String) env.get(ACCESS_KEY), (String) env.get(SECRET_KEY))));
	}

	@Test
	public void createAuthenticatedByProperties() throws IOException {
		Properties props = new Properties();
		props.setProperty(SECRET_KEY, "better secret key");
		props.setProperty(ACCESS_KEY, "better access key");
		doReturn(props).when(s3fsProvider).loadAmazonProperties();
		URI uri = S3UnitTest.S3_GLOBAL_URI;

		FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better access key", "better secret key")));
	}

	@Test
	public void createsAnonymous() throws IOException {
		URI uri = S3UnitTest.S3_GLOBAL_URI;
		FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object> of());

		assertNotNull(fileSystem);
		verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps(null, null)));
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createFailsIfAlreadyCreated() throws IOException {
		FileSystem fileSystem = s3fsProvider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		s3fsProvider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
	}

	@Test
	public void getFileSystem() throws IOException {
		FileSystem fileSystem = s3fsProvider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		FileSystem other = s3fsProvider.getFileSystem(S3UnitTest.S3_GLOBAL_URI);
		assertSame(fileSystem, other);
	}

	@Test
	public void getPathWithEmtpyEndpoint() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(S3UnitTest.S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void getPath() {
		FileSystem fs = FileSystems.getFileSystem(URI.create("s3://endpoint1/"));
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void getAnotherPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"), ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test(expected = IllegalArgumentException.class)
	public void getPathWithEndpointAndWithoutBucket() {
		FileSystem fs = FileSystems.getFileSystem(URI.create("s3://endpoint1/"));
		fs.provider().getPath(URI.create("s3://endpoint1//falta-bucket"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void getPathWithDefaultEndpointAndWithoutBucket() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(S3UnitTest.S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		fs.provider().getPath(URI.create("s3:////falta-bucket"));
	}

	@Test
	public void closeFileSystemReturnNewFileSystem() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();
		FileSystem fileSystem = provider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, env);
		assertNotNull(fileSystem);
		fileSystem.close();
		FileSystem fileSystem2 = provider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, env);
		assertNotSame(fileSystem, fileSystem2);
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createTwoFileSystemThrowError() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();
		FileSystem fileSystem = provider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, env);
		assertNotNull(fileSystem);
		provider.newFileSystem(S3UnitTest.S3_GLOBAL_URI, env);
	}

	// stream directory

	@Test
	public void createStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addFile(mocket, "file1");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		// assert
		assertNewDirectoryStream(bucket, "file1");
	}

	@Test
	public void createAnotherStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addFile(mocket, "file1");
		client.addFile(mocket, "file2");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "file2");
	}

	@Test
	public void createAnotherWithDirStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir1");
		client.addFile(mocket, "file1");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "dir1");
	}

	@Test
	public void createStreamDirectoryFromDirectoryReader() throws IOException {

		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addDirectory(mockDir, "file2");
		client.addFile(mockDir, "file1");
		// act
		Path dir = createNewS3FileSystem().getPath("/bucketA", "dir");

		// assert
		assertNewDirectoryStream(dir, "file1", "file2");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void removeIteratorStreamDirectoryReader() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir1");
		client.addFile(mockDir, "file1", "content");

		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// act
		try (DirectoryStream<Path> dir = Files.newDirectoryStream(bucket)) {
			dir.iterator().remove();
		}

	}

	@Test
	public void list999Paths() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path bucketA = client.addBucket("bucketA");
		final int count999 = 999;
		for (int i = 0; i < count999; i++) {
			Path path = bucketA.resolve(i + "file");
			if (!Files.exists(path))
				Files.createFile(path);
		}
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		int count = 0;
		try (DirectoryStream<Path> files = Files.newDirectoryStream(bucket)) {
			Iterator<Path> iterator = files.iterator();
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
		}
		assertEquals(count999, count);
	}

	@Test
	public void list1050Paths() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path bucketA = client.addBucket("bucketA");
		final int count1050 = 1050;
		for (int i = 0; i < count1050; i++) {
			Path path = bucketA.resolve(i + "file");
			if (!Files.exists(path))
				Files.createFile(path);
		}
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		int count = 0;
		try (DirectoryStream<Path> files = Files.newDirectoryStream(bucket)) {
			Iterator<Path> iterator = files.iterator();
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
		}
		assertEquals(count1050, count);
	}

	// newInputStream

	@Test
	public void inputStreamFile() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path bucketA = client.addBucket("bucketA");
		client.addFile(bucketA, "file1", "content");

		Path file = createNewS3FileSystem().getPath("/bucketA/file1");
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals("content".getBytes(), buffer);
	}

	@Test
	public void anotherInputStreamFile() throws IOException {
		String res = "another content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path bucketA = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(bucketA, "dir");
		client.addFile(mockDir, "file1", res);
		// act
		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file1");

		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(res.getBytes(), buffer);
	}

	@Test(expected = IOException.class)
	public void inputStreamDirectory() throws IOException {
		// fixtures
		Path result = getS3Directory();
		// act
		s3fsProvider.newInputStream(result);
	}

	// newOutputStream 

	@Test
	public void outputStreamWithCreateNew() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");
		final String content = "sample content";

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
			stream.write(content.getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

	@Test(expected = FileAlreadyExistsException.class)
	public void outputStreamWithCreateNewAndFileExists() throws IOException {
		Path base = getS3Directory();
		Path file = Files.createFile(base.resolve("file1"));
		s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW);
	}

	@Test
	public void outputStreamWithCreateAndFileExists() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");
		Files.createFile(file);

		final String content = "sample content";

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE)) {
			stream.write(content.getBytes());
			stream.flush();
			stream.close();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

	@Test
	public void outputStreamWithCreateAndFileNotExists() throws IOException {
		Path base = getS3Directory();

		Path file = base.resolve("file1");

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE)) {
			stream.write("sample content".getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals("sample content".getBytes(), buffer);
	}

	@Test
	public void anotherOutputStream() throws IOException {
		Path base = getS3Directory();
		final String content = "heyyyyyy";
		Path file = base.resolve("file1");

		try (OutputStream stream = s3fsProvider.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
			stream.write(content.getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

	// seekable

	@Test
	public void seekable() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		final String content = "content";

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
			seekable.write(buffer);
			ByteBuffer bufferRead = ByteBuffer.allocate(7);
			seekable.position(0);
			seekable.read(bufferRead);

			assertArrayEquals(bufferRead.array(), buffer.array());
			seekable.close();
		}

		assertArrayEquals(content.getBytes(), Files.readAllBytes(base.resolve("file")));
	}

	@Test
	public void seekableSize() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {

			long size = seekable.size();

			assertEquals(content.length(), size);
		}
	}

	@Test
	public void seekableAnotherSize() throws IOException {
		final String content = "content-more-large";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {

			long size = seekable.size();

			assertEquals(content.length(), size);
		}
	}

	@Test
	public void seekablePosition() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			long position = seekable.position();
			assertEquals(0, position);

			seekable.position(10);
			long position2 = seekable.position();
			assertEquals(10, position2);
		}
	}

	@Test
	public void seekablePositionRead() throws IOException {
		final String content = "content-more-larger";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		ByteBuffer copy = ByteBuffer.allocate(3);

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.READ))) {
			long position = seekable.position();
			assertEquals(0, position);

			seekable.read(copy);
			long position2 = seekable.position();
			assertEquals(3, position2);
		}
	}

	@Test
	public void seekablePositionWrite() throws IOException {
		final String content = "content-more-larger";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		ByteBuffer copy = ByteBuffer.allocate(5);

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE))) {
			long position = seekable.position();
			assertEquals(0, position);

			seekable.write(copy);
			long position2 = seekable.position();
			assertEquals(5, position2);
		}
	}

	@Test
	public void seekableIsOpen() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE))) {
			assertTrue(seekable.isOpen());
		}

		SeekableByteChannel seekable = s3fsProvider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.READ));
		assertTrue(seekable.isOpen());
		seekable.close();
		assertTrue(!seekable.isOpen());
	}

	@Test
	public void seekableRead() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		final String content = "content";
		Path file = Files.write(base.resolve("file"), content.getBytes());

		ByteBuffer bufferRead = ByteBuffer.allocate(7);
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))) {
			seekable.position(0);
			seekable.read(bufferRead);
		}
		assertArrayEquals(bufferRead.array(), content.getBytes());

		assertArrayEquals(content.getBytes(), Files.readAllBytes(base.resolve("file")));
	}

	@Test
	public void seekableReadPartialContent() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		final String content = "content";
		Path file = Files.write(base.resolve("file"), content.getBytes());

		ByteBuffer bufferRead = ByteBuffer.allocate(4);
		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))) {
			seekable.position(3);
			seekable.read(bufferRead);
		}

		assertArrayEquals(bufferRead.array(), "tent".getBytes());
		assertArrayEquals("content".getBytes(), Files.readAllBytes(base.resolve("file")));
	}

	@Test
	public void seekableTruncate() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			// discard all content except the first c.
			seekable.truncate(1);
		}

		assertArrayEquals("c".getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableAnotherTruncate() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			// discard all content except the first three chars 'con'
			seekable.truncate(3);
		}

		assertArrayEquals("con".getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableruncateGreatherThanSize() throws IOException {
		final String content = "content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file", content);

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))) {
			seekable.truncate(10);
		}

		assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableCreateEmpty() throws IOException {

		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		Path file = base.resolve("file");

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.CREATE))) {
			//
		}

		assertTrue(Files.exists(file));
		assertArrayEquals("".getBytes(), Files.readAllBytes(file));
	}

	@Test
	public void seekableDeleteOnClose() throws IOException {

		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		Path file = Files.createFile(base.resolve("file"));

		try (SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE))) {
			seekable.close();
		}

		assertTrue(Files.notExists(file));
	}

	@Test
	public void seekableCloseTwice() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

		Path file = Files.createFile(base.resolve("file"));
		SeekableByteChannel seekable = s3fsProvider.newByteChannel(file, EnumSet.noneOf(StandardOpenOption.class));
		seekable.close();
		seekable.close();

		assertTrue(Files.exists(file));
	}

	// createDirectory

	@Test
	public void createDirectory() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.addBucket("bucketA");

		// act
		Path base = createNewS3FileSystem().getPath("/bucketA/dir");
		Files.createDirectory(base);
		// assert
		assertTrue(Files.exists(base));
		assertTrue(Files.isDirectory(base));
		assertTrue(Files.exists(base));
	}

	// delete

	@Test
	public void deleteFile() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");
		// act
		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
		s3fsProvider.delete(file);
		// assert
		assertTrue(Files.notExists(file));
	}

	@Test
	public void deleteEmptyDirectory() throws IOException {
		Path base = getS3Directory();
		s3fsProvider.delete(base);
		// assert
		assertTrue(Files.notExists(base));
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void deleteDirectoryWithEntries() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
		s3fsProvider.delete(file.getParent());
	}

	@Test(expected = NoSuchFileException.class)
	public void deleteFileNotExists() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
		s3fsProvider.delete(file);
	}

	// copy

	@Test
	public void copy() throws IOException {
		final String content = "content-file-1";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1", content);
		client.addDirectory(mocket, "dir2");

		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		s3fsProvider.copy(file, fileDest);
		// assert
		assertTrue(Files.exists(fileDest));
		assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
	}

	@Test
	public void copySameFile() throws IOException {
		final String content = "sample-content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1", content);
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA", "dir", "file1");
		Path fileDest = fs.getPath("/bucketA", "dir", "file1");
		s3fsProvider.copy(file, fileDest);
		// assert
		assertTrue(Files.exists(fileDest));
		assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
		assertEquals(file, fileDest);
	}

	@Test
	public void copyAlreadyExistsWithReplace() throws IOException {
		final String content = "sample-content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1", content);
		client.addFile(mockDir, "file2");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA", "dir", "file1");
		Path fileDest = fs.getPath("/bucketA", "dir", "file2");
		s3fsProvider.copy(file, fileDest, StandardCopyOption.REPLACE_EXISTING);
		// assert
		assertTrue(Files.exists(fileDest));
		assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
	}

	@Test(expected = FileAlreadyExistsException.class)
	public void copyAlreadyExists() throws IOException {
		final String content = "sample-content";
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1", content);
		client.addFile(mockDir, "file2", content);
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA", "dir", "file1");
		Path fileDest = fs.getPath("/bucketA", "dir", "file2");
		s3fsProvider.copy(file, fileDest);
	}

	// move

	@Test(expected = UnsupportedOperationException.class)
	public void move() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1");
		client.addDirectory(mocket, "dir2");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		s3fsProvider.move(file, fileDest);
	}

	// isSameFile

	@Test
	public void isSameFileTrue() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir/file1");
		// assert
		assertTrue(s3fsProvider.isSameFile(file1, fileCopy));
	}

	@Test
	public void isSameFileFalse() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1");
		Path mockDir2 = client.addDirectory(mocket, "dir2");
		client.addFile(mockDir2, "file13");
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir2/file2");
		// assert
		assertTrue(!s3fsProvider.isSameFile(file1, fileCopy));
	}

	// isHidden

	@Test
	public void isHidden() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1");
		// act
		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
		// assert
		assertTrue(!s3fsProvider.isHidden(file1));
	}

	// getFileStore

	@Test(expected = UnsupportedOperationException.class)
	public void getFileStore() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1");

		// act
		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
		// assert
		s3fsProvider.getFileStore(file1);
	}

	// getFileAttributeView

	@Test(expected = UnsupportedOperationException.class)
	public void getFileAttributeView() {
		s3fsProvider.getFileAttributeView(null, null);
	}

	// readAttributes

	@Test
	public void readAttributesFileEmpty() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1");

		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");

		BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(file1, BasicFileAttributes.class);

		assertNotNull(fileAttributes);
		assertEquals(false, fileAttributes.isDirectory());
		assertEquals(true, fileAttributes.isRegularFile());
		assertEquals(false, fileAttributes.isSymbolicLink());
		assertEquals(false, fileAttributes.isOther());
		assertEquals(0L, fileAttributes.size());
	}

	@Test
	public void readAttributesFile() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path dir = client.addDirectory(mocket, "dir");

		final String content = "sample";
		Path memoryFile = Files.write(dir.resolve("file"), content.getBytes());

		BasicFileAttributes expectedAttributes = Files.readAttributes(memoryFile, BasicFileAttributes.class);

		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA/dir/file");

		BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(file, BasicFileAttributes.class);

		assertNotNull(fileAttributes);
		assertEquals(false, fileAttributes.isDirectory());
		assertEquals(true, fileAttributes.isRegularFile());
		assertEquals(false, fileAttributes.isSymbolicLink());
		assertEquals(false, fileAttributes.isOther());
		assertEquals(content.getBytes().length, fileAttributes.size());
		assertEquals("dir/file", fileAttributes.fileKey());
		assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.lastModifiedTime());
		// TODO: creation and access are the same that last modified time
		assertEquals(fileAttributes.lastModifiedTime(), fileAttributes.creationTime());
		assertEquals(fileAttributes.lastModifiedTime(), fileAttributes.lastAccessTime());
	}

	@Test
	public void readAttributesDirectory() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path memoryDir = client.addDirectory(mocket, "dir");

		BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir, BasicFileAttributes.class);

		FileSystem fs = createNewS3FileSystem();
		Path dir = fs.getPath("/bucketA/dir");

		BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);

		assertNotNull(fileAttributes);
		assertEquals(true, fileAttributes.isDirectory());
		assertEquals(false, fileAttributes.isRegularFile());
		assertEquals(false, fileAttributes.isSymbolicLink());
		assertEquals(false, fileAttributes.isOther());
		assertEquals(0L, fileAttributes.size());
		assertEquals("dir/", fileAttributes.fileKey());
		assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.lastModifiedTime());
		assertEquals(expectedAttributes.creationTime(), fileAttributes.creationTime());
		assertEquals(expectedAttributes.lastAccessTime(), fileAttributes.lastAccessTime());
		// TODO: creation and access are the same that last modified time
		assertEquals(fileAttributes.creationTime(), fileAttributes.lastModifiedTime());
		assertEquals(fileAttributes.lastAccessTime(), fileAttributes.lastModifiedTime());
	}

	@Test
	public void readAnotherAttributesDirectory() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path directory = client.addDirectory(mocket, "dir");
		client.addFile(directory, "dir", "content");

		FileSystem fs = createNewS3FileSystem();
		Path dir = fs.getPath("/bucketA/dir");

		BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);
		assertNotNull(fileAttributes);
		assertEquals(true, fileAttributes.isDirectory());
	}

	@Test
	public void readAttributesDirectoryNotExistsAtAmazon() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		Path memoryDir = client.addDirectory(mockDir, "dir2");

		BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir, BasicFileAttributes.class);

		FileSystem fs = createNewS3FileSystem();
		Path dir = fs.getPath("/bucketA/dir");

		BasicFileAttributes fileAttributes = s3fsProvider.readAttributes(dir, BasicFileAttributes.class);

		assertNotNull(fileAttributes);
		assertEquals(true, fileAttributes.isDirectory());
		assertEquals(false, fileAttributes.isRegularFile());
		assertEquals(false, fileAttributes.isSymbolicLink());
		assertEquals(false, fileAttributes.isOther());
		assertEquals(0L, fileAttributes.size());
		assertEquals("dir/", fileAttributes.fileKey());
		assertEquals(expectedAttributes.lastModifiedTime(), fileAttributes.lastModifiedTime());
		assertEquals(expectedAttributes.creationTime(), fileAttributes.creationTime());
		assertEquals(expectedAttributes.lastAccessTime(), fileAttributes.lastAccessTime());
		// TODO: creation and access are the same that last modified time
		assertEquals(fileAttributes.creationTime(), fileAttributes.lastModifiedTime());
		assertEquals(fileAttributes.lastAccessTime(), fileAttributes.lastModifiedTime());
	}

	@Test(expected = NoSuchFileException.class)
	public void readAttributesFileNotExists() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");

		s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
	}

	@Test(expected = NoSuchFileException.class)
	public void readAttributesFileNotExistsButExistsAnotherThatContainsTheKey() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file1hola", "content");

		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");

		s3fsProvider.readAttributes(file1, BasicFileAttributes.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesNotAcceptedSubclass() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		FileSystem fs = createNewS3FileSystem();
		Path dir = fs.getPath("/bucketA/dir");

		s3fsProvider.readAttributes(dir, DosFileAttributes.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesString() throws IOException {
		s3fsProvider.readAttributes(null, "");
	}

	// setAttribute

	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesObject() throws IOException {
		s3fsProvider.setAttribute(null, "", new Object());
	}

	// check access

	@Test
	public void checkAccessRead() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");

		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file");

		s3fsProvider.checkAccess(file1, AccessMode.READ);
	}

	@Test(expected = AccessDeniedException.class)
	public void checkAccessReadWithoutPermission() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");

		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir");

		s3fsProvider.checkAccess(file1, AccessMode.READ);
	}

	@Test
	public void checkAccessWrite() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");

		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file");

		s3fsProvider.checkAccess(file1, AccessMode.WRITE);
	}

	@Test(expected = AccessDeniedException.class)
	public void checkAccessWriteDifferentUser() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "readOnly");
		// return empty list
		doReturn(client.createReadOnly("bucketA")).when(client).getObjectAcl("bucketA", "dir/readOnly");

		S3FileSystem fs = createNewS3FileSystem();
		S3Path file1 = fs.getPath("/bucketA/dir/readOnly");

		s3fsProvider.checkAccess(file1, AccessMode.WRITE);
	}

	@Test(expected = AccessDeniedException.class)
	public void checkAccessWriteWithoutPermission() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");
		// return empty list
		doReturn(new AccessControlList()).when(client).getObjectAcl("bucketA", "dir/");

		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir");

		s3fsProvider.checkAccess(file1, AccessMode.WRITE);
	}

	@Test(expected = AccessDeniedException.class)
	public void checkAccessExecute() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		Path mockDir = client.addDirectory(mocket, "dir");
		client.addFile(mockDir, "file");

		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file");

		s3fsProvider.checkAccess(file1, AccessMode.EXECUTE);
	}

	private Map<String, ?> buildFakeEnv() {
		return ImmutableMap.<String, Object> builder().put(ACCESS_KEY, "access key").put(SECRET_KEY, "secret key").build();
	}

	private Properties buildFakeProps(String access_key, String secret_key) {
		Properties props = new Properties();
		props.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.AmazonS3MockFactory");
		if (access_key != null)
			props.setProperty(ACCESS_KEY, access_key);
		if (secret_key != null)
			props.setProperty(SECRET_KEY, secret_key);
		return props;
	}

	private void assertNewDirectoryStream(Path base, final String... files) throws IOException {
		try (DirectoryStream<Path> dir = Files.newDirectoryStream(base)) {
			assertNotNull(dir);
			assertNotNull(dir.iterator());
			assertTrue(dir.iterator().hasNext());

			Set<String> filesNamesExpected = new HashSet<>(Arrays.asList(files));
			Set<String> filesNamesActual = new HashSet<>();

			for (Path path : dir) {
				String fileName = path.getFileName().toString();
				filesNamesActual.add(fileName);
			}

			assertEquals(filesNamesExpected, filesNamesActual);
		}
	}

	private Path getS3Directory() throws IOException {
		// fixtures
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("bucketA");
		client.addDirectory(mocket, "dir");
		return s3fsProvider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
	}

	/**
	 * create a new file system for s3 scheme with fake credentials
	 * and global endpoint
	 * @return FileSystem
	 * @throws IOException
	 */
	private S3FileSystem createNewS3FileSystem() throws IOException {
		return s3fsProvider.getFileSystem(S3UnitTest.S3_GLOBAL_URI);
	}
}
