package org.weakref.s3fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.weakref.s3fs.util.AmazonS3ClientMock;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.google.common.collect.ImmutableMap;

public class FileSystemProviderTest {
	S3FileSystemProvider provider;
	FileSystem fsMem;
	
	@Before
	public void cleanup() throws IOException{
		fsMem = MemoryFileSystemBuilder.newLinux().build("basescheme");
		try{
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException e){}
		
		//S3FileSystem fs = spy(new S3FileSystem(provider, client, endpoint))
		provider = spy(new S3FileSystemProvider());
		doReturn(new Properties()).when(provider).loadAmazonProperties();
	}
	
	@After
	public void closeMemory() throws IOException{
		fsMem.close();
	}
	
	
	private void mockFileSystem(final Path memoryBucket){
		try {
			AmazonS3ClientMock clientMock = new AmazonS3ClientMock(memoryBucket);
			S3FileSystem s3ileS3FileSystem = new S3FileSystem(provider, clientMock, "endpoint");
			doReturn(s3ileS3FileSystem).when(provider).createFileSystem(any(URI.class), anyObject(), anyObject());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void createsAuthenticatedByEnv() throws IOException {
		
		Map<String, ?> env = buildFakeEnv();
		URI uri = URI.create("s3:///");
		
		FileSystem fileSystem = provider.newFileSystem(uri,
				env);

		assertNotNull(fileSystem);	
		verify(provider).createFileSystem(eq(uri),eq(env.get(S3FileSystemProvider.ACCESS_KEY)), eq(env.get(S3FileSystemProvider.SECRET_KEY)));
	}
	
	@Test
	public void createAuthenticatedByProperties() throws IOException{
		Properties props = new Properties();
		props.setProperty(S3FileSystemProvider.SECRET_KEY, "secret key");
		props.setProperty(S3FileSystemProvider.ACCESS_KEY, "access key");
		doReturn(props).when(provider).loadAmazonProperties();
		URI uri = URI.create("s3:///");
		
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		
		verify(provider).createFileSystem(eq(uri), eq("access key"), eq("secret key"));
	}

	@Test
	public void createsAnonymous() throws IOException {
		URI uri = URI.create("s3:///");
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(uri),eq(null),eq(null));
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createFailsIfAlreadyCreated() throws IOException {
		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
	}

	@Test
	public void getFileSystem() throws IOException {
		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		FileSystem other = provider.getFileSystem(URI.create("s3:///"));
		assertSame(fileSystem, other);
	}

	@Test(expected = FileSystemNotFoundException.class)
	public void getFailsIfNotYetCreated() {
		provider.getFileSystem(URI.create("s3:///"));
	}

	@Test
	public void getPathWithEmtpyEndpoint() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}
	
	@Test
	public void getPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void getAnotherPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, fs.getPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void getPathWithInvalidEndpoint () throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		fs.provider().getPath(URI.create("s3://endpoint2/bucket/path/to/file"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void getPathWithEndpointAndWithoutBucket() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		fs.provider().getPath(URI.create("s3://endpoint1//falta-bucket"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void getPathWithDefaultEndpointAndWithoutBucket() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		fs.provider().getPath(URI.create("s3:////falta-bucket"));
	}

	@Test
	public void closeFileSystemReturnNewFileSystem() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				env);
		assertNotNull(fileSystem);

		fileSystem.close();

		FileSystem fileSystem2 = provider.newFileSystem(URI.create("s3:///"),
				env);

		assertNotSame(fileSystem, fileSystem2);
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createTwoFileSystemThrowError() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				env);
		assertNotNull(fileSystem);
		provider.newFileSystem(URI.create("s3:///"), env);

	}
	
	// stream directory
	
	@Test
	public void createStreamDirectoryReader() throws IOException{
		
		// fixtures
		Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		Files.createFile(bucketA.resolve("file1"));
		
		mockFileSystem(fsMem.getPath("/base"));
		
		Path bucket = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA");
		// assert
		assertNewDirectoryStream(bucket, "file1");
	}
	
	@Test
	public void createAnotherStreamDirectoryReader() throws IOException{
		
		// fixtures
		Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		Files.createFile(bucketA.resolve("file1"));
		Files.createFile(bucketA.resolve("file2"));
		
		mockFileSystem(fsMem.getPath("/base"));
		Path bucket = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "file2");
	}
	
	@Test
	public void createAnotherWithDirStreamDirectoryReader() throws IOException{
		
		// fixtures
		Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		Files.createFile(bucketA.resolve("file1"));
		Files.createDirectory(bucketA.resolve("dir1"));
		
		mockFileSystem(fsMem.getPath("/base"));
		Path bucket = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "dir1");
	}
	
	// newInputStream
	
	@Test
	public void inputStreamFile() throws IOException{
		
		// fixtures
		byte[] res = "contenido".getBytes();
		Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		Files.write(bucketA.resolve("file1"), res, StandardOpenOption.CREATE_NEW);
		
		mockFileSystem(fsMem.getPath("/base"));
		Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/file1");
		
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals(res, buffer);
	}
	
	@Test
	public void anotherInputStreamFile() throws IOException{
		// fixtures
		byte[] res = "contenido diferente".getBytes();
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.write(dir.resolve("file1"), res, StandardOpenOption.CREATE_NEW);
		
		mockFileSystem(fsMem.getPath("/base"));
		Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir/file1");
		
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(res, buffer);
	}

	
	@Test(expected = IOException.class)
	public void inputStreamDirectory() throws IOException{
		// fixtures
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		mockFileSystem(fsMem.getPath("/base"));
		Path result = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
		// act
		provider.newInputStream(result);
	}
	
	// newOutputStream 
	
	@Test
	public void outputStream() throws IOException{
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		mockFileSystem(fsMem.getPath("/base"));
		Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
		
		Path file = base.resolve("file1");
		
		try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE_NEW)){
			stream.write("hola que haces".getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals("hola que haces".getBytes(), buffer);
	}
	
	@Test
	public void anotherOutputStream() throws IOException{
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		mockFileSystem(fsMem.getPath("/base"));
		Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
		
		Path file = base.resolve("file1");
		
		try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE_NEW)){
			stream.write("heyyyyy".getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals("heyyyyy".getBytes(), buffer);
	}
	
	// seekable
	
	//TODO: @Test
	public void seekable() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Path file = Files.createFile(dir.resolve("file"));
		mockFileSystem(fsMem.getPath("/base"));
		Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
		
		try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
			ByteBuffer buffer = ByteBuffer.wrap("content".getBytes());
			seekable.write(buffer);
			ByteBuffer bufferRead = ByteBuffer.allocate(7);
			seekable.position(0);
			seekable.read(bufferRead);

			assertArrayEquals(bufferRead.array(), buffer.array());
		}
		
		assertArrayEquals("content".getBytes(), Files.readAllBytes(base.resolve("file")));
	}
	
	// createDirectory
	
	@Test
	public void createDirectory() throws IOException{
		
		Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		mockFileSystem(fsMem.getPath("/base"));
		// act
		Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
		Files.createDirectory(base);
		// assert
		assertTrue(Files.exists(base));
		assertTrue(Files.isDirectory(base));
		assertTrue(Files.exists(base));
	}
	
	// delete
	
	@Test
	public void delete() throws IOException{
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		mockFileSystem(fsMem.getPath("/base"));
		// act
		Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
		provider.delete(base);
		// assert
		assertTrue(Files.notExists(base));
	}
	
	// copy
	
	@Test
	public void copy() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.write(dir.resolve("file1"), "content-file-1".getBytes(), StandardOpenOption.CREATE);
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		provider.copy(file, fileDest);
		// assert
		assertArrayEquals("content-file-1".getBytes(), Files.readAllBytes(fileDest));
	}
	
	// move
	
	@Test(expected = UnsupportedOperationException.class)
	public void move() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.write(dir.resolve("file1"), "content-file-1".getBytes(), StandardOpenOption.CREATE);
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		provider.move(file, fileDest);
	}
	
	// isSameFile
	
	@Test
	public void isSameFileTrue() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.write(dir.resolve("file1"), "content-file-1".getBytes(), StandardOpenOption.CREATE);
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir/file1");
		// act
		assertTrue(provider.isSameFile(file1, fileCopy));
	}
	
	@Test
	public void isSameFileFalse() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.createFile(dir.resolve("file1"));
		Files.createFile(dir.resolve("file2"));
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir/file2");
		// act
		assertTrue(!provider.isSameFile(file1, fileCopy));
	}
	
	// isHidden
	
	@Test
	public void isHidden() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.createFile(dir.resolve("file1"));
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file1 = fs.getPath("/bucketA/dir/file1");
		// act
		assertTrue(!provider.isHidden(file1));
	}
	
	// getFileStore
	
	@Test(expected = UnsupportedOperationException.class)
	public void getFileStore() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.createFile(dir.resolve("file1"));
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file1 = fs.getPath("/bucketA/dir/file1");
		// act
		provider.getFileStore(file1);
	}
	
	// getFileAttributeView
	
	@Test(expected = UnsupportedOperationException.class)
	public void getFileAttributeView(){
		provider.getFileAttributeView(null, null, null);
	}
	
	// readAttributes
	
	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesString() throws IOException{
		provider.readAttributes(null, "", null);
	}
	
	// setAttribute
	
	@Test(expected = UnsupportedOperationException.class)
	public void readAttributesObject() throws IOException{
		provider.setAttribute(null, "", new Object(), null);
	}
	
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
	}
	
	
	private void assertNewDirectoryStream(Path base, final String ... files) throws IOException {
		
		try (DirectoryStream<Path> dir = provider.newDirectoryStream(base, new  DirectoryStream.Filter<Path>(){
			@Override public boolean accept(Path entry) throws IOException {
				return true;
			}
		})){
			int finded = 0;
			assertNotNull(dir);
			assertNotNull(dir.iterator());
			assertTrue(dir.iterator().hasNext());
			for (Path path: dir){
				String fileName = path.getFileName().toString();
				for (String file : files){
					if (fileName.equals(file)){
						finded++;
					}
				}
			}
			assertEquals(files.length, finded);
		}
	}
}
