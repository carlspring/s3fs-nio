package com.upplication.s3fs;

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
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.upplication.s3fs.util.AmazonS3ClientMock;

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
		
		provider = spy(new S3FileSystemProvider());
        // TODO: we need some real temp dir with unique path when is called
        doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .when(provider).createTempDir();
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
		verify(provider).createFileSystem(eq(uri), eq(env.get(S3FileSystemProvider.ACCESS_KEY)), eq(env.get(S3FileSystemProvider.SECRET_KEY)));
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
		verify(provider).createFileSystem(eq(uri), eq(null), eq(null));
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
		// act
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

        // act
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
		String content = "content";
		Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		Files.write(bucketA.resolve("file1"), content.getBytes(), StandardOpenOption.CREATE_NEW);
		
		mockFileSystem(fsMem.getPath("/base"));
		Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/file1");
		
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}
	
	@Test
	public void anotherInputStreamFile() throws IOException{
		// fixtures
		String res = "another content";
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.write(dir.resolve("file1"), res.getBytes(), StandardOpenOption.CREATE_NEW);
		
		mockFileSystem(fsMem.getPath("/base"));
		Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir/file1");
		
		byte[] buffer = Files.readAllBytes(file);
		// check
		assertArrayEquals(res.getBytes(), buffer);
	}

	
	@Test(expected = IOException.class)
	public void inputStreamDirectory() throws IOException{
		// fixtures
        Path result = getS3Directory();
		// act
		provider.newInputStream(result);
	}
	
	// newOutputStream 
	
	@Test
	public void outputStreamWithCreateNew() throws IOException{
        Path base = getS3Directory();
		
		Path file = base.resolve("file1");
        final String content = "sample content";
		
		try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE_NEW)){
			stream.write(content.getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}

    @Test(expected = FileAlreadyExistsException.class)
    public void outputStreamWithCreateNewAndFileExists() throws IOException{
        Path base = getS3Directory();

        Path file = Files.createFile(base.resolve("file1"));

        try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE_NEW)){
        }
    }

    @Test
    public void outputStreamWithCreateAndFileExists() throws IOException{
        Path base = getS3Directory();

        Path file = base.resolve("file1");
        Files.createFile(file);

        final String content = "sample content";

        try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE)){
            stream.write(content.getBytes());
            stream.flush();
        }
        // get the input
        byte[] buffer =  Files.readAllBytes(file);
        // check
        assertArrayEquals(content.getBytes(), buffer);
    }

    @Test
    public void outputStreamWithCreateAndFileNotExists() throws IOException{
        Path base = getS3Directory();

        Path file = base.resolve("file1");

        try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE)){
            stream.write("sample content".getBytes());
            stream.flush();
        }
        // get the input
        byte[] buffer =  Files.readAllBytes(file);
        // check
        assertArrayEquals("sample content".getBytes(), buffer);
    }
	
	@Test
	public void anotherOutputStream() throws IOException{
        Path base = getS3Directory();
        final String content = "heyyyyyy";
		Path file = base.resolve("file1");

		try (OutputStream stream = provider.newOutputStream(file, StandardOpenOption.CREATE_NEW)){
			stream.write(content.getBytes());
			stream.flush();
		}
		// get the input
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}
	
	// seekable
	
	@Test
	public void seekable() throws IOException{
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createFile(dir.resolve("file"));
		mockFileSystem(fsMem.getPath("/base"));
		Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");

        final String content = "content";
		
		try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
			ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
			seekable.write(buffer);
			ByteBuffer bufferRead = ByteBuffer.allocate(7);
			seekable.position(0);
			seekable.read(bufferRead);

			assertArrayEquals(bufferRead.array(), buffer.array());
		}
		
		assertArrayEquals(content.getBytes(), Files.readAllBytes(base.resolve("file")));
	}
    @Test
    public void seekableRead() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));
        Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");

        final String content = "content";
        Path file = Files.write(base.resolve("file"), content.getBytes());

        ByteBuffer bufferRead = ByteBuffer.allocate(7);
        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))){
            seekable.position(0);
            seekable.read(bufferRead);
        }
        assertArrayEquals(bufferRead.array(), content.getBytes());

        assertArrayEquals(content.getBytes(), Files.readAllBytes(base.resolve("file")));
    }

    @Test
    public void seekableReadPartialContent() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));
        Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");

        final String content = "content";
        Path file = Files.write(base.resolve("file"), content.getBytes());

        ByteBuffer bufferRead = ByteBuffer.allocate(4);
        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.READ))){
            seekable.position(3);
            seekable.read(bufferRead);
        }

        assertArrayEquals(bufferRead.array(), "tent".getBytes());
        assertArrayEquals("content".getBytes(), Files.readAllBytes(base.resolve("file")));
    }

    @Test
    public void seekableCreateEmpty() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));
        Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");

        Path file = base.resolve("file");

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.CREATE))){

        }

        assertTrue(Files.exists(file));
        assertArrayEquals("".getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void seekableDeleteOnClose() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));
        Path base = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");

        Path file = Files.createFile(base.resolve("file"));

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE))){

        }

        assertTrue(Files.notExists(file));
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
	public void deleteFile() throws IOException{
        Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        Files.createFile(dir.resolve("file"));
        mockFileSystem(fsMem.getPath("/base"));
        Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir/file");
		provider.delete(file);
		// assert
		assertTrue(Files.notExists(file));
	}

    @Test
    public void deleteEmptyDirectory() throws IOException{
        Path base = getS3Directory();
        provider.delete(base);
        // assert
        assertTrue(Files.notExists(base));
    }

    @Test(expected = DirectoryNotEmptyException.class)
    public void deleteDirectoryWithEntries() throws IOException{
        Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        Files.createFile(dir.resolve("file"));
        mockFileSystem(fsMem.getPath("/base"));
        Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir/file");
        provider.delete(file.getParent());
    }

    @Test(expected = NoSuchFileException.class)
    public void deleteFileNotExists() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));
        Path file = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir/file");
        provider.delete(file);
    }
	
	// copy
	
	@Test
	public void copy() throws IOException{
        final String content = "content-file-1";
		Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
		Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir2"));
		Files.write(dir.resolve("file1"), content.getBytes(), StandardOpenOption.CREATE);
		mockFileSystem(fsMem.getPath("/base"));
		// act
		FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		provider.copy(file, fileDest);
		// assert
        assertTrue(Files.exists(fileDest));
		assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
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
		// assert
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
		// assert
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
		// assert
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
		// assert
		provider.getFileStore(file1);
	}
	
	// getFileAttributeView
	
	@Test(expected = UnsupportedOperationException.class)
	public void getFileAttributeView(){
		provider.getFileAttributeView(null, null, null);
	}
	
	// readAttributes

    @Test
    public void readAttributesFileEmpty() throws IOException {

        Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        Files.createFile(dir.resolve("file1"));
        mockFileSystem(fsMem.getPath("/base"));

        FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
        Path file1 = fs.getPath("/bucketA/dir/file1");

        BasicFileAttributes fileAttributes = provider.readAttributes(file1, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.isDirectory());
        assertEquals(true, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
    }

    @Test
    public void readAttributesFile() throws IOException {

        Path dir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        final String content = "sample";
        Files.write(dir.resolve("file1"), content.getBytes());
        mockFileSystem(fsMem.getPath("/base"));

        FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
        Path file1 = fs.getPath("/bucketA/dir/file1");

        BasicFileAttributes fileAttributes = provider.readAttributes(file1, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(false, fileAttributes.isDirectory());
        assertEquals(true, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(content.getBytes().length, fileAttributes.size());
    }

    @Test
    public void readAttributesDirectory() throws IOException {

        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));

        FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
        Path dir1 = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = provider.readAttributes(dir1, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertEquals(true, fileAttributes.isDirectory());
        assertEquals(false, fileAttributes.isRegularFile());
        assertEquals(false, fileAttributes.isSymbolicLink());
        assertEquals(false, fileAttributes.isOther());
        assertEquals(0L, fileAttributes.size());
    }

    @Test(expected= NoSuchFileException.class)
    public void readAttributesFileNotExists() throws IOException {

        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));

        FileSystem fs = provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv());
        Path file1 = fs.getPath("/bucketA/dir/file1");

        provider.readAttributes(file1, BasicFileAttributes.class);
    }

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

    private Path getS3Directory() throws IOException {
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));
        return provider.newFileSystem(URI.create("s3://endpoint1/"), buildFakeEnv()).getPath("/bucketA/dir");
    }
}
