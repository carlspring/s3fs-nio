package com.upplication.s3fs;

import com.amazonaws.services.s3.model.AccessControlList;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class FileSystemProviderTest {

    public static final URI S3_GLOBAL_URI = URI.create("s3:///");

    S3FileSystemProvider provider;
	FileSystem fsMem;

	@Before
	public void cleanup() throws IOException{
		fsMem = MemoryFileSystemBuilder.newLinux().build("basescheme");
		try{
			FileSystems.getFileSystem(S3_GLOBAL_URI).close();
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
	
	
	private AmazonS3ClientMock mockFileSystem(final Path memoryBucket){
		try {
			AmazonS3ClientMock clientMock = spy(new AmazonS3ClientMock(memoryBucket));
			S3FileSystem s3ileS3FileSystem = new S3FileSystem(provider, clientMock, "endpoint");
			doReturn(s3ileS3FileSystem).when(provider).createFileSystem(any(URI.class), anyObject(), anyObject());
		    return clientMock;
        } catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void createsAuthenticatedByEnv() throws IOException {
		
		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, env);

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(S3_GLOBAL_URI), eq(env.get(S3FileSystemProvider.ACCESS_KEY)), eq(env.get(S3FileSystemProvider.SECRET_KEY)));
	}
	
	@Test
	public void createAuthenticatedByProperties() throws IOException{
		Properties props = new Properties();
		props.setProperty(S3FileSystemProvider.SECRET_KEY, "secret key");
		props.setProperty(S3FileSystemProvider.ACCESS_KEY, "access key");
		doReturn(props).when(provider).loadAmazonProperties();
		URI uri = S3_GLOBAL_URI;
		
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		
		verify(provider).createFileSystem(eq(uri), eq("access key"), eq("secret key"));
	}

	@Test
	public void createsAnonymous() throws IOException {
		URI uri = S3_GLOBAL_URI;
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(uri), eq(null), eq(null));
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createFailsIfAlreadyCreated() throws IOException {
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI.create("s3:///"),
                ImmutableMap.<String, Object>of());
		assertNotNull(fileSystem);

		provider.newFileSystem(S3_GLOBAL_URI,
				ImmutableMap.<String, Object> of());
	}

	@Test
	public void getFileSystem() throws IOException {
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI,
                ImmutableMap.<String, Object>of());
		assertNotNull(fileSystem);

		FileSystem other = provider.getFileSystem(S3_GLOBAL_URI);
		assertSame(fileSystem, other);
	}

	@Test(expected = FileSystemNotFoundException.class)
	public void getFailsIfNotYetCreated() {
		provider.getFileSystem(S3_GLOBAL_URI);
	}

	@Test
	public void getPathWithEmtpyEndpoint() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(S3_GLOBAL_URI,
                ImmutableMap.<String, Object>of());
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
		FileSystem fs = FileSystems.newFileSystem(S3_GLOBAL_URI,
                ImmutableMap.<String, Object>of());
		fs.provider().getPath(URI.create("s3:////falta-bucket"));
	}

	@Test
	public void closeFileSystemReturnNewFileSystem() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI,
                env);
		assertNotNull(fileSystem);

		fileSystem.close();

		FileSystem fileSystem2 = provider.newFileSystem(S3_GLOBAL_URI,
				env);

		assertNotSame(fileSystem, fileSystem2);
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void createTwoFileSystemThrowError() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI,
                env);
		assertNotNull(fileSystem);
		provider.newFileSystem(S3_GLOBAL_URI, env);

	}
	
	// stream directory
	
	@Test
	public void createStreamDirectoryReader() throws IOException{
		
		// fixtures
        createPathsAndMock(
                new String[]{"/bucketA/file1", "content"}
        );
		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");
		// assert
		assertNewDirectoryStream(bucket, "file1");
	}
	
	@Test
	public void createAnotherStreamDirectoryReader() throws IOException{
		
		// fixtures
        createPathsAndMock(
                new String[]{"/bucketA/file1", "content"},
                new String[]{"/bucketA/file2", "content"}
        );

        // act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "file2");
	}
	
	@Test
	public void createAnotherWithDirStreamDirectoryReader() throws IOException{
		
		// fixtures
        createPathsAndMock(
                new String[]{"/bucketA/dir1/"},
                new String[]{"/bucketA/file1", "content"}
        );
		// act
		Path bucket = createNewS3FileSystem().getPath("/bucketA");

		// assert
		assertNewDirectoryStream(bucket, "file1", "dir1");
	}

    @Test(expected = UnsupportedOperationException.class)
    public void removeIteratorStreamDirectoryReader() throws IOException{

        // fixtures
        createPathsAndMock(
                new String[]{"/bucketA/dir1/"},
                new String[]{"/bucketA/file1", "content"}
        );
        // act
        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        // act
        try (DirectoryStream<Path> dir = Files.newDirectoryStream(bucket)){
            dir.iterator().remove();
        }

    }

    @Test
    public void list999Paths() throws IOException {

        // fixtures
        Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));

        final int count999 = 999;

        for (int i = 0; i < count999; i++) {
            Path path = bucketA.resolve(i + "file");
            Files.createFile(path);
        }

        mockFileSystem(fsMem.getPath("/base"));


        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        int count = 0;

        try(DirectoryStream<Path> files = Files.newDirectoryStream(bucket)) {
            for(Path file : files) {
                count++;
            }
        }

        assertEquals(count999, count);

    }

    @Test
    public void list1050Paths() throws IOException {

        // fixtures
        Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));

        final int count1050 = 1050;

        for (int i = 0; i < count1050; i++) {
            Path path = bucketA.resolve(i + "file");
            Files.createFile(path);
        }

        mockFileSystem(fsMem.getPath("/base"));

        Path bucket = createNewS3FileSystem().getPath("/bucketA");

        int count = 0;

        try(DirectoryStream<Path> files = Files.newDirectoryStream(bucket)) {
            for(Path file : files) {
                count++;
            }
        }

        assertEquals(count1050, count);

    }
	
	// newInputStream
	
	@Test
	public void inputStreamFile() throws IOException{
		
		// fixtures
		String content = "content";
        createPathsAndMock(
                new String[]{"/bucketA/file1", content}
        );

		Path file = createNewS3FileSystem().getPath("/bucketA/file1");
		
		byte[] buffer =  Files.readAllBytes(file);
		// check
		assertArrayEquals(content.getBytes(), buffer);
	}
	
	@Test
	public void anotherInputStreamFile() throws IOException{
		// fixtures
		String res = "another content";

        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", res}
        );
		// act
		Path file = createNewS3FileSystem().getPath("/bucketA/dir/file1");
		
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
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", ""}
        );

		Path base = createNewS3FileSystem().getPath("/bucketA/dir");

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
    public void seekableSize() throws IOException {
        final String content = "content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){

            long size = seekable.size();

            assertEquals(content.length(), size);
        }
    }

    @Test
    public void seekableAnotherSize() throws IOException {
        final String content = "content-more-large";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){

            long size = seekable.size();

            assertEquals(content.length(), size);
        }
    }

    @Test
    public void seekablePosition() throws IOException {
        final String content = "content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
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
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        ByteBuffer copy = ByteBuffer.allocate(3);

        try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.READ))){
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
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        ByteBuffer copy = ByteBuffer.allocate(5);

        try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE))){
            long position = seekable.position();
            assertEquals(0, position);

            seekable.write(copy);
            long position2 = seekable.position();
            assertEquals(5, position2);
        }
    }

    @Test
    public void seekableIsOpen() throws IOException {
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", ""}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        try (SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"), EnumSet.of(StandardOpenOption.WRITE))){
            assertTrue(seekable.isOpen());
        }

        SeekableByteChannel seekable = provider.newByteChannel(base.resolve("file"),EnumSet.of(StandardOpenOption.READ));
        assertTrue(seekable.isOpen());
        seekable.close();
        assertTrue(!seekable.isOpen());
    }

    @Test
    public void seekableRead() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/"}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

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
        createPathsAndMock(
                new String[]{"/bucketA/dir/"}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

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
    public void seekableTruncate() throws IOException {
        final String content = "content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
            // discard all content except the first c.
            seekable.truncate(1);
        }

        assertArrayEquals("c".getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void seekableAnotherTruncate() throws IOException {
        final String content = "content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
            // discard all content except the first three chars 'con'
            seekable.truncate(3);
        }

        assertArrayEquals("con".getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void seekableruncateGreatherThanSize() throws IOException {
        final String content = "content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", content}
        );

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ))){
            seekable.truncate(10);
        }

        assertArrayEquals(content.getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void seekableCreateEmpty() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/"}
        );

        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        Path file = base.resolve("file");

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.CREATE))){

        }

        assertTrue(Files.exists(file));
        assertArrayEquals("".getBytes(), Files.readAllBytes(file));
    }

    @Test
    public void seekableDeleteOnClose() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/"}
        );
        Path base = createNewS3FileSystem().getPath("/bucketA/dir");

        Path file = Files.createFile(base.resolve("file"));

        try (SeekableByteChannel seekable = provider.newByteChannel(file, EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE))){

        }

        assertTrue(Files.notExists(file));
    }
	
	// createDirectory
	
	@Test
	public void createDirectory() throws IOException{

        createPathsAndMock(
                new String[]{"/bucketA/"}
        );

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
	public void deleteFile() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", "content"}
        );
        // act
        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
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

        createPathsAndMock(
                new String[]{"/bucketA/dir/file", "content"}
        );

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        provider.delete(file.getParent());
    }

    @Test(expected = NoSuchFileException.class)
    public void deleteFileNotExists() throws IOException{
        createPathsAndMock(
            new String[]{"/bucketA/dir"}
        );

        Path file = createNewS3FileSystem().getPath("/bucketA/dir/file");
        provider.delete(file);
    }
	
	// copy
	
	@Test
	public void copy() throws IOException{
        final String content = "content-file-1";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", content},
                new String[]{"/bucketA/di2/"}
        );

		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		provider.copy(file, fileDest);
		// assert
        assertTrue(Files.exists(fileDest));
		assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
	}

    @Test
    public void copySameFile() throws IOException{
        final String content = "sample-content";
        createPathsAndMock(new String[]{"/bucketA/dir/file1", content});
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file1");
        provider.copy(file, fileDest);
        // assert
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
        assertEquals(file, fileDest);
    }

    @Test
    public void copyAlreadyExistsWithReplace() throws IOException{
        final String content = "sample-content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", content},
                new String[]{"/bucketA/dir/file2", "content-2"}
        );
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");
        provider.copy(file, fileDest, StandardCopyOption.REPLACE_EXISTING);
        // assert
        assertTrue(Files.exists(fileDest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(fileDest));
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void copyAlreadyExists() throws IOException{
        final String content = "sample-content";
        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", content},
                new String[]{"/bucketA/dir/file2", content}
        );
        // act
        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA", "dir", "file1");
        Path fileDest = fs.getPath("/bucketA", "dir", "file2");
        provider.copy(file, fileDest);
    }

    // move
	
	@Test(expected = UnsupportedOperationException.class)
	public void move() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", "some content"},
                new String[]{"/bucketA/dir2/"}
        );
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file = fs.getPath("/bucketA/dir/file1");
		Path fileDest = fs.getPath("/bucketA", "dir2", "file2");
		provider.move(file, fileDest);
	}
	
	// isSameFile
	
	@Test
	public void isSameFileTrue() throws IOException{

        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", "some content"}
        );
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir/file1");
		// assert
		assertTrue(provider.isSameFile(file1, fileCopy));
	}
	
	@Test
	public void isSameFileFalse() throws IOException{

        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", "some content"},
                new String[]{"/bucketA/dir2/file2", "some content"}
        );
		// act
		FileSystem fs = createNewS3FileSystem();
		Path file1 = fs.getPath("/bucketA/dir/file1");
		Path fileCopy = fs.getPath("/bucketA/dir/file2");
		// assert
		assertTrue(!provider.isSameFile(file1, fileCopy));
	}
	
	// isHidden
	
	@Test
	public void isHidden() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", "some content"},
                new String[]{"/bucketA/dir2/"}
        );
		// act
		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
		// assert
		assertTrue(!provider.isHidden(file1));
	}
	
	// getFileStore
	
	@Test(expected = UnsupportedOperationException.class)
	public void getFileStore() throws IOException{

        createPathsAndMock(
                new String[]{"/bucketA/dir/file1", "some content"},
                new String[]{"/bucketA/dir2/"}
        );

		// act
		Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
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

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");

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
        Path memoryFile = Files.write(dir.resolve("file"), content.getBytes());

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryFile,  BasicFileAttributes.class);

        mockFileSystem(fsMem.getPath("/base"));

        FileSystem fs = createNewS3FileSystem();
        Path file = fs.getPath("/bucketA/dir/file");

        BasicFileAttributes fileAttributes = provider.readAttributes(file, BasicFileAttributes.class);

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

        Path memoryDir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        mockFileSystem(fsMem.getPath("/base"));

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir,  BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = provider.readAttributes(dir, BasicFileAttributes.class);

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
    public void readAttributesDirectoryNotExistsAtAmazon() throws IOException {

        Path memoryDir = Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir", "dir2"))
                .getParent();
        mockFileSystem(fsMem.getPath("/base"));

        BasicFileAttributes expectedAttributes = Files.readAttributes(memoryDir,  BasicFileAttributes.class);

        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        BasicFileAttributes fileAttributes = provider.readAttributes(dir, BasicFileAttributes.class);

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

    @Test(expected= NoSuchFileException.class)
    public void readAttributesFileNotExists() throws IOException {
        createPathsAndMock(
                new String[]{"/bucketA/dir/"}
        );


        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file1");

        provider.readAttributes(file1, BasicFileAttributes.class);
    }

    @Test(expected= UnsupportedOperationException.class)
    public void readAttributesNotAcceptedSubclass() throws IOException {
        createPathsAndMock(
                new String[]{"/bucketA/dir/"}
        );


        FileSystem fs = createNewS3FileSystem();
        Path dir = fs.getPath("/bucketA/dir");

        provider.readAttributes(dir, DosFileAttributes.class);
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

    // check access

    @Test
    public void checkAccessRead() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", "content"}
        );

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file");

        provider.checkAccess(file1, AccessMode.READ);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessReadWithoutPermission() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        AmazonS3ClientMock amazonS3ClientMock = mockFileSystem(fsMem.getPath("/base"));
        // return empty list
        doReturn(new AccessControlList()).when(amazonS3ClientMock).getObjectAcl("bucketA", "dir/");

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir");

        provider.checkAccess(file1, AccessMode.READ);
    }

    @Test
    public void checkAccessWrite() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", "content"}
        );

        FileSystem fs = createNewS3FileSystem();
        Path file1 = fs.getPath("/bucketA/dir/file");

        provider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessWriteWithoutPermission() throws IOException{
        Files.createDirectories(fsMem.getPath("/base", "bucketA", "dir"));
        AmazonS3ClientMock amazonS3ClientMock = mockFileSystem(fsMem.getPath("/base"));
        // return empty list
        doReturn(new AccessControlList()).when(amazonS3ClientMock).getObjectAcl("bucketA", "dir/");

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir");

        provider.checkAccess(file1, AccessMode.WRITE);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessExecute() throws IOException{
        createPathsAndMock(
                new String[]{"/bucketA/dir/file", "content"}
        );

        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file");

        provider.checkAccess(file1, AccessMode.EXECUTE);
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
    /**
     *
     * @param pathsAndContents string with two optional parameters, the first set the path and the second the content
     *                         if the path ends with slash '/' is a directory, otherwise is a file. If is a directory
     *                         not need to set the content
     * @return AmazonS3ClientMock internal spyed AmazonS3Client that you can fake it with mockito.
     */
    private AmazonS3ClientMock createPathsAndMock(String[] ... pathsAndContents ) {
        try {
            for (String[] pathAndContent : pathsAndContents){

                Path path = fsMem.getPath("/base" + pathAndContent[0]);
                if (pathAndContent[0].endsWith("/")) {
                    Files.createDirectories(path);
                }
                else{
                    Files.createDirectories(path.getParent());
                }

                if (pathAndContent.length == 2){
                    Files.write(path, pathAndContent[1].getBytes());
                }
            }
            return mockFileSystem(fsMem.getPath("/base"));

        }
        catch (IOException e){
            throw new RuntimeException("Test error", e);
        }
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     * @return FileSystem
     * @throws IOException
     */
    private FileSystem createNewS3FileSystem() throws IOException {
        return provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());
    }
}
