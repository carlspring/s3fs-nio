package org.weakref.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.weakref.s3fs.S3Path.forPath;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class FileSystemProviderTest {
	
	@Before
	public void cleanup() throws IOException{
		try{
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException e){
			
		}
	}
	
	@Test
	public void testCreatesAuthenticated() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		Map<String, ?> env = buildFakeEnv();

		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				env);

		assertNotNull(fileSystem);
	}

	@Test
	public void testCreatesAnonymous() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();

		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());

		assertNotNull(fileSystem);
	}

	@Test(expected = FileSystemAlreadyExistsException.class)
	public void testCreateFailsIfAlreadyCreated() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();

		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
	}

	@Test
	public void testGetFileSystem() throws IOException {
		S3FileSystemProvider provider = new S3FileSystemProvider();

		FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);

		FileSystem other = provider.getFileSystem(URI.create("s3:///"));
		assertSame(fileSystem, other);
	}

	@Test(expected = FileSystemNotFoundException.class)
	public void testGetFailsIfNotYetCreated() {
		S3FileSystemProvider provider = new S3FileSystemProvider();
		provider.getFileSystem(URI.create("s3:///"));
	}

	@Test
	public void testGetPathWithEmtpyEndpoint() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, forPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}
	
	@Test
	public void testGetPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, forPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void testGetPath2() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, forPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testGetPathWithInvalidEndpoint () throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		fs.provider().getPath(URI.create("s3://endpoint2/bucket/path/to/file"));
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
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
	}
	

}
