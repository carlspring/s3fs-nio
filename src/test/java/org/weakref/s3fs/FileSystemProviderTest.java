package org.weakref.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.weakref.s3fs.S3Path.forPath;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class FileSystemProviderTest {
	S3FileSystemProvider provider;
	
	@Before
	public void cleanup() throws IOException{
		try{
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException e){}
		// mock touch files
		provider = spy(new S3FileSystemProvider());
		doReturn(new Properties()).when(provider).loadAmazonProperties();
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

		assertEquals(path, forPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}
	
	@Test
	public void getPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3:///bucket/path/to/file"));

		assertEquals(path, forPath("/bucket/path/to/file"));
		assertSame(path.getFileSystem(), fs);
	}

	@Test
	public void getAnotherPath() throws IOException {
		FileSystem fs = FileSystems.newFileSystem(URI.create("s3://endpoint1/"),
				ImmutableMap.<String, Object> of());
		Path path = fs.provider().getPath(URI.create("s3://endpoint1/bucket/path/to/file"));

		assertEquals(path, forPath("/bucket/path/to/file"));
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
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
	}
	

}
