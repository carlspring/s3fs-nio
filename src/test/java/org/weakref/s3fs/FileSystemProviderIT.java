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

public class FileSystemProviderIT {
	S3FileSystemProvider provider;
	
	@Before
	public void cleanup() throws IOException{
		try{
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException e){}
		
		provider = spy(new S3FileSystemProvider());
	}
	
	@Test
	public void createAuthenticatedByProperties() throws IOException{

		URI uri = URI.create("s3:///");
		
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		
		verify(provider).createFileSystem(eq(uri), eq("access key for test"), eq("secret key for test"));
	}
	
	
	@Test
	public void createsAuthenticatedByEnvOverridesProps() throws IOException {
		
		Map<String, ?> env = buildFakeEnv();
		URI uri = URI.create("s3:///");
		
		FileSystem fileSystem = provider.newFileSystem(uri,
				env);

		assertNotNull(fileSystem);	
		verify(provider).createFileSystem(eq(uri),eq(env.get(S3FileSystemProvider.ACCESS_KEY)), eq(env.get(S3FileSystemProvider.SECRET_KEY)));
	}

	@Test
	public void createsAnonymousNotPossible() throws IOException {
		URI uri = URI.create("s3:///");
		FileSystem fileSystem = provider.newFileSystem(uri, ImmutableMap.<String, Object> of());

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(uri), eq("access key for test"), eq("secret key for test"));
	}
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
	}
	

}
