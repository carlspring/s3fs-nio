package com.upplication.s3fs;
import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.S3UnitTest.S3_GLOBAL_URI;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.util.EnvironmentBuilder;

public class FileSystemProviderIT {
	S3FileSystemProvider provider;
	
	@Before
	public void setup() throws IOException{
		System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
		try {
			FileSystems.getFileSystem(S3_GLOBAL_URI).close();
		}
		catch(FileSystemNotFoundException e){
			// ignore this
		}
		provider = spy(new S3FileSystemProvider());
	}
	
	@Test
	public void createAuthenticatedByProperties() throws IOException{

		URI uri = URI.create("s3://yadi/");
		
		FileSystem fileSystem = provider.newFileSystem(uri, null);
		assertNotNull(fileSystem);
		
		verify(provider).createFileSystem(eq(uri), eq(buildFakeProps("access key for test", "secret key for test")));
	}
	
	
	@Test
	public void createsAuthenticatedByEnvOverridesProps() throws IOException {
		
		Map<String, ?> env = buildFakeEnv();
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, env);

		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(S3_GLOBAL_URI), eq(buildFakeProps((String) env.get(ACCESS_KEY), (String) env.get(SECRET_KEY))));
	}

	@Test
	public void createsAnonymousNotPossible() throws IOException {
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(S3_GLOBAL_URI), eq(buildFakeProps("access key for test", "secret key for test")));
	}
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
			.put(ACCESS_KEY, "access key")
			.put(SECRET_KEY, "secret key").build();
	}

	private Properties buildFakeProps(String access_key, String secret_key) {
		Properties props = new Properties();
		props.setProperty(ACCESS_KEY, access_key);
		props.setProperty(SECRET_KEY, secret_key);
		props.setProperty(EnvironmentBuilder.BUCKET_NAME_KEY,"/your-bucket-name for test");
		return props;
	}
}