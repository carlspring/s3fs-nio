package com.upplication.s3fs;
import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.S3UnitTestBase.S3_GLOBAL_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
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
import org.mockito.ArgumentMatcher;

public class FileSystemProviderIT {

	private S3FileSystemProvider provider;
	
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
        doReturn(buildFakeProps()).when(provider).loadAmazonProperties();
	}
	
	@Test
	public void createAuthenticatedByProperties(){

		URI uri = URI.create("s3://yadi/");
		
		FileSystem fileSystem = provider.newFileSystem(uri, null);
		assertNotNull(fileSystem);
		
		verify(provider).createFileSystem(eq(uri), eq(buildFakeProps()));
	}
	
	
	@Test
	public void createsAuthenticatedByEnvOverridesProps() {
		
		final Map<String, String> env = buildFakeEnv();
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, env);

		assertNotNull(fileSystem);
        Properties props = new Properties();
        props.putAll(env);
		verify(provider).createFileSystem(eq(S3_GLOBAL_URI), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties)argument;
                assertEquals(env.get(ACCESS_KEY), called.get(ACCESS_KEY));
                assertEquals(env.get(SECRET_KEY), called.get(SECRET_KEY));
                return true;
            }
        }));
	}

	@Test
	public void createsAnonymousNotPossible() {
		FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, ImmutableMap.<String, Object> of());
		assertNotNull(fileSystem);
		verify(provider).createFileSystem(eq(S3_GLOBAL_URI), eq(buildFakeProps()));
	}
	
	private Map<String, String> buildFakeEnv(){
		return ImmutableMap.<String, String> builder()
			.put(ACCESS_KEY, "access-key")
			.put(SECRET_KEY, "secret-key").build();
	}

	private Properties buildFakeProps() {
        try {
            Properties props = new Properties();
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon-test-sample.properties"));
            return props;
        }
        catch (IOException e){
            throw new RuntimeException("amazon-test.properties not present");
        }
	}
}