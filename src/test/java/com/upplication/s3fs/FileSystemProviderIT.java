package com.upplication.s3fs;
import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.S3UnitTestBase.S3_GLOBAL_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

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
import org.mockito.ArgumentMatcher;

public class FileSystemProviderIT {

	private S3FileSystemProvider provider;
	
	@Before
	public void setup() throws IOException {
		System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);
		try {
			FileSystems.getFileSystem(S3_GLOBAL_URI).close();
		}
		catch(FileSystemNotFoundException e) {
			// ignore this
		}
		provider = spy(new S3FileSystemProvider());
        doReturn(buildFakeProps()).when(provider).loadAmazonProperties();
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(false).when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());
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
		provider.newFileSystem(S3_GLOBAL_URI, env);

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
    public void createsAuthenticatedBySystemProps() {

        doCallRealMethod().when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        final String propAccessKey = "env-access-key";
        final String propSecretKey = "env-secret-key";
        doReturn(propAccessKey).when(provider).systemGetEnv(eq(ACCESS_KEY));
        doReturn(propSecretKey).when(provider).systemGetEnv(eq(SECRET_KEY));

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, null);
        assertNotNull(fileSystem);
        verify(provider).createFileSystem(eq(S3_GLOBAL_URI), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties)argument;
                assertEquals(propAccessKey, called.get(ACCESS_KEY));
                assertEquals(propSecretKey, called.get(SECRET_KEY));
                return true;
            }
        }));
    }

    @Test
    public void createsAuthenticatedBySystemEnv() {

        doCallRealMethod().when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());

        final String propAccessKey = "prop-access-key";
        final String propSecretKey = "prop-secret-key";
        System.setProperty(ACCESS_KEY, propAccessKey);
        System.setProperty(SECRET_KEY, propSecretKey);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI, null);
        assertNotNull(fileSystem);
        verify(provider).createFileSystem(eq(S3_GLOBAL_URI), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties)argument;
                assertEquals(propAccessKey, called.get(ACCESS_KEY));
                assertEquals(propSecretKey, called.get(SECRET_KEY));
                return true;
            }
        }));
    }

    @Test
    public void createsAuthenticatedByUri() {
        final String accessKeyUri = "access-key-uri";
        final String secretKeyUri = "secret-key-uri";
        URI uri = URI.create("s3://" + accessKeyUri + ":" + secretKeyUri + "@s3.amazon.com");

        provider.newFileSystem(uri, null);

        verify(provider).createFileSystem(eq(uri), argThat(new ArgumentMatcher<Properties>() {
            @Override
            public boolean matches(Object argument) {
                Properties called = (Properties)argument;
                assertEquals(accessKeyUri, called.get(ACCESS_KEY));
                assertEquals(secretKeyUri, called.get(SECRET_KEY));
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