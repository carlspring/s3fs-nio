package org.weakref.s3fs.util;

import static org.weakref.s3fs.S3FileSystemProvider.ACCESS_KEY;
import static org.weakref.s3fs.S3FileSystemProvider.SECRET_KEY;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;

public abstract class EnvironmentBuilder {
	/**
	 * Get credentials from environment vars, and if not found from amazon-test.properties
	 * @return Map with the credentials
	 */
	public static Map<String, Object> getRealEnv(){
		Map<String, Object> env = null;
		
		String accessKey = System.getenv(ACCESS_KEY);
		String secretKey = System.getenv(SECRET_KEY);
		
		if (accessKey != null && secretKey != null){
			env = ImmutableMap.<String, Object> builder()
				.put(ACCESS_KEY, accessKey)
				.put(SECRET_KEY, secretKey).build();
		}
		else{
			final Properties props = new Properties();
			try {
				props.load(EnvironmentBuilder.class.getResourceAsStream("/amazon-test.properties"));
			} catch (IOException e) {
				throw new RuntimeException("not found amazon-test.properties in the classpath", e);
			}
			env = ImmutableMap.<String, Object> builder()
					.put(ACCESS_KEY, props.getProperty(ACCESS_KEY))
					.put(SECRET_KEY, props.getProperty(SECRET_KEY)).build();
		}
		
		return env;
	}
}
