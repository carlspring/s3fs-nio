package org.weakref.s3fs;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
/**
 * utility for build a {@link S3FileSystem}
 * @author Javier Arnáiz González, javier[at]upplication.co
 *
 */
public class S3FileSystemBuilder {
	
	private URI uri;
	
	/**
	 * S3FileSystemBuilder with the default endpoint (s3.amazonaws.com)
	 * @return S3FileSystemBuilder
	 */
	public static S3FileSystemBuilder newDefault(){
		return new S3FileSystemBuilder(URI.create("s3:///"));
	}
	/**
	 * S3FileSystemBuilder with endpoint
	 * @param endpoint String with not slash. (s3-eu-west-1.amazonaws.com or s3-us-west-1.amazonaws.com ...)
	 * @see http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
	 * @return S3FileSystemBuilder
	 */
	public static S3FileSystemBuilder newEndpoint(String endpoint){
		return new S3FileSystemBuilder(URI.create("s3://" + endpoint + "/"));
	}
	
	private S3FileSystemBuilder(URI uri){
		this.uri = uri;
	}
	/**
	 * register new fileSystem with a acessKey and a secretKey
	 * @param accessKey
	 * @param secretKey
	 * @return
	 * @throws IOException
	 */
	public FileSystem build(String accessKey, String secretKey) throws IOException {
		 Map<String, ?> env = ImmutableMap
					.<String, Object> builder()
					.put(S3FileSystemProvider.ACCESS_KEY, accessKey)
					.put(S3FileSystemProvider.SECRET_KEY, secretKey).build();
	    ClassLoader classLoader = S3FileSystemProvider.class.getClassLoader();
	    	    
	    return FileSystems.newFileSystem(uri, env, classLoader);
	}
}
