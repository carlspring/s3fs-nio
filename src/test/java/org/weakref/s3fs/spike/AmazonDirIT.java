package org.weakref.s3fs.spike;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.weakref.s3fs.FilesOperationsIT;
import org.weakref.s3fs.S3FileAttributes;
import org.weakref.s3fs.S3FileSystemProvider;
import org.weakref.s3fs.S3Path;
import org.weakref.s3fs.util.CopyDirVisitor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public class AmazonDirIT {
	
	private static final URI uri = URI.create("s3://s3-eu-west-1.amazonaws.com/");
	private static final URI uriDefaultEndpoint = URI.create("s3:///");
	private static final String bucket = "/test-storage-upplication"; 
	
	
	private Map<String, ? extends Object> buildEnv() throws IOException{
		final Properties props = new Properties();
		props.load(FilesOperationsIT.class.getResourceAsStream("/amazon-test.properties"));
		return new HashMap<String, Object>(){{
			put("access-key", props.getProperty("accessKeyId"));
			put("secret-key", props.getProperty("secretKey"));
		}};
	}
	
	@Test
	public void createDirWithoutEndSlash() throws IOException{
		
		S3FileSystemProvider provider = new S3FileSystemProvider(){
			/**
			 * Nueva implementación: probamos si funcionaria
			 */
			@Override
			public void createDirectory(Path dir, FileAttribute<?>... attrs)
					throws IOException {
				S3Path s3Path = (S3Path) dir;
				
				Preconditions.checkArgument(attrs.length == 0,
						"attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO

				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(0);

				s3Path.getFileSystem()
						.getClient()
						.putObject(s3Path.getBucket(), s3Path.getKey(),
								new ByteArrayInputStream(new byte[0]), metadata);
			}
		};
		
		FileSystem fileSystem = provider.newFileSystem(uri, buildEnv());
		
		String name = UUID.randomUUID().toString();
		
		Path dir = fileSystem.getPath(bucket, name);
		
		Files.createDirectory(dir);
		
		assertTrue(Files.exists(dir));
		
		// añadimos mas ficheros dentro:
		
		Path dir2 = fileSystem.getPath(bucket, name);
		
		// como se si un fichero es directorio? en amazon pueden existir 
		// tanto como directorios como ficheros con el mismo nombre
		assertTrue(!Files.isDirectory(dir2));
		
		fileSystem.close();
	}
	
	@Test
	public void testCreatedFromAmazonWebConsoleNotExistkeyForFolder() throws IOException{
		S3FileSystemProvider provider = new S3FileSystemProvider();
		
		String folder = UUID.randomUUID().toString();
		String file1 = folder+"/file.html";
		
		FileSystem fileSystem = provider.newFileSystem(uri, buildEnv());
		Path dir = fileSystem.getPath(bucket, folder);
		
		S3Path s3Path = (S3Path)dir;
		// subimos un fichero sin sus paths
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		s3Path.getFileSystem().getClient().putObject(s3Path.getBucket(), file1,
				new ByteArrayInputStream(new byte[0]), metadata);
		
		// para amazon no existe el path: folder
		try{
			s3Path.getFileSystem().getClient().getObjectMetadata(s3Path.getBucket(), s3Path.getKey());
			assertTrue(false);
		}
		catch(AmazonS3Exception e){
			assertEquals(404, e.getStatusCode());
		}
	}
}
