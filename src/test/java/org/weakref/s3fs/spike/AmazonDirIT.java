package org.weakref.s3fs.spike;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.weakref.s3fs.util.EnvironmentBuilder.getBucket;
import static org.weakref.s3fs.util.EnvironmentBuilder.getEndpoint;
import static org.weakref.s3fs.util.EnvironmentBuilder.getRealEnv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.UUID;

import org.junit.Test;
import org.weakref.s3fs.S3FileSystemProvider;
import org.weakref.s3fs.S3Path;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.weakref.s3fs.util.EnvironmentBuilder;

public class AmazonDirIT {

    private static final URI uri = URI.create("s3://"+ getEndpoint() + "/");

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
		
		FileSystem fileSystem = provider.newFileSystem(uri, getRealEnv());
		
		String name = UUID.randomUUID().toString();
		
		Path dir = fileSystem.getPath(getBucket(), name);
		
		Files.createDirectory(dir);
		
		assertTrue(Files.exists(dir));
		
		// añadimos mas ficheros dentro:
		
		Path dir2 = fileSystem.getPath(getBucket(), name);
		
		// como se si un fichero es directorio? en amazon pueden existir 
		// tanto como directorios como ficheros con el mismo nombre
		assertTrue(!Files.isDirectory(dir2));
		
		fileSystem.close();
	}
	
	@Test
	public void testCreatedFromAmazonWebConsoleNotExistKeyForFolder() throws IOException{
		S3FileSystemProvider provider = new S3FileSystemProvider();
		
		String folder = UUID.randomUUID().toString();
		String file1 = folder+"/file.html";
		
		FileSystem fileSystem = provider.newFileSystem(uri, getRealEnv());
		Path dir = fileSystem.getPath(getBucket(), folder);
		
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
