package org.weakref.s3fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilesOperationsIT {
	
	private static final URI uri = URI.create("s3://s3-eu-west-1.amazonaws.com/");
	private static final URI uriDefaultEndpoint = URI.create("s3:///");
	private static final String bucket = "/test-storage-upplication"; 
	
	private FileSystem fileSystemAmazon;
	
	@Before
	public void setup() throws IOException{
		fileSystemAmazon = build();
	}
	
	@After
	public void close() throws IOException{
		fileSystemAmazon.close();
	}
	
	private static FileSystem build() throws IOException{
		
		try {
			return FileSystems.getFileSystem(uriDefaultEndpoint);
		} catch(FileSystemNotFoundException e){
			final Properties props = new Properties();
			props.load(FilesOperationsIT.class.getResourceAsStream("/amazon-test.properties"));
			return FileSystems.newFileSystem(URI.create("s3://s3-eu-west-1.amazonaws.com/"), 
					new HashMap<String, Object>(){{
						put("access-key", props.getProperty("accessKeyId"));
						put("secret-key", props.getProperty("secretKey"));
					}}, FilesOperationsIT.class.getClassLoader());
		}
	}
	
	@Test
	public void buildEnv() throws IOException{
		FileSystem fileSystem = FileSystems.getFileSystem(uri);
		assertSame(fileSystemAmazon, fileSystem);
	}
	
	@Test
	public void buildEnvAnotherURIReturnSame() throws IOException{
		FileSystem fileSystem = FileSystems.getFileSystem(uriDefaultEndpoint);
		assertSame(fileSystemAmazon, fileSystem);
	}
	
	@Test
	public void buildEnvWithoutEndPointReturnSame() throws IOException{
		FileSystem fileSystem = FileSystems.getFileSystem(uriDefaultEndpoint);
		FileSystem fileSystem2 = FileSystems.getFileSystem(uri);
		assertSame(fileSystem2, fileSystem);
	}
	@Test
	public void notExistsDir() throws IOException{

		Path dir = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + "/");
		assertTrue(!Files.exists(dir));
	}
	
	@Test
	public void notExistsFile() throws IOException{

		Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());
		assertTrue(!Files.exists(file));
	}
	
	@Test
	public void existsFile() throws IOException{

		Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());
		
		
		EnumSet<StandardOpenOption> options =
	            EnumSet.<StandardOpenOption>of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
	    Files.newByteChannel(file, options).close();
	
		assertTrue(Files.exists(file));
	}
	
	@Test
	public void createEmptyDir() throws IOException{
		
		Path dir = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + "/");
		
		Files.createDirectory(dir);
		
		assertTrue(Files.exists(dir));
		assertTrue(Files.isDirectory(dir));
	}
	
	@Test
	public void createEmptyFile() throws IOException{
		
		Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());
		
		Files.createFile(file);
		
		assertTrue(Files.exists(file));
		assertTrue(Files.isRegularFile(file));
	}
	
	@Test
	public void createTempFile() throws IOException{
		
		Path dir = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + "/");
		
		Files.createDirectory(dir);
		
		Path file = Files.createTempFile(dir, "file", "temp");
		
		assertTrue(Files.exists(file));
	}
	
	@Test
	public void createTempDir() throws IOException{

		Path dir = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + "/");
		
		Files.createDirectory(dir);
		
		Path dir2 = Files.createTempDirectory(dir, "dir-temp");
		
		assertTrue(Files.exists(dir2));
	}
	
	@Test
	public void testCopyDir() throws IOException, URISyntaxException {

		Path dir = uploadDir();

		assertTrue(Files.exists(dir.resolve("assets1/")));
		assertTrue(Files.exists(dir.resolve("assets1/").resolve("index.html")));
		assertTrue(Files.exists(dir.resolve("assets1/").resolve("img").resolve("Penguins.jpg")));
		assertTrue(Files.exists(dir.resolve("assets1/").resolve("js").resolve("main.js")));
	}
	
	@Test
	public void testDeleteFullDir() throws IOException, URISyntaxException {

		Path dir = uploadDir();
		
		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {

    	    @Override
    	    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
    	        Files.delete(file);
    	        return FileVisitResult.CONTINUE;
    	    }

    	    @Override
    	    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
    	        if(exc == null){
    	            Files.delete(dir);
    	            return FileVisitResult.CONTINUE;
    	        }
    	        throw exc;
    	    }
    	});
		
		assertTrue(!Files.exists(dir));
		
	}
	
	@Test
	public void copyUpload() throws URISyntaxException, IOException{
		Path result = uploadSingleFile();
		
		assertTrue(Files.exists(result));
		assertArrayEquals(Files.readAllBytes(Paths.get(this.getClass().getResource("/dirFile/assets1/index.html")
				.toURI())), Files.readAllBytes(result));
	}

	
	
	@Test
	public void copyDownload() throws IOException, URISyntaxException{
		Path result = uploadSingleFile();
		
		Path localResult = Files.createTempDirectory("temp-local-file");
		Path notExistLocalResult = localResult.resolve("result");
		
		Files.copy(result, notExistLocalResult);
		
		assertTrue(Files.exists(notExistLocalResult));
		
		assertArrayEquals(Files.readAllBytes(result), Files.readAllBytes(notExistLocalResult));
		
	}
	
	private Path uploadSingleFile() throws IOException, URISyntaxException {
		
		Path localFile =Paths.get(this.getClass().getResource("/dirFile/assets1/index.html")
				.toURI());
		
		Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());
		
		Files.copy(localFile, result);
		return result;
	}
	
	private Path uploadDir() throws IOException, URISyntaxException {

		Path dir = fileSystemAmazon.getPath(bucket, "0000example" + UUID.randomUUID().toString() + "/");

		Path dirPath = Paths.get(this.getClass().getResource("/dirFile")
				.toURI());
		
		Files.exists(dir);

		Files.walkFileTree(dirPath, new CopyDirVisitor(dirPath, dir));
		return dir;
	}

	/**
	 * Created by IntelliJ IDEA. User: bbejeck Date: 1/23/12 Time: 10:29 PM
	 */
	public static class CopyDirVisitor extends SimpleFileVisitor<Path> {

		private Path fromPath;
		private Path toPath;
		private StandardCopyOption copyOption;

		public CopyDirVisitor(Path fromPath, Path toPath,
				StandardCopyOption copyOption) {
			this.fromPath = fromPath;
			this.toPath = toPath;
			this.copyOption = copyOption;
		}

		public CopyDirVisitor(Path fromPath, Path toPath) {
			this(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);
		}

		@Override
		public FileVisitResult preVisitDirectory(Path dir,
				BasicFileAttributes attrs) throws IOException {

			// permitimos resolver entre distintos providers
			Path targetPath = appendPath(dir);

			if (!Files.exists(targetPath)) {
				if (!targetPath.getFileName().toString().endsWith("/")){
					targetPath = targetPath.getParent().resolve(targetPath.getFileName().toString() + "/");
				}
				Files.createDirectory(targetPath);
			}
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
				throws IOException {

			Path targetPath = appendPath(file);

			Files.copy(file, targetPath, copyOption);
			return FileVisitResult.CONTINUE;
		}

		/**
		 * Obtenemos el path que corresponde en el parametro: {@link #fromPath}
		 * relativo al parametro <code>Path to</code>
		 * 
		 * @param to
		 *            Path
		 * @return
		 */
		private Path appendPath(Path to) {
			Path targetPath = toPath;
			// sacamos el path relativo y lo recorremos para
			// añadirlo al nuevo

			for (Path path : fromPath.relativize(to)) {
				// si utilizamos path en vez de string: lanza error por ser
				// distintos paths
				targetPath = targetPath.resolve(path.getFileName().toString());
			}

			return targetPath;
		}
	}
}
