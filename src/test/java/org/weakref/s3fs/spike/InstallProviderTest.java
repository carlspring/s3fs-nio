package org.weakref.s3fs.spike;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.*;

import org.junit.Test;
import org.weakref.s3fs.S3FileSystemProvider;

public class InstallProviderTest {
	
	@Test
	public void useZipProvider() throws IOException{
		
		Path path = createZipTempFile();
		String pathFinal = pathToString(path);
		
		FileSystem fs = FileSystems.newFileSystem(URI.create("jar:file:" + pathFinal), new HashMap<String,Object>(), this.getClass().getClassLoader());
		
		Path zipPath = fs.getPath("test.zip");
	
		assertNotNull(zipPath);
		assertNotNull(zipPath.getFileSystem());
		assertNotNull(zipPath.getFileSystem().provider());
		//assertTrue(zipPath.getFileSystem().provider() instanceof com.sun.nio.zipfs.ZipFileSystemProvider);
	}
	
	@Test(expected = FileSystemNotFoundException.class)
	public void useZipProviderPathNotExists() throws IOException{
		FileSystems.newFileSystem(URI.create("jar:file:/not/exists/zip.zip"), new HashMap<String,Object>(), this.getClass().getClassLoader());
	}

	
	@Test
	public void useAlternativeZipProvider() throws IOException{
		
		Path path = createZipTempFile();
		String pathFinal = pathToString(path);
		
		FileSystem fs = FileSystems.newFileSystem(URI.create("zipfs:file:" + pathFinal), new HashMap<String,Object>(), this.getClass().getClassLoader());
		
		Path zipPath = fs.getPath("test.zip");
	
		assertNotNull(zipPath);
		assertNotNull(zipPath.getFileSystem());
		assertNotNull(zipPath.getFileSystem().provider());
		assertTrue(zipPath.getFileSystem().provider() instanceof com.github.marschall.com.sun.nio.zipfs.ZipFileSystemProvider);
	}
	
	@Test
	public void useS3Provider() throws IOException{
		URI uri = URI.create("s3:///hola/que/tal/");
		FileSystem fs = null;
		try{
			fs = FileSystems.newFileSystem(uri, new HashMap<String,Object>(), this.getClass().getClassLoader());
		}
		catch(FileSystemAlreadyExistsException e){
			fs = FileSystems.getFileSystem(uri);
		}

		Path zipPath = fs.getPath("test.zip");
		assertNotNull(zipPath);
		assertNotNull(zipPath.getFileSystem());
		assertNotNull(zipPath.getFileSystem().provider());
		assertTrue(zipPath.getFileSystem().provider() instanceof S3FileSystemProvider);
	}
	
	private Path createZipTempFile() throws IOException{
		File zip = Files.createTempFile("temp", ".zip").toFile();
		
		try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zip))){
			ZipEntry entry = new ZipEntry("text.txt");
			out.putNextEntry(entry);
		}
		
		return zip.toPath();
	}

	private String pathToString(Path pathNext) {
		StringBuilder pathFinal = new StringBuilder();
		
		for (; pathNext.getParent() != null; pathNext = pathNext.getParent()){
			pathFinal.insert(0,  "/" + pathNext.getFileName().toString());
		}
		return pathFinal.toString();
	}
	
}
