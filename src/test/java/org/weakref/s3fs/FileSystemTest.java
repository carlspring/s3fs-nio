package org.weakref.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.weakref.s3fs.S3Path.forPath;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class FileSystemTest {
	
	private FileSystem fs;
	
	@Before
	public void setup() throws IOException{
		try {
			fs = FileSystems.getFileSystem(URI.create("s3:///"));
		
		} catch(FileSystemNotFoundException e){
			fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
		}
	}

	@Test
	public void getPath() {
		
		assertEquals(fs.getPath("/bucket/path/to/file"),
				forPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("/bucket", "path", "to", "file"),
				forPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("bucket", "path", "to", "file"),
				forPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("bucket", "path", "to", "dir/"),
				forPath("/bucket/path/to/dir/"));
		assertEquals(fs.getPath("bucket", "path/", "to/", "dir/"),
				forPath("/bucket/path/to/dir/"));
		assertEquals(fs.getPath("/bucket//path/to//file"),
				forPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("path/to//file"),
				forPath("path/to/file"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void getPathWithoutBucket() {
		fs.getPath("//path/to/file");
	}
	
	
	@Test
	public void close() throws IOException {
		
		assertTrue(fs.isOpen());
		
		fs.close();
		
		assertTrue(!fs.isOpen());
	}
}
