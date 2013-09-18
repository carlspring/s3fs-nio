package org.weakref.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.weakref.s3fs.S3Path.forPath;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.weakref.s3fs.util.AmazonS3ClientMock;

import com.google.common.collect.ImmutableMap;

public class FileSystemTest {
	
	private FileSystem fs;
	
	@Before
	public void setup() throws IOException {
		
		S3FileSystemProvider provider = spy(new S3FileSystemProvider());
		
		AmazonS3ClientMock clientMock = new AmazonS3ClientMock();
		S3FileSystem s3ileS3FileSystem = new S3FileSystem(provider, clientMock, "endpoint");
		doReturn(s3ileS3FileSystem).when(provider).createFileSystem(any(URI.class), anyObject(), anyObject());
				
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
	
	@Test
	public void readOnlyAlwaysFalse(){
		assertTrue(!fs.isReadOnly());
	}
	
	@Test
	public void getSeparatorSlash(){
		assertEquals("/", fs.getSeparator());
		assertEquals("/", S3Path.PATH_SEPARATOR);
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void getPathMatcherThrowException(){
		fs.getPathMatcher("");
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void getUserPrincipalLookupServiceThrowException(){
		fs.getUserPrincipalLookupService();
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void newWatchServiceThrowException() throws Exception {
		fs.newWatchService();
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void getPathWithoutBucket() {
		fs.getPath("//path/to/file");
	}
	
	@Test
	public void getFileStoresReturnEmptyList(){
		Iterable<FileStore> result = fs.getFileStores();
		
		assertNotNull(result);
		assertNotNull(result.iterator());
		assertTrue(!result.iterator().hasNext());
	}
	
	@Test
	public void supportedFileAttributeViewsReturnBasic(){
		Set<String> operations = fs.supportedFileAttributeViews();
		
		assertNotNull(operations);
		assertTrue(!operations.isEmpty());
		
		for (String operation: operations){
			assertEquals("basic", operation);
		}
	}
	
	@Test
	public void getRootDirectories(){
		fs.getRootDirectories();
	}
	
	@Test
	public void close() throws IOException {
		assertTrue(fs.isOpen());
		fs.close();
		assertTrue(!fs.isOpen());
	}
}
