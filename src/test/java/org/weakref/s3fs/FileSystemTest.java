package org.weakref.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.weakref.s3fs.util.AmazonS3ClientMock;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class FileSystemTest {
	
	private FileSystem fs;
	private FileSystem fsMem;
	private S3FileSystemProvider provider;
	
	@Before
	public void cleanup() throws IOException{
		fsMem = MemoryFileSystemBuilder.newLinux().build("basescheme");
		
		try{
			FileSystems.getFileSystem(URI.create("s3:///")).close();
		}
		catch(FileSystemNotFoundException e){}
		
		provider = spy(new S3FileSystemProvider());
		doReturn(new Properties()).when(provider).loadAmazonProperties();
		
		Path bucketA = Files.createDirectories(fsMem.getPath("/base", "bucketA"));
		Path bucketB = Files.createDirectories(fsMem.getPath("/base", "bucketB"));
		Files.createFile(bucketA.resolve("file1"));
		Files.createFile(bucketB.resolve("file2"));
		
		mockFileSystem(fsMem.getPath("/base"));
		
		fs = provider.newFileSystem(URI.create("s3:///"), buildFakeEnv());
	}
	
	@After
	public void closeMemory() throws IOException{
		fsMem.close();
	}
	
	
	private void mockFileSystem(final Path memoryBucket){
		try {
			AmazonS3ClientMock clientMock = new AmazonS3ClientMock(memoryBucket);
			S3FileSystem s3ileS3FileSystem = new S3FileSystem(provider, clientMock, "endpoint");
			doReturn(s3ileS3FileSystem).when(provider).createFileSystem(any(URI.class), anyObject(), anyObject());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void getPath() throws IOException {
		
		assertEquals(fs.getPath("/bucket/path/to/file"),
				fs.getPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("/bucket", "path", "to", "file"),
				fs.getPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("bucket", "path", "to", "file"),
				fs.getPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("bucket", "path", "to", "dir/"),
				fs.getPath("/bucket/path/to/dir/"));
		assertEquals(fs.getPath("bucket", "path/", "to/", "dir/"),
				fs.getPath("/bucket/path/to/dir/"));
		assertEquals(fs.getPath("/bucket//path/to//file"),
				fs.getPath("/bucket/path/to/file"));
		assertEquals(fs.getPath("path/to//file"),
				fs.getPath("path/to/file"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void getPathWithoutBucket() {
		fs.getPath("//path/to/file");
	}
	
	@Test
	public void getRootDirectoriesReturnBuckets() {
		
		Iterable<Path> iterables = fs.getRootDirectories();
		
		assertNotNull(iterables);
		
		List<Path> buckets = Lists.newArrayList(iterables);
		
		assertEquals(2, buckets.size());
		//TODO: assertTrue(Files.isDirectory(buckets.get(0)));
		assertEquals("bucketA", buckets.get(0).getFileName().toString());
		
		//TODO: assertTrue(Files.isDirectory(buckets.get(1)));
		assertEquals("bucketB", buckets.get(1).getFileName().toString());
		
	}
	
	@Test
	public void close() throws IOException {
		
		assertTrue(fs.isOpen());
		
		fs.close();
		
		assertTrue(!fs.isOpen());
	}
	
	private Map<String, ?> buildFakeEnv(){
		return ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
	}
}
