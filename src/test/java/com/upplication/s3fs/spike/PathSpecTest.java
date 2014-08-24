package com.upplication.s3fs.spike;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;

import static org.junit.Assert.*;

public class PathSpecTest {

	FileSystem fs;

	@Before
	public void setup() throws IOException {
		fs = MemoryFileSystemBuilder.newLinux().build("linux");
	}

	@After
	public void close() throws IOException {
		fs.close();
	}

    // first and more

    @Test
    public void firstAndMore(){
        assertEquals(fs.getPath("/dir","dir", "file"), fs.getPath("/dir", "dir/file"));
        assertEquals(fs.getPath("/dir/dir/file"), fs.getPath("/dir", "dir/file"));
    }

	// absolute relative

	@Test
	public void relative() {
		assertTrue(!get("file").isAbsolute());
	}

	@Test
	public void absolute() {
		assertTrue(get("/file/file2").isAbsolute());
	}

	// test starts with

	@Test
	public void startsWith() {
		assertTrue(get("/file/file1").startsWith(get("/file")));
	}

	@Test
	public void startsWithBlank() {
		assertFalse(get("/file").startsWith(get("")));
	}
	
	@Test
   	public void startsWithBlankRelative(){
		assertFalse(get("file1").startsWith(get("")));
	}
      

	@Test
	public void startsWithBlankBlank() {
		assertTrue(get("").startsWith(get("")));
	}

	@Test
	public void startsWithRelativeVsAbsolute() {
		assertFalse(get("/file/file1").startsWith(get("file")));
	}

	@Test
	public void startsWithFalse() {
		assertFalse(get("/file/file1").startsWith(get("/file/file1/file2")));
		assertTrue(get("/file/file1/file2").startsWith(get("/file/file1")));
	}

	@Test
	public void startsWithNotNormalize() {
		assertFalse(get("/file/file1/file2").startsWith(get("/file/file1/../")));
	}

	@Test
	public void startsWithNormalize() {
		assertTrue(get("/file/file1/file2").startsWith(
				get("/file/file1/../").normalize()));
	}

	@Test
	public void startsWithRelative() {
		assertTrue(get("file/file1").startsWith(get("file")));
	}

	@Test
	public void startsWithString() {
		assertTrue(get("/file/file1").startsWith("/file"));
	}

	// ends with

	@Test
	public void endsWithAbsoluteRelative() {
		assertTrue(get("/file/file1").endsWith(get("file1")));
	}

	@Test
	public void endsWithAbsoluteAbsolute() {
		assertTrue(get("/file/file1").endsWith(get("/file/file1")));
	}

	@Test
	public void endsWithRelativeRelative() {
		assertTrue(get("file/file1").endsWith(get("file1")));
	}

	@Test
	public void endsWithRelativeAbsolute() {
		assertFalse(get("file/file1").endsWith(get("/file")));
	}

	@Test
	public void endsWithDifferenteFileSystem() {
		assertFalse(get("/file/file1").endsWith(Paths.get("/file/file1")));
	}

	@Test
	public void endsWithBlankRelativeAbsolute() {
		assertFalse(get("").endsWith(get("/bucket")));
	}
	
	@Test
	public void endsWithBlankBlank() {
		assertTrue(get("").endsWith(get("")));
	}

	@Test
	public void endsWithRelativeBlankAbsolute() {
		assertFalse(get("/bucket/file1").endsWith(get("")));
	}
	
	@Test
 	public void endsWithRelativeBlankRelative(){
 		assertFalse(get("file1").endsWith(get("")));
 	}

    // file name

    @Test
    public void getFileName() throws IOException {
        try (FileSystem windows = MemoryFileSystemBuilder.newWindows().build("widows")){
            Path fileName = windows.getPath("C:/file").getFileName();
            Path rootName = windows.getPath("C:/").getFileName();

            assertEquals(windows.getPath("file"), fileName);
            assertNull(rootName);
        }
    }

	// ~ helpers methods

	private Path get(String path) {
		return fs.getPath(path);
	}
}
