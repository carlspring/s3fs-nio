package org.weakref.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.weakref.s3fs.S3Path.forPath;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class S3PathTest {
	
	@BeforeClass
	public static void setup() throws IOException {
		try {
			FileSystems.getFileSystem(URI.create("s3:///"));
		} catch(FileSystemNotFoundException e) {
			FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
		}
	}
	
    @Test
    public void createNoPath() {
        S3Path path = forPath("/bucket");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "");
    }

    @Test
    public void createWithTrailingSlash() {
        S3Path path = forPath("/bucket/");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "");
    }

    @Test
    public void createWithPath() {
        S3Path path = forPath("/bucket/path/to/file");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "path/to/file");
    }

    @Test
    public void createWithPathAndTrailingSlash() {
        S3Path path = forPath("/bucket/path/to/file/");

        assertEquals("bucket", path.getBucket());
        assertEquals("path/to/file", path.getKey());
    }

    @Test
    public void createRelative() {
        S3Path path = forPath("path/to/file");
        assertNull(path.getBucket());
        assertEquals(path.getKey(), "path/to/file");
        assertFalse(path.isAbsolute());
    }

    @Test
    public void getParent() {
        assertEquals(forPath("/bucket/path/to/file").getParent(), forPath("/bucket/path/to/"));
        assertEquals(forPath("/bucket/path/to/file/").getParent(), forPath("/bucket/path/to/"));
        assertNull(forPath("/bucket/").getParent());
        assertNull(forPath("/bucket").getParent());
    }
    
    @Test
    public void nameCount() {
        assertEquals(forPath("/bucket/path/to/file").getNameCount(), 3);
        assertEquals(forPath("/bucket/").getNameCount(), 0);
    }
    
    @Test
    public void resolve() {
        assertEquals(forPath("/bucket/path/to/dir/").resolve(forPath("child/xyz")), forPath("/bucket/path/to/dir/child/xyz"));
        assertEquals(forPath("/bucket/path/to/dir").resolve(forPath("child/xyz")), forPath("/bucket/path/to/dir/child/xyz"));
        assertEquals(forPath("/bucket/path/to/file").resolve(forPath("")), forPath("/bucket/path/to/file"));  // TODO: should this be "path/to/dir/"
        assertEquals(forPath("path/to/file").resolve(forPath("child/xyz")), forPath("path/to/file/child/xyz"));
        assertEquals(forPath("path/to/file").resolve(forPath("")), forPath("path/to/file")); // TODO: should this be "path/to/dir/"
        assertEquals(forPath("/bucket/path/to/file").resolve(forPath("/bucket2/other/child")), forPath("/bucket2/other/child"));
    }
    
    @Test
    public void name() {
        assertEquals(forPath("/bucket/path/to/file").getName(0), forPath("path/"));
        assertEquals(forPath("/bucket/path/to/file").getName(1), forPath("to/"));
        assertEquals(forPath("/bucket/path/to/file").getName(2), forPath("file"));
    }

    @Test
    public void subPath() {
        assertEquals(forPath("/bucket/path/to/file").subpath(0, 1), forPath("path/"));
        assertEquals(forPath("/bucket/path/to/file").subpath(0, 2), forPath("path/to/"));
        assertEquals(forPath("/bucket/path/to/file").subpath(0, 3), forPath("path/to/file"));
        assertEquals(forPath("/bucket/path/to/file").subpath(1, 2), forPath("to/"));
        assertEquals(forPath("/bucket/path/to/file").subpath(1, 3), forPath("to/file"));
        assertEquals(forPath("/bucket/path/to/file").subpath(2, 3), forPath("file"));
    }
    
    @Test
    public void iterator() {
        Iterator<Path> iterator = forPath("/bucket/path/to/file").iterator();
        
        assertEquals(iterator.next(), forPath("path/"));
        assertEquals(iterator.next(), forPath("to/"));
        assertEquals(iterator.next(), forPath("file"));
    }
    
    @Test
    public void resolveSibling() {
        // absolute (non-root) vs...
        assertEquals(forPath("/bucket/path/to/file").resolveSibling(forPath("other/child")), forPath("/bucket/path/to/other/child"));
        assertEquals(forPath("/bucket/path/to/file").resolveSibling(forPath("/bucket2/other/child")), forPath("/bucket2/other/child"));
        assertEquals(forPath("/bucket/path/to/file").resolveSibling(forPath("")), forPath("/bucket/path/to/"));

        // absolute (root) vs ...
        assertEquals(forPath("/bucket").resolveSibling(forPath("other/child")), forPath("other/child"));
        assertEquals(forPath("/bucket").resolveSibling(forPath("/bucket2/other/child")), forPath("/bucket2/other/child"));
        assertEquals(forPath("/bucket").resolveSibling(forPath("")), forPath(""));

        // relative (empty) vs ...
        assertEquals(forPath("").resolveSibling(forPath("other/child")), forPath("other/child"));
        assertEquals(forPath("").resolveSibling(forPath("/bucket2/other/child")), forPath("/bucket2/other/child"));
        assertEquals(forPath("").resolveSibling(forPath("")), forPath(""));

        // relative (non-empty) vs ...
        assertEquals(forPath("path/to/file").resolveSibling(forPath("other/child")), forPath("path/to/other/child"));
        assertEquals(forPath("path/to/file").resolveSibling(forPath("/bucket2/other/child")), forPath("/bucket2/other/child"));
        assertEquals(forPath("path/to/file").resolveSibling(forPath("")), forPath("path/to/"));
    }
    
    @Test
    public void relativize(){
    	Path path = forPath("/bucket/path/to/file");
    	Path other = forPath("/bucket/path/to/file/hello");
    	assertEquals(forPath("hello"), path.relativize(other));
    	
    	// another

    	assertEquals(forPath("file/hello"),  forPath("/bucket/path/to/").relativize(forPath("/bucket/path/to/file/hello")));
    	
    	// empty
    	
    	assertEquals(forPath(""),  forPath("/bucket/path/to/").relativize(forPath("/bucket/path/to/")));
    }
    
    @Test
    public void toUri() {
    	Path path = forPath("/bucket/path/to/file");
    	URI uri = path.toUri();
    	
    	// the scheme is s3
    	assertEquals("s3", uri.getScheme());
    	
    	// could get the correct fileSystem
    	FileSystem fs = FileSystems.getFileSystem(uri);
    	assertTrue(fs instanceof S3FileSystem);
    	// the host is the endpoint specified in fileSystem
    	assertEquals(((S3FileSystem)fs).getEndpoint(), uri.getHost());
    	
    	// bucket name as first path
    	Path pathActual = fs.provider().getPath(uri);
    	
    	assertEquals(path, pathActual);
    }
    
    // tests startsWith
    
    @Test
	public void startsWith(){
		assertTrue(forPath("/bucket/file1").startsWith(forPath("/bucket")));
	}
    
    @Test
	public void startsWithBlank(){
    	assertFalse(forPath("/bucket/file1").startsWith(forPath("")));
	}
    
    @Test
   	public void startsWithBlankRelative(){
       	assertFalse(forPath("file1").startsWith(forPath("")));
   	}
    
    @Test
   	public void startsWithBlankBlank(){
   		assertTrue(forPath("").startsWith(forPath("")));
   	}
    
    @Test
  	public void startsWithOnlyBuckets(){
  		assertTrue(forPath("/bucket").startsWith(forPath("/bucket")));
  	}
	
	@Test
	public void startsWithRelativeVsAbsolute(){
		assertFalse(forPath("/bucket/file1").startsWith(forPath("file1")));
	}
	
	@Test
	public void startsWithRelativeVsAbsoluteInBucket(){
		assertFalse(forPath("/bucket/file1").startsWith(forPath("bucket")));
	}
	
	@Test
	public void startsWithFalse(){
		assertFalse(forPath("/bucket/file1").startsWith(forPath("/bucket/file1/file2")));
		assertTrue(forPath("/bucket/file1/file2").startsWith(forPath("/bucket/file1")));
	}
	
	@Test
	public void startsWithNotNormalize(){
		assertFalse(forPath("/bucket/file1/file2").startsWith(forPath("/bucket/file1/../")));
	}
	
	@Test
	public void startsWithNormalize(){
		// in this implementation not exists .. or . special paths
		assertFalse(forPath("/bucket/file1/file2").startsWith(forPath("/bucket/file1/../").normalize()));
	}
	
	@Test
	public void startsWithRelative(){
		assertTrue(forPath("file/file1").startsWith(forPath("file")));
	}
   
    @Test
    public void startsWithDifferentProvider() {
    	assertFalse(forPath("/bucket/hello").startsWith(Paths.get("/bucket")));
    }
    
    @Test
    public void startsWithString(){
    	assertTrue(forPath("/bucket/hello").startsWith("/bucket/hello"));
    }
    
    @Test
    public void startsWithStringRelative(){
    	assertTrue(forPath("subkey1/hello").startsWith("subkey1/hello"));
    }
    
    @Test
  	public void startsWithStringOnlyBuckets(){
  		assertTrue(forPath("/bucket").startsWith("/bucket"));
  	}
	
	@Test
	public void startsWithStringRelativeVsAbsolute(){
		assertFalse(forPath("/bucket/file1").startsWith("file1"));
	}
    
    @Test
	public void startsWithStringFalse(){
		assertFalse(forPath("/bucket/file1").startsWith("/bucket/file1/file2"));
		assertTrue(forPath("/bucket/file1/file2").startsWith("/bucket/file1"));
	}
    
    @Test
	public void startsWithStringRelativeVsAbsoluteInBucket(){
		assertFalse(forPath("/bucket/file1").startsWith("bucket"));
	}
    
    // ends with
	
 	@Test
 	public void endsWithAbsoluteRelative(){
 		assertTrue(forPath("/bucket/file1").endsWith(forPath("file1")));
 	}
 	
 	@Test
 	public void endsWithAbsoluteAbsolute(){
 		assertTrue(forPath("/bucket/file1").endsWith(forPath("/bucket/file1")));
 	}
 	
 	@Test
 	public void endsWithRelativeRelative(){
 		assertTrue(forPath("file/file1").endsWith(forPath("file1")));
 	}
 	
 	@Test
 	public void endsWithRelativeAbsolute(){
 		assertFalse(forPath("file/file1").endsWith(forPath("/bucket")));
 	}
 	
 	@Test
 	public void endsWithDifferenteFileSystem(){
 		assertFalse(forPath("/bucket/file1").endsWith(Paths.get("/bucket/file1")));
 	}
 	
 	@Test
 	public void endsWithBlankRelativeAbsolute(){
 		assertFalse(forPath("").endsWith(forPath("/bucket")));
 	}
 	
 	@Test
	public void endsWithBlankBlank() {
		assertTrue(forPath("").endsWith(forPath("")));
	}
 	
 	@Test
 	public void endsWithRelativeBlankAbsolute(){
 		assertFalse(forPath("/bucket/file1").endsWith(forPath("")));
 	}
 	
 	@Test
 	public void endsWithRelativeBlankRelative(){
 		assertFalse(forPath("file1").endsWith(forPath("")));
 	}
 	
}
