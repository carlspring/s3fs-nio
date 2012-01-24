package org.weakref.s3fs;

import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.weakref.s3fs.S3Path.forPath;

public class TestS3Path
{
    @Test
    public void testCreateNoPath()
    {
        S3Path path = forPath("/bucket");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "");
        assertTrue(path.isDirectory());
    }

    @Test
    public void testCreateWithTrailingSlash()
    {
        S3Path path = forPath("/bucket/");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "");
        assertTrue(path.isDirectory());
    }

    @Test
    public void testCreateWithPath()
    {
        S3Path path = forPath("/bucket/path/to/file");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "path/to/file");
        assertFalse(path.isDirectory());
    }

    @Test
    public void testCreateWithPathAndTrailingSlash()
    {
        S3Path path = forPath("/bucket/path/to/file/");

        assertEquals(path.getBucket(), "bucket");
        assertEquals(path.getKey(), "path/to/file/");
        assertTrue(path.isDirectory());
    }

    @Test
    public void testCreateRelative()
    {
        S3Path path = forPath("path/to/file");
        assertNull(path.getBucket());
        assertEquals(path.getKey(), "path/to/file");
        assertFalse(path.isDirectory());
        assertFalse(path.isAbsolute());
    }

    @Test
    public void testGetParent()
    {
        assertEquals(forPath("/bucket/path/to/file").getParent(), forPath("/bucket/path/to/"));
        assertEquals(forPath("/bucket/path/to/file/").getParent(), forPath("/bucket/path/to/"));
        assertNull(forPath("/bucket/").getParent());
        assertNull(forPath("/bucket").getParent());
    }
    
    @Test
    public void testNameCount()
    {
        assertEquals(forPath("/bucket/path/to/file").getNameCount(), 3);
        assertEquals(forPath("/bucket/").getNameCount(), 0);
    }
    
    @Test
    public void testResolve()
    {
        assertEquals(forPath("/bucket/path/to/dir/").resolve(forPath("child/xyz")), forPath("/bucket/path/to/dir/child/xyz"));
        assertEquals(forPath("/bucket/path/to/dir").resolve(forPath("child/xyz")), forPath("/bucket/path/to/dir/child/xyz"));
        assertEquals(forPath("/bucket/path/to/file").resolve(forPath("")), forPath("/bucket/path/to/file"));  // TODO: should this be "path/to/dir/"
        assertEquals(forPath("path/to/file").resolve(forPath("child/xyz")), forPath("path/to/file/child/xyz"));
        assertEquals(forPath("path/to/file").resolve(forPath("")), forPath("path/to/file")); // TODO: should this be "path/to/dir/"
        assertEquals(forPath("/bucket/path/to/file").resolve(forPath("/bucket2/other/child")), forPath("/bucket2/other/child"));
    }
    
    @Test
    public void testName()
    {
        assertEquals(forPath("/bucket/path/to/file").getName(0), forPath("path/"));
        assertEquals(forPath("/bucket/path/to/file").getName(1), forPath("to/"));
        assertEquals(forPath("/bucket/path/to/file").getName(2), forPath("file"));
    }

    @Test
    public void testSubPath()
    {
        assertEquals(forPath("/bucket/path/to/file").subpath(0, 1), forPath("path/"));
        assertEquals(forPath("/bucket/path/to/file").subpath(0, 2), forPath("path/to/"));
        assertEquals(forPath("/bucket/path/to/file").subpath(0, 3), forPath("path/to/file"));
        assertEquals(forPath("/bucket/path/to/file").subpath(1, 2), forPath("to/"));
        assertEquals(forPath("/bucket/path/to/file").subpath(1, 3), forPath("to/file"));
        assertEquals(forPath("/bucket/path/to/file").subpath(2, 3), forPath("file"));
    }
    
    @Test
    public void testIterator()
    {
        Iterator<Path> iterator = forPath("/bucket/path/to/file").iterator();
        
        assertEquals(iterator.next(), forPath("path/"));
        assertEquals(iterator.next(), forPath("to/"));
        assertEquals(iterator.next(), forPath("file"));
    }
    
    @Test
    public void testResolveSibling()
    {
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
}
