package org.weakref.s3fs;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.weakref.s3fs.S3Path.forPath;

public class TestFileSystemProvider
{
    @Test
    public void testCreatesAuthenticated()
            throws IOException
    {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        Map<String,?> env = ImmutableMap.<String, Object>builder()
                .put(S3FileSystemProvider.ACCESS_KEY, "access key")
                .put(S3FileSystemProvider.SECRET_KEY, "secret key")
                .build();

        FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"), env);

        assertNotNull(fileSystem);
    }

    @Test
    public void testCreatesAnonymous()
            throws IOException
    {
        S3FileSystemProvider provider = new S3FileSystemProvider();

        FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);
    }

    @Test(expectedExceptions = FileSystemAlreadyExistsException.class)
    public void testCreateFailsIfAlreadyCreated()
            throws IOException
    {
        S3FileSystemProvider provider = new S3FileSystemProvider();

        FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);

        provider.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
    }
    
    @Test
    public void testGetFileSystem()
            throws IOException
    {
        S3FileSystemProvider provider = new S3FileSystemProvider();

        FileSystem fileSystem = provider.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
        assertNotNull(fileSystem);

        FileSystem other = provider.getFileSystem(URI.create("s3:///"));
        assertSame(fileSystem, other);
    }

    @Test(expectedExceptions = FileSystemNotFoundException.class)
    public void testGetFailsIfNotYetCreated()
    {
        S3FileSystemProvider provider = new S3FileSystemProvider();
        provider.getFileSystem(URI.create("s3:///"));
    }
    
    @Test
    public void testGetPath()
            throws IOException
    {
        FileSystem fs = FileSystems.newFileSystem(URI.create("s3:///"), ImmutableMap.<String, Object>of());
        Path path = Paths.get(URI.create("s3:///bucket/path/to/file"));

        assertEquals(path, forPath("/bucket/path/to/file"));
        assertSame(path.getFileSystem(), fs);
    }
    
}
