package org.carlspring.cloud.storage.s3fs.spike;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.*;

/**
 * A test to search using ServiceLoader for available FileSystemsProvider-s and call them with newFileSystem.
 *
 * @author jarnaiz
 */
public class InstallProviderTest
{


    @BeforeEach
    public void cleanup()
    {
        //clean resources
        try
        {
            FileSystems.getFileSystem(S3_GLOBAL_URI_TEST).close();
        }
        catch (FileSystemNotFoundException | IOException e)
        {
            // ignore this
        }
    }

    @Test
    public void useZipProvider()
            throws IOException
    {
        Path path = createZipTempFile();
        String pathFinal = pathToString(path);

        FileSystem fs = FileSystems.newFileSystem(URI.create("jar:file:" + pathFinal),
                                                  new HashMap<String, Object>(),
                                                  this.getClass().getClassLoader());

        Path zipPath = fs.getPath("test.zip");

        assertNotNull(zipPath);
        assertNotNull(zipPath.getFileSystem());
        assertNotNull(zipPath.getFileSystem().provider());
        //assertTrue(zipPath.getFileSystem().provider() instanceof com.sun.nio.zipfs.ZipFileSystemProvider);
    }

    @Test
    public void useZipProviderPathNotExists()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> {
            FileSystems.newFileSystem(URI.create("jar:file:/not/exists/zip.zip"),
                                      new HashMap<String, Object>(),
                                      this.getClass().getClassLoader());
        });

        assertNotNull(exception);
    }

    @Test
    public void useAlternativeZipProvider()
            throws IOException
    {
        Path path = createZipTempFile();
        String pathFinal = pathToString(path);

        FileSystem fs = FileSystems.newFileSystem(URI.create("zipfs:file:" + pathFinal),
                                                  new HashMap<String, Object>(),
                                                  this.getClass().getClassLoader());

        Path zipPath = fs.getPath("test.zip");

        assertNotNull(zipPath);
        assertNotNull(zipPath.getFileSystem());
        assertNotNull(zipPath.getFileSystem().provider());
        assertTrue(zipPath.getFileSystem()
                          .provider() instanceof com.github.marschall.com.sun.nio.zipfs.ZipFileSystemProvider);
    }

    @Test
    public void newS3Provider()
            throws IOException
    {
        URI uri = URI.create("s3:///hello/there/");

        // if meta-inf/services/java.ni.spi.FileSystemProvider is not present with
        // the content: org.carlspring.cloud.storage.s3fs.S3FileSystemProvider
        // this method return ProviderNotFoundException
        FileSystem fs = FileSystems.newFileSystem(uri, new HashMap<String, Object>(), this.getClass().getClassLoader());

        Path path = fs.getPath("test.zip");

        assertNotNull(path);
        assertNotNull(path.getFileSystem());
        assertNotNull(path.getFileSystem().provider());
        assertTrue(path.getFileSystem().provider() instanceof S3FileSystemProvider);

        // close fs (FileSystems.getFileSystem throw exception)
        fs.close();
    }

    @Test
    public void getZipProvider()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> {
            URI uri = URI.create("jar:file:/file.zip");
            FileSystems.getFileSystem(uri);
        });

        assertNotNull(exception);
    }

    // deviation from spec
    @Test
    public void getZipPath()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> {
            Paths.get(URI.create("jar:file:/file.zip!/BAR"));
        });

        assertNotNull(exception);
    }

    // deviation from spec
    @Test
    public void getMemoryPath()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> {
            Paths.get(URI.create("memory:hellou:/file.zip"));
        });

        assertNotNull(exception);
    }

    // ~ helper methods

    private Path createZipTempFile()
            throws IOException
    {
        File zip = Files.createTempFile("temp", ".zip").toFile();

        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zip)))
        {
            ZipEntry entry = new ZipEntry("text.txt");
            out.putNextEntry(entry);
        }

        return zip.toPath();
    }

    private String pathToString(Path pathNext)
    {
        StringBuilder pathFinal = new StringBuilder();

        for (; pathNext.getParent() != null; pathNext = pathNext.getParent())
        {
            pathFinal.insert(0, "/" + pathNext.getFileName().toString());
        }

        String pathName = pathNext.toString();
        if (pathName.endsWith("\\"))
        {
            pathName = pathName.substring(0, pathName.length() - 1);
        }

        pathFinal.insert(0, "/" + pathName);

        return pathFinal.toString();
    }

}
