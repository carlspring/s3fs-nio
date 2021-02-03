package org.carlspring.cloud.storage.s3fs.spike;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test to search using ServiceLoader for available FileSystemsProvider-s and call them with newFileSystem.
 *
 * @author jarnaiz
 */
class InstallProviderTest
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
    void useZipProvider()
            throws IOException
    {
        Path path = createZipTempFile();
        String pathFinal = pathToString(path);

        FileSystem fs = FileSystems.newFileSystem(URI.create("jar:file:" + pathFinal),
                                                  new HashMap<>(),
                                                  this.getClass().getClassLoader());

        Path zipPath = fs.getPath("test.zip");

        assertNotNull(zipPath);
        assertNotNull(zipPath.getFileSystem());
        assertNotNull(zipPath.getFileSystem().provider());
    }

    @Test
    void useZipProviderPathNotExists()
    {
        final URI uri = URI.create("jar:file:/not/exists/zip.zip");
        final HashMap<String, Object> envMap = new HashMap<>();
        final ClassLoader classLoader = this.getClass().getClassLoader();

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NoSuchFileException.class,
                                           () -> FileSystems.newFileSystem(uri, envMap, classLoader));

        assertNotNull(exception);
    }

    @Test
    void useAlternativeZipProvider()
            throws IOException
    {
        Path path = createZipTempFile();
        String pathFinal = pathToString(path);

        FileSystem fs = FileSystems.newFileSystem(URI.create("zipfs:file:" + pathFinal),
                                                  new HashMap<>(),
                                                  this.getClass().getClassLoader());

        Path zipPath = fs.getPath("test.zip");

        assertNotNull(zipPath);
        assertNotNull(zipPath.getFileSystem());
        assertNotNull(zipPath.getFileSystem().provider());
        assertTrue(zipPath.getFileSystem()
                          .provider() instanceof com.github.marschall.com.sun.nio.zipfs.ZipFileSystemProvider);
    }

    @Test
    void newS3Provider()
            throws IOException
    {
        final URI uri = URI.create("s3:///hello/there/");

        // Region property is needed to create S3 client.
        final Map<String, Object> envMap = ImmutableMap.<String, Object>builder().put(REGION, Region.US_EAST_1.id())
                                                                                 .build();

        // if META-INF/services/java.nio.file.spi.FileSystemProvider is not present with
        // the content: org.carlspring.cloud.storage.s3fs.S3FileSystemProvider
        // this method return ProviderNotFoundException
        FileSystem fs = FileSystems.newFileSystem(uri, envMap, this.getClass().getClassLoader());

        Path path = fs.getPath("test.zip");

        assertNotNull(path);
        assertNotNull(path.getFileSystem());
        assertNotNull(path.getFileSystem().provider());
        assertTrue(path.getFileSystem().provider() instanceof S3FileSystemProvider);

        // close fs (FileSystems.getFileSystem throw exception)
        fs.close();
    }

    @Test
    void getZipProvider()
    {
        final URI uri = URI.create("jar:file:/file.zip");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> FileSystems.getFileSystem(uri));

        assertNotNull(exception);
    }

    // deviation from spec
    @Test
    void getZipPath()
    {
        final URI uri = URI.create("jar:file:/file.zip!/BAR");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> Paths.get(uri));

        assertNotNull(exception);
    }

    // deviation from spec
    @Test
    void getMemoryPath()
    {
        final URI uri = URI.create("memory:hellou:/file.zip");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemNotFoundException.class, () -> Paths.get(uri));

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
