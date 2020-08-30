package org.carlspring.cloud.storage.s3fs.spike;

import java.io.IOException;
import java.nio.file.*;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class PathSpecTest
{

    FileSystem fs;

    FileSystem fsWindows;


    @BeforeEach
    public void setup()
            throws IOException
    {
        fs = MemoryFileSystemBuilder.newLinux().build("linux");
        fsWindows = MemoryFileSystemBuilder.newWindows().build("windows");
    }

    @AfterEach
    public void close()
            throws IOException
    {
        try
        {
            fs.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        fsWindows.close();
    }

    // first and more

    @Test
    public void firstAndMore()
    {
        assertEquals(fs.getPath("/dir", "dir", "file"), fs.getPath("/dir", "dir/file"));
        assertEquals(fs.getPath("/dir/dir/file"), fs.getPath("/dir", "dir/file"));
    }

    // absolute relative

    @Test
    public void relative()
    {
        assertFalse(get("file").isAbsolute());
    }

    @Test
    public void absolute()
    {
        assertTrue(get("/file/file2").isAbsolute());
    }

    // test starts with

    @Test
    public void startsWith()
    {
        assertTrue(get("/file/file1").startsWith(get("/file")));
    }

    @Test
    public void startsWithBlank()
    {
        assertFalse(get("/file").startsWith(get("")));
    }

    @Test
    public void startsWithBlankRelative()
    {
        assertFalse(get("file1").startsWith(get("")));
    }

    @Test
    public void startsWithBlankBlank()
    {
        assertTrue(get("").startsWith(get("")));
    }

    @Test
    public void startsWithRelativeVsAbsolute()
    {
        assertFalse(get("/file/file1").startsWith(get("file")));
    }

    @Test
    public void startsWithFalse()
    {
        assertFalse(get("/file/file1").startsWith(get("/file/file1/file2")));
        assertTrue(get("/file/file1/file2").startsWith(get("/file/file1")));
    }

    @Test
    public void startsWithNotNormalize()
    {
        assertFalse(get("/file/file1/file2").startsWith(get("/file/file1/../")));
    }

    @Test
    public void startsWithNormalize()
    {
        assertTrue(get("/file/file1/file2").startsWith(get("/file/file1/../").normalize()));
    }

    @Test
    public void startsWithRelative()
    {
        assertTrue(get("file/file1").startsWith(get("file")));
    }

    @Test
    public void startsWithString()
    {
        assertTrue(get("/file/file1").startsWith("/file"));
    }

    // ends with

    @Test
    public void endsWithAbsoluteRelative()
    {
        assertTrue(get("/file/file1").endsWith(get("file1")));
    }

    @Test
    public void endsWithAbsoluteAbsolute()
    {
        assertTrue(get("/file/file1").endsWith(get("/file/file1")));
    }

    @Test
    public void endsWithRelativeRelative()
    {
        assertTrue(get("file/file1").endsWith(get("file1")));
    }

    @Test
    public void endsWithRelativeAbsolute()
    {
        assertFalse(get("file/file1").endsWith(get("/file")));
    }

    @Test
    public void endsWithDifferentFileSystem()
    {
        assertFalse(get("/file/file1").endsWith(Paths.get("/file/file1")));
    }

    @Test
    public void endsWithBlankRelativeAbsolute()
    {
        assertFalse(get("").endsWith(get("/bucket")));
    }

    @Test
    public void endsWithBlankBlank()
    {
        assertTrue(get("").endsWith(get("")));
    }

    @Test
    public void endsWithRelativeBlankAbsolute()
    {
        assertFalse(get("/bucket/file1").endsWith(get("")));
    }

    @Test
    public void endsWithRelativeBlankRelative()
    {
        assertFalse(get("file1").endsWith(get("")));
    }

    @Test
    public void getParentNull()
    {
        assertNull(get("/").getParent());
    }

    @Test
    public void getParentWindowsNull()
    {
        assertNull(fsWindows.getPath("C://").getParent());
    }

    // file name

    @Test
    public void getFileName()
            throws IOException
    {
        try (FileSystem windows = MemoryFileSystemBuilder.newWindows().build("widows"))
        {
            Path fileName = windows.getPath("C:/file").getFileName();
            Path rootName = windows.getPath("C:/").getFileName();

            assertEquals(windows.getPath("file"), fileName);
            assertNull(rootName);
        }
    }

    @Test
    public void getFileNameRootIsNull()
    {
        Path fileNameRoot = fs.getRootDirectories().iterator().next().getFileName();

        assertNull(fileNameRoot);
    }

    // root

    @Test
    public void getRootReturnBucket()
    {
        assertEquals(get("/"), get("/dir/dir/file").getRoot());
    }

    @Test
    public void fileWithSameNameAsDir()
            throws IOException
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> {
            Files.createFile(fs.getPath("/tmp"));
            Files.createDirectory(fs.getPath("/tmp/"));
        });

        assertNotNull(exception);
    }

    @Test
    public void dirWithSameNameAsFile()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> {
            Files.createDirectories(fs.getPath("/tmp/"));
            Files.createFile(fs.getPath("/tmp"));
        });

        assertNotNull(exception);
    }

    @Test
    public void createDirWithoutEndSlash()
            throws IOException
    {
        Path dir = Files.createDirectory(fs.getPath("/tmp"));
        Files.isDirectory(dir);
    }

    @Test
    public void getRootRelativeReturnNull()
    {
        assertNull(get("dir/file").getRoot());
    }

    @Test
    public void getRoot()
    {
        System.out.println("Default:");
        System.out.println("-------");

        for (Path root : FileSystems.getDefault().getRootDirectories())
        {
            System.out.println("- " + root);
        }

        System.out.println("\nLinux:");
        System.out.println("-----");

        for (Path root : fs.getRootDirectories())
        {
            System.out.println("- " + root);
        }

        System.out.println("\nWindows:");
        System.out.println("-------");

        for (Path root : fsWindows.getRootDirectories())
        {
            System.out.println("- " + root);
        }
    }

    // ~ helpers methods

    private Path get(String path)
    {
        return fs.getPath(path);
    }

}
