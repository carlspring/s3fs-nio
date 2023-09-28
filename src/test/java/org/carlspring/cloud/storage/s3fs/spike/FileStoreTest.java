package org.carlspring.cloud.storage.s3fs.spike;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileStoreTest
{

    FileSystem fs;

    FileSystem fsWindows;

    FileSystem fsMac;


    @BeforeEach
    public void setup()
            throws IOException
    {
        fs = MemoryFileSystemBuilder.newLinux().build("linux");
        fsWindows = MemoryFileSystemBuilder.newWindows().build("windows");
        fsMac = MemoryFileSystemBuilder.newMacOs().build("mac");
    }

    @AfterEach
    public void close()
            throws IOException
    {
        fs.close();
        fsWindows.close();
        fsMac.close();
    }

    @Test
    void getFileStore()
    {
        System.out.println("Default:");
        System.out.println("-------");

        for (FileStore fileStore : FileSystems.getDefault().getFileStores())
        {
            System.out.println("- " + fileStore.name());
        }

        System.out.println("\nLinux:");
        System.out.println("-----");

        for (FileStore fileStore : fs.getFileStores())
        {
            System.out.println("- " + fileStore.name());
        }

        System.out.println("\nWindows:");
        System.out.println("-------");

        for (FileStore fileStore : fsWindows.getFileStores())
        {
            System.out.println("- " + fileStore.name());
        }

        System.out.println("\nMac:");
        System.out.println("---");

        for (FileStore fileStore : fsMac.getFileStores())
        {
            System.out.println("- " + fileStore.name());
        }
    }

}
