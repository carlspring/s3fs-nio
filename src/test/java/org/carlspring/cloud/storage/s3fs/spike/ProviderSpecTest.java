package org.carlspring.cloud.storage.s3fs.spike;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProviderSpecTest
{

    FileSystem fs;


    @BeforeEach
    public void setup()
            throws IOException
    {
        fs = MemoryFileSystemBuilder.newLinux().build("linux");
    }

    @AfterEach
    public void close()
            throws IOException
    {
        fs.close();
    }

    @Test
    public void readNothing()
            throws IOException
    {
        //Path base = Files.createDirectories(fs.getPath("/dir"));
        Path base = Files.createTempDirectory("asdadadasd");
        try (SeekableByteChannel seekable = Files.newByteChannel(Files.createFile(base.resolve("file1.html")),
                                                                 EnumSet.of(StandardOpenOption.DELETE_ON_CLOSE)))
        {
            // do nothing
        }

        assertTrue(Files.notExists(base.resolve("file1.html")));
    }

    @Test
    public void seekableRead()
            throws IOException
    {
        Path path = Files.write(Files.createTempFile("asdas", "asdsadad"),
                                "content uyuhu".getBytes(),
                                StandardOpenOption.APPEND);

        try (SeekableByteChannel channel = Files.newByteChannel(path))
        {
            //channel = Paths.get("Path to file").newByteChannel(StandardOpenOption.READ);
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            while (channel.read(buffer) > 0)
            {
                buffer.rewind();
                buffer.flip();
            }
        }
    }

}
