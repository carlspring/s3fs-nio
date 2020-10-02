package org.carlspring.cloud.storage.s3fs.util;

import org.carlspring.cloud.storage.s3fs.S3Factory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import static org.mockito.Mockito.spy;

public class S3MockFactory
        extends S3Factory
{

    private static FileSystem fsMem;

    private static S3ClientMock s3Client;


    @Override
    public S3Client getS3Client(final URI uri, final Properties props)
    {
        return getS3ClientMock();
    }

    @Override
    protected S3Client createS3Client(final S3ClientBuilder builder)
    {
        return getS3ClientMock();
    }

    public static S3ClientMock getS3ClientMock()
    {
        if (s3Client == null)
        {
            final Path path = getFsMem().getPath("/");
            final S3ClientMock s3ClientMock = new S3ClientMock(path);
            s3Client = spy(s3ClientMock);
        }

        return s3Client;
    }

    private static FileSystem getFsMem()
    {
        if (fsMem == null)
        {
            try
            {
                fsMem = MemoryFileSystemBuilder.newLinux()
                                               .setCurrentWorkingDirectory("/")
                                               .build(UUID.randomUUID().toString());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        return fsMem;
    }

}
