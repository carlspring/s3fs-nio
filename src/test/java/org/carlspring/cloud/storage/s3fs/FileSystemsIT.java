package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

@S3IntegrationTest
class FileSystemsIT
        extends BaseIntegrationTest
{

    private static final URI URI_EUROPE = URI.create("s3://s3-eu-west-1.amazonaws.com/");
    private static final URI URI_GLOBAL = ENVIRONMENT_CONFIGURATION.getGlobalUrl();

    private FileSystem fileSystemAmazon;


    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);

        fileSystemAmazon = build();
    }

    private static FileSystem build()
            throws IOException
    {
        try
        {
            FileSystems.getFileSystem(URI_GLOBAL).close();

            return createNewFileSystem();
        }
        catch (FileSystemNotFoundException e)
        {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem()
            throws IOException
    {
        return FileSystems.newFileSystem(URI_GLOBAL, ENVIRONMENT_CONFIGURATION.asMap());
    }

    @Test
    void buildEnv()
    {
        FileSystem fileSystem = FileSystems.getFileSystem(URI_GLOBAL);

        assertSame(fileSystemAmazon, fileSystem);
    }

    @Test
    void buildEnvAnotherURIReturnDifferent()
            throws IOException
    {
        FileSystem fileSystem = FileSystems.newFileSystem(URI_EUROPE, ENVIRONMENT_CONFIGURATION.asMap());

        assertNotSame(fileSystemAmazon, fileSystem);
    }

}
