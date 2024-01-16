package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.HashMap;

import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.*;

@S3IntegrationTest
class FileSystemsIT extends BaseIntegrationTest
{

    private static final URI uriEurope = URI.create("s3://s3-eu-west-1.amazonaws.com/");

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3EndpointConstant.S3_GLOBAL_URI_IT);

    private static FileSystem provisionedFileSystem;

    @BeforeAll
    public static void setup() throws IOException
    {
        provisionedFileSystem = BaseTest.provisionFilesystem(uriGlobal);
    }

    @Test
    void buildEnvAnotherURIReturnDifferent()
            throws IOException
    {
        FileSystem newFileSystem = FileSystems.newFileSystem(uriEurope, EnvironmentBuilder.getRealEnv());
        assertNotSame(provisionedFileSystem, newFileSystem);
    }

    @Test
    void shouldReturnExistingFileSystem()
    {
        FileSystem retrieveFileSystem = FileSystems.getFileSystem(uriGlobal);
        assertSame(provisionedFileSystem, retrieveFileSystem);
    }

    @Test
    void shouldReturnThrowExceptionOnMissingFileSystem()
    {
        assertThrows(FileSystemNotFoundException.class,() -> FileSystems.getFileSystem(uriEurope));
    }

    @Test
    void shouldThrowExceptionOnExistingFileSystem() throws IOException
    {
        assertThrows(FileSystemAlreadyExistsException.class,() -> BaseTest.createNewFileSystem(uriGlobal));
    }

}
