package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.S3FileSystemProvider.S3_FACTORY_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public abstract class S3UnitTestBase extends BaseTest
{

    protected S3FileSystemProvider s3fsProvider;

    protected FileSystem fileSystem;
    protected FileSystem mockFileSystem;

    @BeforeEach
    public void setProperties()
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);

        System.setProperty(S3_FACTORY_CLASS, "org.carlspring.cloud.storage.s3fs.util.S3MockFactory");
    }

    @BeforeEach
    public void setupS3fsProvider()
    {
        s3fsProvider = spy(new S3FileSystemProvider());

        // stub the possibility to add system envs var
        doReturn(false).when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(new Properties()).when(s3fsProvider).loadAmazonProperties();
    }

    @AfterEach
    public void closeMemory()
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.close();
        ArrayList<FileSystemProvider> fileSystemProviders = FileSystemProvider.installedProviders()
                                                                              .stream()
                                                                              .filter(p -> p.getClass().isAssignableFrom(S3FileSystemProvider.class))
                                                                              .collect(Collectors.toCollection(ArrayList::new));
        for (FileSystemProvider provider : fileSystemProviders)
        {
            if (S3FileSystemProvider.class.isAssignableFrom(provider.getClass()))
            {
                S3FileSystemProvider s3Provider = (S3FileSystemProvider) provider;
                s3Provider.close();
            }
        }
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        try
        {
            if (s3fsProvider != null)
            {
                s3fsProvider.close((S3FileSystem) fileSystem);
            }

            if (fileSystem != null)
            {
                fileSystem.close();
            }
            if (mockFileSystem != null)
            {
                mockFileSystem.close();
            }
        }
        catch (Throwable ignored)
        {
        }
    }

    public S3FileSystemProvider getS3fsProvider()
    {
        return this.s3fsProvider;
    }

}
