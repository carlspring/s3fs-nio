package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.util.Map;
import java.util.Properties;

import org.apache.http.client.utils.URIBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.S3FileSystemProvider.S3_FACTORY_CLASS;
import static org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder.getRealEnv;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_REGION_URI_IT;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class S3UnitTestBase
{

    protected S3FileSystemProvider s3fsProvider;

    protected FileSystem fileSystem;


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

        for (S3FileSystem s3FileSystem : S3FileSystemProvider.getFilesystems().values())
        {
            try
            {
                s3FileSystem.close();
            }
            catch (Exception e)
            {
                //ignore
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
        }
        catch (Throwable ignored)
        {
        }
    }

    public S3FileSystemProvider getS3fsProvider()
    {
        return this.s3fsProvider;
    }

    /**
     * get the URI with the region.
     *
     * @param s3GlobalUri URI a test s3 endpoint.
     * @return URI never null
     */
    public static URI getS3URI(final URI s3GlobalUri)
    {
        final Map<String, Object> env = getRealEnv();

        try
        {
            final String region = (String) env.get(REGION);
            final URI s3Uri = region != null ? URI.create(String.format(S3_REGION_URI_IT, region)) : s3GlobalUri;
            return new URIBuilder(s3Uri).build();
        }
        catch (final URISyntaxException e)
        {
            throw new RuntimeException("Error building uri with the env: " + env);
        }
    }

}
