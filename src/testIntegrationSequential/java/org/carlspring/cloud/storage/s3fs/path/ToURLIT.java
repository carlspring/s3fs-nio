package org.carlspring.cloud.storage.s3fs.path;

import org.carlspring.cloud.storage.s3fs.BaseIntegrationTest;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;

import static org.carlspring.cloud.storage.s3fs.S3Factory.PATH_STYLE_ACCESS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROTOCOL;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@S3IntegrationTest
class ToURLIT extends BaseIntegrationTest
{

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3EndpointConstant.S3_GLOBAL_URI_IT);

    private static final Map<String, Object> realEnv = EnvironmentBuilder.getRealEnv();

    public FileSystem getS3FileSystem(Entry... props)
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);

        Map<String, Object> env = new HashMap<>(realEnv);
        if (props != null)
        {
            for (Entry entry : props)
            {
                env.put(entry.getKey(), entry.getValue());
            }
        }

        try
        {
            return FileSystems.newFileSystem(uriGlobal, env);
        }
        catch (FileSystemAlreadyExistsException e)
        {
            FileSystems.getFileSystem(uriGlobal).close();

            return FileSystems.newFileSystem(uriGlobal, env);
        }
    }

    @Test
    void toURLDefault()
            throws IOException
    {
        final FileSystem fs = getS3FileSystem(new Entry(PROTOCOL, "https"));

        final String bucketName = "bucket-with-hyphens";
        final S3Path s3Path = (S3Path) fs.getPath("/" + bucketName).resolve("index.html");

        /*
         * According to software.amazon.awssdk.services.s3.internal.BucketUtils.isVirtualAddressingCompatibleBucketName,
         * if bucket name contain dots, it can't be prefixed to the host URL. If this method returns true,
         * then the method software.amazon.awssdk.services.s3.internal.S3EndpointUTILS.changeToDnsEndpoint allows
         * to change this host URL.
         */
        final String host = getHost(bucketName);
        final URL expected = new URL("https", host, "/index.html");
        final URL actual = s3Path.toURL();
        assertEquals(expected, actual);
    }

    @Test
    void toURLWithPathStyle()
            throws IOException
    {
        final FileSystem fs = getS3FileSystem(new Entry(PATH_STYLE_ACCESS, "true"));

        final String bucketName = "bucket-with-hyphens";
        final S3Path s3Path = (S3Path) fs.getPath("/" + bucketName).resolve("index.html");

        final String host = getHost(null);
        final URL expected = new URL("https", host, "/bucket-with-hyphens/index.html");
        final URL actual = s3Path.toURL();
        assertEquals(expected, actual);
    }

    @Test
    void toURLNull()
            throws IOException
    {
        FileSystem fs = getS3FileSystem(new Entry(PATH_STYLE_ACCESS, "true"));

        S3Path s3Path = (S3Path) fs.getPath("directory").resolve("index.html");

        assertNull(s3Path.toURL());
    }

    private String getHost(final String bucketName)
    {
        final String region = (String) realEnv.get(REGION);
        final URI uriWithRegion = URI.create(String.format(S3EndpointConstant.S3_REGION_URI_IT, region));
        if(bucketName != null){
            return String.format("%s.%s", bucketName, uriWithRegion.getHost());
        }

        return uriWithRegion.getHost();
    }

    public static class Entry
    {

        private String key;

        private String value;


        public Entry(String key, String value)
        {
            this.key = key;
            this.value = value;
        }

        public String getKey()
        {
            return key;
        }

        public void setKey(String key)
        {
            this.key = key;
        }

        public String getValue()
        {
            return value;
        }

        public void setValue(String value)
        {
            this.value = value;
        }
    }

}
