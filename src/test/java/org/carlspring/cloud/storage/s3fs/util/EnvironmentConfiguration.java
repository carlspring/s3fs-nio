package org.carlspring.cloud.storage.s3fs.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.apache.http.client.utils.URIBuilder;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROTOCOL;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder.BUCKET_NAME_KEY;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.MINIO_GLOBAL_URI_IT;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_REGION_URI_IT;

public class EnvironmentConfiguration
{

    private final String accessKey;
    private final String secretKey;
    private final String protocol;
    private final String region;
    private final String bucketName;

    private EnvironmentConfiguration(String accessKey,
                                     String secretKey,
                                     String protocol,
                                     String region,
                                     String bucketName)
    {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.protocol = protocol;
        this.region = region;
        this.bucketName = bucketName;
    }

    public static EnvironmentConfigurationBuilder builder()
    {
        return new EnvironmentConfigurationBuilder();
    }

    public Map<String, String> asMap()
    {
        return ImmutableMap.<String, String>builder().put(ACCESS_KEY, accessKey)
                                                     .put(SECRET_KEY, secretKey)
                                                     .put(REGION, region)
                                                     .put(PROTOCOL, protocol)
                                                     .put(BUCKET_NAME_KEY, bucketName)
                                                     .build();
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public String getProtocol()
    {
        return protocol;
    }

    public String getRegion()
    {
        return region;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public URI getGlobalUrl()
    {
        try
        {
            URI s3Uri;

            // When we're running -Pit-s3 - proceed as usual.
            if (!BaseIntegrationTest.isMinioEnv())
            {
                s3Uri = Optional.ofNullable(region)
                                .map(s3Region -> URI.create(String.format(S3_REGION_URI_IT, region)))
                                .orElse(S3_GLOBAL_URI_IT);
            }
            // When we're running -Pit-minio - overwrite any region configuration to localhost:9000 where minio is running.
            else
            {
                s3Uri = MINIO_GLOBAL_URI_IT;
            }

            return new URIBuilder(s3Uri)
                           .setUserInfo(accessKey, secretKey)
                           .build();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException("Error building uri with the environment: " + this);
        }
    }

    public static class EnvironmentConfigurationBuilder
    {

        private String accessKey;
        private String secretKey;
        private String protocol;
        private String region;
        private String bucketName;

        EnvironmentConfigurationBuilder()
        {
        }

        public EnvironmentConfigurationBuilder accessKey(String accessKey)
        {
            this.accessKey = accessKey;
            return this;
        }

        public EnvironmentConfigurationBuilder secretKey(String secretKey)
        {
            this.secretKey = secretKey;
            return this;
        }

        public EnvironmentConfigurationBuilder protocol(String protocol)
        {
            this.protocol = protocol;
            return this;
        }

        public EnvironmentConfigurationBuilder region(String region)
        {
            this.region = region;
            return this;
        }

        public EnvironmentConfigurationBuilder bucketName(String bucketName)
        {
            this.bucketName = bucketName;
            return this;
        }

        public EnvironmentConfiguration build()
        {
            return new EnvironmentConfiguration(accessKey, secretKey, protocol, region, bucketName);
        }

        public String toString()
        {
            return "StorageConfiguration.StorageConfigurationBuilder(accessKey=" + this.accessKey + ", secretKey=" +
                   this.secretKey + ", protocol=" + this.protocol + ", region=" + this.region + ", bucketName=" +
                   this.bucketName + ")";
        }
    }
}
