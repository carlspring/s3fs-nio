package org.carlspring.cloud.storage.s3fs.util;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROTOCOL;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_REGION_URI_IT;

/**
 * Test Helper
 */
public abstract class EnvironmentBuilder
{

    public static final String BUCKET_NAME_KEY = "s3fs.bucket.name";


    /**
     * Get credentials from environment vars, and if not found from amazon-test.properties
     *
     * @return Map with the credentials
     */
    public static Map<String, Object> getRealEnv()
    {
        Map<String, Object> env;

        String accessKey = getEnvValue(ACCESS_KEY);
        String secretKey = getEnvValue(SECRET_KEY);
        String region = getEnvValue(REGION);
        String protocol = getEnvValue(PROTOCOL);
        String bucket = getEnvValue(BUCKET_NAME_KEY);

        if (accessKey != null && secretKey != null && region != null && protocol != null && bucket != null)
        {
            env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, accessKey)
                                           .put(SECRET_KEY, secretKey)
                                           .put(REGION, region)
                                           .put(PROTOCOL, protocol)
                                           .put(BUCKET_NAME_KEY, bucket)
                                           .build();
        }
        else
        {
            final Properties props = new Properties();

            try
            {
                props.load(getPropertiesResource());
            }
            catch (IOException e)
            {
                throw new RuntimeException("Unable to load properties file from classpath nor to find environment variables!", e);
            }

            env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, getPropFromSystemOrFallback(ACCESS_KEY, props))
                                           .put(SECRET_KEY, getPropFromSystemOrFallback(SECRET_KEY, props))
                                           .put(REGION, getPropFromSystemOrFallback(REGION, props))
                                           .put(PROTOCOL, getPropFromSystemOrFallback(PROTOCOL, props))
                                           .put(BUCKET_NAME_KEY, getPropFromSystemOrFallback(BUCKET_NAME_KEY, props))
                                           .build();
        }

        return env;
    }

    private static InputStream getPropertiesResource()
            throws IOException
    {
        URL resourceUrl = EnvironmentBuilder.class.getResource("/amazon-test.properties");
        if(resourceUrl != null)
        {
            return EnvironmentBuilder.class.getResourceAsStream("/amazon-test.properties");
        }
        else
        {
            throw new IOException("File [classpath:/amazon-test.properties] not found!");
        }
    }

    private static String getPropFromSystemOrFallback(String key, Properties fallback) {
        return System.getProperty(key, null) != null ? System.getProperty(key) : fallback.getProperty(key, null);
    }

    /**
     * Attempt to retrieve OS Environment Variable
     * @return
     */
    private static String getEnvValue(@Nonnull String key)
    {
        String envKeyName = getS3EnvName(key);
        String value = System.getenv(envKeyName);

        return value;
    }

    /**
     * @param key Non-prefixed environment name (i.e. bucket.name)
     * @return Prefixed uppercase environment name (i.e. S3FS_BUCKET_NAME || S3FS_MINIO_BUCKET_NAME)
     */
    private static String getS3EnvName(@Nonnull String key)
    {
        // Sometimes the key will contain `S3FS_` or `s3fs.property.name` which is why we need to sanitize the string
        // to ensure consistency.
        String sanitized = StringUtils.removeStartIgnoreCase(key, "S3FS.");
        sanitized = StringUtils.removeStartIgnoreCase(sanitized, "S3FS_");
        sanitized = sanitized.replaceAll("\\.", "_").toUpperCase();

        sanitized = "S3FS_" + sanitized;

        return sanitized;
    }

    /**
     * get default bucket name
     *
     * @return String without end separator
     */
    public static String getBucket()
    {
        String bucketName = getEnvValue(BUCKET_NAME_KEY);
        if (bucketName != null)
        {
            if (!bucketName.endsWith("/"))
            {
                bucketName += "/";
            }

            return bucketName;
        }

        final Properties props = new Properties();

        try
        {
            props.load(getPropertiesResource());

            bucketName = props.getProperty(BUCKET_NAME_KEY);
            if (bucketName != null && !bucketName.endsWith("/"))
            {
                bucketName += "/";
            }

            return bucketName;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load properties file from classpath nor to find environment variables!", e);
        }
    }

    /**
     * get the URI with the access key and secret key as authority (plain text)
     *
     * @param s3GlobalUri URI a valid s3 endpoint, look at http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
     * @return URI never null
     */
    public static URI getS3URI(URI s3GlobalUri)
    {
        Map<String, Object> env = getRealEnv();

        try
        {
            final String accessKey = (String) env.get(ACCESS_KEY);
            final String secretKey = (String) env.get(SECRET_KEY);
            final String region = (String) env.get(REGION);

            URI s3Uri = region != null ? URI.create(String.format(S3_REGION_URI_IT, region)) : s3GlobalUri;

            return new URIBuilder(s3Uri).setUserInfo(accessKey, secretKey)
                                        .build();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException("Error building uri with the env: " + env);
        }
    }

}
