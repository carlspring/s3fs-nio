package org.carlspring.cloud.storage.s3fs.util;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROTOCOL;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.MINIO_GLOBAL_URI_IT;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_REGION_URI_IT;

/**
 * Test Helper
 */
public abstract class EnvironmentBuilder
{

    public static final String AMAZON_CONFIGURATION_PROPERTIES_FILE = "/amazon-test.properties";
    public static final String BUCKET_NAME_KEY = "s3fs.bucket.name";

    /**
     * Get credentials from environment vars, and if not found from amazon-test.properties
     *
     * @return Map with the credentials
     */
    public static EnvironmentConfiguration getEnvironmentConfiguration()
    {
        EnvironmentConfiguration configurationFromEnvironmentVariables = loadConfigurationFromEnvironmentVariables();

        return Optional.ofNullable(configurationFromEnvironmentVariables)
                       .orElseGet(EnvironmentBuilder::loadConfigurationFromPropertiesFile);
    }

    private static EnvironmentConfiguration loadConfigurationFromEnvironmentVariables()
    {
        final String accessKey = getEnvironmentValue(ACCESS_KEY);
        final String secretKey = getEnvironmentValue(SECRET_KEY);
        final String region = getEnvironmentValue(REGION);
        final String protocol = getEnvironmentValue(PROTOCOL);
        final String bucketName = getEnvironmentValue(BUCKET_NAME_KEY);

        final boolean isAnyOfArgumentsNull = Stream.of(accessKey, secretKey, region, protocol, bucketName)
                                                   .anyMatch(Objects::isNull);
        if (isAnyOfArgumentsNull)
        {
            return null;
        }

        return EnvironmentConfiguration.builder()
                                       .accessKey(accessKey)
                                       .secretKey(secretKey)
                                       .region(region)
                                       .protocol(protocol)
                                       .bucketName(bucketName)
                                       .build();
    }

    private static EnvironmentConfiguration loadConfigurationFromPropertiesFile()
    {
        final Properties properties = new Properties();

        try
        {
            properties.load(getPropertiesResource());
        }
        catch (IOException e)
        {
            throw new RuntimeException(
                    "Unable to load properties file from classpath nor to find environment variables!", e);
        }

        return EnvironmentConfiguration.builder()
                                       .accessKey(properties.getProperty(ACCESS_KEY))
                                       .secretKey(properties.getProperty(SECRET_KEY))
                                       .region(properties.getProperty(REGION))
                                       .protocol(properties.getProperty(PROTOCOL))
                                       .bucketName(properties.getProperty(BUCKET_NAME_KEY))
                                       .build();
    }

    private static InputStream getPropertiesResource()
            throws IOException
    {
        final URL resourceUrl = EnvironmentBuilder.class.getResource(AMAZON_CONFIGURATION_PROPERTIES_FILE);
        return Optional.ofNullable(resourceUrl)
                       .map(url -> EnvironmentBuilder.class.getResourceAsStream(AMAZON_CONFIGURATION_PROPERTIES_FILE))
                       .orElseThrow(() -> new IOException("File [classpath:/amazon-test.properties] not found!"));
    }

    /**
     * Attempt to retrieve OS Environment Variable
     *
     * @return Environment value by environment variable key name
     */
    private static String getEnvironmentValue(@Nonnull String key)
    {
        final String environmentKeyName = getSanitizedS3EnvironmentName(key);
        return System.getenv(environmentKeyName);
    }

    /**
     * @param key Non-prefixed environment name (i.e. bucket.name)
     * @return Prefixed uppercase environment name (i.e. S3FS_BUCKET_NAME || S3FS_MINIO_BUCKET_NAME)
     */
    private static String getSanitizedS3EnvironmentName(@Nonnull String key)
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
     * get the URI with the access key and secret key as authority (plain text)
     *
     * @param s3GlobalUri URI a valid s3 endpoint, look at http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
     * @return URI never null
     */
    public static URI getS3URI(URI s3GlobalUri)
    {
        final EnvironmentConfiguration environment = getEnvironmentConfiguration();

        try
        {
            final String accessKey = environment.getAccessKey();
            final String secretKey = environment.getSecretKey();
            final String region = environment.getRegion();

            URI s3Uri;

            // When we're running -Pit-s3 - proceed as usual.
            if (!BaseIntegrationTest.isMinioEnv())
            {
                s3Uri = region != null ? URI.create(String.format(S3_REGION_URI_IT, region)) : s3GlobalUri;
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
            throw new RuntimeException("Error building uri with the environment: " + environment);
        }
    }

}
