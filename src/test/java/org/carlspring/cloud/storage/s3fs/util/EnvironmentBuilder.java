package org.carlspring.cloud.storage.s3fs.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.http.client.utils.URIBuilder;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.*;

/**
 * Test Helper
 */
public abstract class EnvironmentBuilder
{

    public static final String BUCKET_NAME_KEY = "bucket_name";


    /**
     * Get credentials from environment vars, and if not found from amazon-test.properties
     *
     * @return Map with the credentials
     */
    public static Map<String, Object> getRealEnv()
    {
        Map<String, Object> env;

        String accessKey = System.getenv(ACCESS_KEY);
        String secretKey = System.getenv(SECRET_KEY);
        String region = System.getenv(REGION);

        if (accessKey != null && secretKey != null && region != null)
        {
            env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, accessKey)
                                                        .put(SECRET_KEY, secretKey)
                                                        .put(REGION, region)
                                                        .build();
        }
        else
        {
            final Properties props = new Properties();

            try
            {
                props.load(EnvironmentBuilder.class.getResourceAsStream("/amazon-test.properties"));
            }
            catch (IOException e)
            {
                throw new RuntimeException("not found amazon-test.properties in the classpath", e);
            }

            env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, props.getProperty(ACCESS_KEY))
                                           .put(SECRET_KEY, props.getProperty(SECRET_KEY))
                                           .put(REGION, props.getProperty(REGION))
                                           .build();
        }

        return env;
    }

    /**
     * get default bucket name
     *
     * @return String without end separator
     */
    public static String getBucket()
    {
        String bucketName = System.getenv(BUCKET_NAME_KEY);
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
            props.load(EnvironmentBuilder.class.getResourceAsStream("/amazon-test.properties"));

            bucketName = props.getProperty(BUCKET_NAME_KEY);
            if (bucketName != null && !bucketName.endsWith("/"))
            {
                bucketName += "/";
            }

            return bucketName;
        }
        catch (IOException e)
        {
            throw new RuntimeException("needed /amazon-test.properties in the classpath");
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
            return new URIBuilder(s3GlobalUri).setUserInfo((String) env.get(ACCESS_KEY), (String) env.get(SECRET_KEY))
                                              .build();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException("Error building uri with the env: " + env);
        }
    }

}
