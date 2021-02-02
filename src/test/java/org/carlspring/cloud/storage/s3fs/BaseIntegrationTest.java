package org.carlspring.cloud.storage.s3fs;


import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.carlspring.cloud.storage.s3fs.util.MinioContainer;

import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;

/**
 * This abstract class holds common integration test logic.
 */
public abstract class BaseIntegrationTest extends BaseTest
{

    static
    {
        if (isMinioEnv())
        {
            try
            {
                final String accessKey = (String) EnvironmentBuilder.getRealEnv().get(ACCESS_KEY);
                final String secretKey = (String) EnvironmentBuilder.getRealEnv().get(SECRET_KEY);
                final String bucketName = (String) EnvironmentBuilder.getRealEnv().get(EnvironmentBuilder.BUCKET_NAME_KEY);
                final MinioContainer minioContainer = new MinioContainer(accessKey, secretKey, bucketName);
                minioContainer.start();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public static boolean isMinioEnv()
    {
        String integrationTestType = System.getProperty("running.it");
        return integrationTestType != null && integrationTestType.equalsIgnoreCase("minio");
    }

}
