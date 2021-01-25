package org.carlspring.cloud.storage.s3fs.util;

import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;

public abstract class BaseIntegrationTest
{

    static {
        if (isMinioEnv())
        {
            final String accessKey = (String) EnvironmentBuilder.getRealEnv().get(ACCESS_KEY);
            final String secretKey = (String) EnvironmentBuilder.getRealEnv().get(SECRET_KEY);
            final MinioContainer minioContainer = new MinioContainer(accessKey, secretKey);
            minioContainer.start();
        }
    }

    private static boolean isMinioEnv()
    {
        String integrationTestType = System.getProperty("running.it");
        return integrationTestType != null && integrationTestType.equalsIgnoreCase("minio");
    }

}
