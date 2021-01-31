package org.carlspring.cloud.storage.s3fs.util;

public abstract class BaseIntegrationTest
{

    protected static final EnvironmentConfiguration ENVIRONMENT_CONFIGURATION = EnvironmentBuilder.getEnvironmentConfiguration();

    static
    {
        if (isMinioEnv())
        {
            try
            {
                final String accessKey = ENVIRONMENT_CONFIGURATION.getAccessKey();
                final String secretKey = ENVIRONMENT_CONFIGURATION.getSecretKey();
                final String bucketName = ENVIRONMENT_CONFIGURATION.getBucketName();
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
