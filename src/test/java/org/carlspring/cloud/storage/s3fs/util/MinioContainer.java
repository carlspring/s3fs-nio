package org.carlspring.cloud.storage.s3fs.util;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

public class MinioContainer
        extends GenericContainer<MinioContainer>
{

    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_IMAGE = "minio/minio";
    private static final String DEFAULT_TAG = "RELEASE.2021-01-30T00-20-58Z";
    private static final String MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
    private static final String MINIO_SECRET_KEY = "MINIO_SECRET_KEY";
    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    public MinioContainer(String accessKey,
                          String secretKey,
                          String bucketName)
    {
        super(DEFAULT_IMAGE + ":" + DEFAULT_TAG);
        withNetworkAliases("minio-" + Base58.randomString(6));
        addExposedPort(DEFAULT_PORT);
        withEnv(MINIO_ACCESS_KEY, accessKey);
        withEnv(MINIO_SECRET_KEY, secretKey);

        // Auto create bucket on start up because minio does not do this for you.
        // https://github.com/minio/minio/issues/4769#issuecomment-331033735
        withCreateContainerCmdModifier(cmd -> {
            cmd.withEntrypoint("/bin/sh");
            cmd.withCmd("-c", "mkdir -p /data" + bucketName + " && minio server " + DEFAULT_STORAGE_DIRECTORY);
        });

        setWaitStrategy(new HttpWaitStrategy().forPort(DEFAULT_PORT)
                                              .forPath(HEALTH_ENDPOINT)
                                              .withStartupTimeout(Duration.ofMinutes(2)));
    }

}

