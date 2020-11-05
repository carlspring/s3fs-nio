package org.carlspring.cloud.storage.s3fs;

import java.util.Map;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder.getRealEnv;

// TODO: Write a fully-working example class with a simple integration test for it.
public class ExampleClass
{

    private S3Client client;

    public ExampleClass()
    {
        final Map<String, Object> env = getRealEnv();

        final AwsCredentials credentialsS3 = AwsBasicCredentials.create(env.get(ACCESS_KEY).toString(),
                                                                        env.get(SECRET_KEY).toString());

        final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentialsS3);

        client = S3Client.builder()
                         .credentialsProvider(credentialsProvider)
                         .region(Region.of(env.get(REGION).toString()))
                         .build();
    }

    public void upload()
    {

    }

    public void download()
    {

    }

    public void copy()
    {

    }

    public void move()
    {

    }

    public void delete()
    {

    }

}
