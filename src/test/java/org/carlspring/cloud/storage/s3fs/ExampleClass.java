package org.carlspring.cloud.storage.s3fs;

import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.*;
import static org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder.getRealEnv;

// TODO: Write a fully-working example class with a simple integration test for it.
public class ExampleClass
{

    private AmazonS3 client;

    public ExampleClass()
    {
        final Map<String, Object> env = getRealEnv();

        BasicAWSCredentials credentialsS3 = new BasicAWSCredentials(env.get(ACCESS_KEY).toString(),
                                                                    env.get(SECRET_KEY).toString());

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentialsS3);

        client = AmazonS3ClientBuilder.standard()
                                      .withCredentials(credentialsProvider)
                                      .withRegion(env.get(REGION).toString())
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
