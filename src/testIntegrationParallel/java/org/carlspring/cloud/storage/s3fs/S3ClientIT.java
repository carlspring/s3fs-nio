package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import static java.util.UUID.randomUUID;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder.getRealEnv;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@S3IntegrationTest
class S3ClientIT extends BaseIntegrationTest
{

    S3Client client;


    @BeforeEach
    public void setup()
    {
        // s3client
        final Map<String, Object> credentials = EnvironmentBuilder.getRealEnv();

        final AwsCredentials credentialsS3 = AwsBasicCredentials.create(credentials.get(ACCESS_KEY).toString(),
                                                                        credentials.get(SECRET_KEY).toString());
        final AwsCredentialsProvider credentialsProviderS3 = StaticCredentialsProvider.create(credentialsS3);

        final String regionStr = credentials.get(REGION).toString();
        final Region region = Region.of(regionStr);
        client = S3Client.builder().region(region).credentialsProvider(credentialsProviderS3).build();
    }

    @Test
    void putObject()
            throws IOException
    {
        Path file = Files.createTempFile("file-se", "file");

        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectRequest request = PutObjectRequest.builder().bucket(getBucket()).key(getTestBasePath() + "/" + randomUUID().toString()).build();
        PutObjectResponse result = client.putObject(request, file);

        assertNotNull(result);
    }

    @Test
    void putObjectWithEndSlash()
            throws IOException
    {
        Path file = Files.createTempFile("file-se", "file");

        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectRequest request = PutObjectRequest.builder()
                                                   .bucket(getBucket())
                                                   .key(getTestBasePathWithUUID() + "/")
                                                   .build();

        PutObjectResponse result = client.putObject(request, file);

        assertNotNull(result);
    }

    @Test
    void putObjectWithStartSlash()
            throws IOException
    {
        Path file = Files.createTempFile("file-se", "file");

        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectRequest request = PutObjectRequest.builder()
                                                   .bucket(getBucket())
                                                   .key("/" + getTestBasePathWithUUID())
                                                   .build();

        PutObjectResponse result = client.putObject(request, file);

        assertNotNull(result);
    }

    @Test
    void putObjectWithBothSlash()
            throws IOException
    {
        Path file = Files.createTempFile("file-se", "file");

        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectRequest request = PutObjectRequest.builder()
                                                   .bucket(getBucket())
                                                   .key("/" + getTestBasePathWithUUID() + "/")
                                                   .build();

        PutObjectResponse result = client.putObject(request, file);

        assertNotNull(result);
    }

    @Test
    void putObjectByteArray()
            throws IOException
    {
        final InputStream inputStream = new ByteArrayInputStream("contents1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        PutObjectRequest request = PutObjectRequest.builder()
                                                   .bucket(getBucket())
                                                   .key(getTestBasePathWithUUID())
                                                   .build();

        PutObjectResponse result = client.putObject(request, requestBody);

        assertNotNull(result);
    }

    private String getBucket()
    {
        return EnvironmentBuilder.getBucket().replace("/", "");
    }

}
