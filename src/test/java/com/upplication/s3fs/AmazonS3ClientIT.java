package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.util.EnvironmentBuilder.getRealEnv;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.junit.Before;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.upplication.s3fs.util.EnvironmentBuilder;
import org.junit.Test;

public class AmazonS3ClientIT {

    AmazonS3 client;

    @Before
    public void setup() {
        // s3client
        final Map<String, Object> credentials = getRealEnv();
        BasicAWSCredentials credentialsS3 = new BasicAWSCredentials(credentials.get(ACCESS_KEY).toString(), credentials.get(SECRET_KEY).toString());
        client = new com.amazonaws.services.s3.AmazonS3Client(credentialsS3);
    }

    @Test
    public void putObject() throws IOException {
        Path file = Files.createTempFile("file-se", "file");
        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectResult result = client.putObject(getBucket(), randomUUID().toString(), file.toFile());

        assertNotNull(result);
    }

    @Test
    public void putObjectWithEndSlash() throws IOException {
        Path file = Files.createTempFile("file-se", "file");
        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectResult result = client.putObject(getBucket(), randomUUID().toString() + "/", file.toFile());

        assertNotNull(result);
    }

    @Test
    public void putObjectWithStartSlash() throws IOException {
        Path file = Files.createTempFile("file-se", "file");
        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        client.putObject(getBucket(), "/" + randomUUID().toString(), file.toFile());
    }

    @Test
    public void putObjectWithBothSlash() throws IOException {
        Path file = Files.createTempFile("file-se", "file");
        Files.write(file, "content".getBytes(), StandardOpenOption.APPEND);

        PutObjectResult result = client.putObject(getBucket(), "/" + randomUUID().toString() + "/", file.toFile());

        assertNotNull(result);
    }

    @Test
    public void putObjectByteArray() {

        PutObjectResult result = client.putObject(getBucket(), randomUUID().toString(), new ByteArrayInputStream("contenido1".getBytes()), new ObjectMetadata());

        assertNotNull(result);
    }

    private String getBucket() {
        return EnvironmentBuilder.getBucket().replace("/", "");
    }
}