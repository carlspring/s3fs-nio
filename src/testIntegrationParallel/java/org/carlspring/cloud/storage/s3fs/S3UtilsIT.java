package org.carlspring.cloud.storage.s3fs;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.attribute.S3PosixFileAttributes;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.CopyDirVisitor;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@S3IntegrationTest
@Execution(ExecutionMode.CONCURRENT)
class S3UtilsIT extends BaseIntegrationTest
{

    private static final String bucket = EnvironmentBuilder.getBucket();

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3EndpointConstant.S3_GLOBAL_URI_IT);

    private static FileSystem provisionedFileSystem;

    @BeforeAll
    public static void setup() throws IOException
    {
        provisionedFileSystem = BaseTest.provisionFilesystem(uriGlobal);
    }

    @Test
    void lookupS3ObjectWhenS3PathIsFile()
            throws IOException
    {
        Path path = getPathFile();

        S3Path s3Path = (S3Path) path.resolve("file");
        S3Object result = getS3ObjectSummary(s3Path);

        assertEquals(s3Path.getKey(), result.key());
    }

    @Test
    void lookupS3ObjectWhenS3PathIsFileAndExistsOtherStartsWithSameName()
            throws IOException
    {

        Path path;

        final String startPath = getTestBasePath() + "/" + UUID.randomUUID().toString() + "/";

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build(startPath.replaceAll("/", "_")))
        {
            Path base = Files.createDirectory(linux.getPath("/base"));

            Files.createFile(base.resolve("file"));
            Files.createFile(base.resolve("file1"));

            path = provisionedFileSystem.getPath(bucket, startPath);

            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        S3Path s3Path = (S3Path) path.resolve("file");
        S3Object result = getS3ObjectSummary(s3Path);

        assertEquals(s3Path.getKey(), result.key());
    }

    @Test
    void lookupS3ObjectWhenS3PathIsADirectory()
            throws IOException
    {

        Path path;

        final String startPath = getTestBasePath() + "/" + UUID.randomUUID().toString() + "/";

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path base = Files.createDirectories(linux.getPath("/base").resolve("dir"));
            path = provisionedFileSystem.getPath(bucket, startPath);

            Files.walkFileTree(base.getParent(), new CopyDirVisitor(base.getParent(), path));
        }

        S3Path s3Path = (S3Path) path.resolve("dir");

        S3Object result = getS3ObjectSummary(s3Path);

        assertEquals(s3Path.getKey() + "/", result.key());
    }

    @Test
    void lookupS3ObjectWhenS3PathIsADirectoryAndExistsOtherDirectoryStartsSameName()
            throws IOException
    {

        final String startPath = getTestBasePath() + "/" + UUID.randomUUID() + "/";

        S3FileSystem s3FileSystem = (S3FileSystem) provisionedFileSystem;

        final RequestBody requestBody = RequestBody.fromInputStream(new ByteArrayInputStream("".getBytes()), 0L);

        String bucketName = bucket.replace("/", "");
        String key1 = startPath + "lib/angular/";
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key1).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        String key2 = startPath + "lib/angular-dynamic-locale/";
        request = PutObjectRequest.builder().bucket(bucketName).key(key2).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = s3FileSystem.getPath(bucket, startPath, "lib", "angular");
        S3Object result = getS3ObjectSummary(s3Path);

        assertEquals(startPath + "lib/angular/", result.key());

    }

    @Test
    void lookupS3ObjectWhenS3PathIsADirectoryAndIsVirtual()
            throws IOException
    {

        String folder = getTestBasePath() + "/angular/" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        final ByteArrayInputStream inputStream = new ByteArrayInputStream("content1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());

        S3FileSystem s3FileSystem = (S3FileSystem) provisionedFileSystem;

        String bucketName = bucket.replace("/", "");
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = (S3Path) provisionedFileSystem.getPath(bucket, folder);
        S3Object result = getS3ObjectSummary(s3Path);

        assertEquals(key, result.key());

    }

    @Test
    void lookupS3BasicFileAttributesWhenS3PathIsFile()
            throws IOException
    {
        Path path = getPathFile();

        S3Path s3Path = (S3Path) path.resolve("file");
        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertFalse(result.isDirectory());
        assertTrue(result.isRegularFile());
        assertFalse(result.isSymbolicLink());
        assertFalse(result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
    }

    @Test
    void lookupS3BasicFileAttributesWhenS3PathIsADirectoryAndIsVirtual()
            throws IOException
    {

        String folder = getTestBasePath() + "/angular/" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem) provisionedFileSystem;

        ByteArrayInputStream inputStream = new ByteArrayInputStream("content1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = bucket.replace("/", "");
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = (S3Path) provisionedFileSystem.getPath(bucket, folder);
        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertTrue(result.isDirectory());
        assertFalse(result.isRegularFile());
        assertFalse(result.isSymbolicLink());
        assertFalse(result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
    }

    @Test
    void lookupS3BasicFileAttributesWhenS3PathIsADirectoryAndIsNotVirtualAndNoContent()
            throws IOException
    {
        String folder = getTestBasePath() + "/folder/" + UUID.randomUUID().toString();
        String key = folder + "/";

        ByteArrayInputStream inputStream = new ByteArrayInputStream("contenido1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = bucket.replace("/", "");
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        S3FileSystem s3FileSystem = (S3FileSystem) provisionedFileSystem;

        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = (S3Path) provisionedFileSystem.getPath(bucket, folder);

        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertTrue(result.isDirectory());
        assertFalse(result.isRegularFile());
        assertFalse(result.isSymbolicLink());
        assertFalse(result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
    }

    @Test
    void lookupS3PosixFileAttributesWhenS3PathIsFile()
            throws IOException
    {
        //given
        final Path path = getPathFile();

        final S3Path s3Path = (S3Path) path.resolve("file");

        //when
        final S3PosixFileAttributes result = new S3Utils().getS3PosixFileAttributes(s3Path);

        //then
        assertFalse(result.isDirectory());
        assertTrue(result.isRegularFile());
        assertNotNull(result.fileKey());
        assertNotNull(result.lastModifiedTime());
        assertEquals(0, result.size());

        // posix
        assertNotNull(result.owner());
        assertNull(result.group());
        assertNotNull(result.permissions());
    }

    @Test
    void lookupS3PosixFileAttributesWhenS3PathIsADirectoryAndIsVirtual()
            throws IOException
    {
        //given
        final String folder = getTestBasePath() + "/angular/" + UUID.randomUUID().toString();
        final String key = folder + "/content.js";

        final ByteArrayInputStream inputStream = new ByteArrayInputStream("content1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        final String bucketName = bucket.replace("/", "");
        final PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        final S3FileSystem s3FileSystem = (S3FileSystem) provisionedFileSystem;
        final S3Client client = s3FileSystem.getClient();

        final S3Path s3Path = (S3Path) provisionedFileSystem.getPath(bucket, folder);

        //when
        client.putObject(request, requestBody);
        S3PosixFileAttributes result = new S3Utils().getS3PosixFileAttributes(s3Path);

        //then
        assertTrue(result.isDirectory());
        assertFalse(result.isRegularFile());
        assertNotNull(result.fileKey());
        assertNotNull(result.lastModifiedTime());
        assertEquals(0, result.size());

        // posix
        assertNotNull(result.owner());
        assertNull(result.group());
        assertNotNull(result.permissions());
    }

    @Test
    void lookupS3PosixFileAttributesWhenS3PathIsADirectoryAndIsNotVirtualAndNoContent()
            throws IOException
    {
        //given
        final String folder = getTestBasePath() + "/folder/" + UUID.randomUUID().toString();
        final String key = folder + "/";

        final ByteArrayInputStream inputStream = new ByteArrayInputStream("".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, 0L);
        final String bucketName = bucket.replace("/", "");
        final PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        final S3FileSystem s3FileSystem = (S3FileSystem) provisionedFileSystem;
        final S3Client client = s3FileSystem.getClient();

        final S3Path s3Path = (S3Path) provisionedFileSystem.getPath(bucket, folder);

        //when
        client.putObject(request, requestBody);
        S3PosixFileAttributes result = new S3Utils().getS3PosixFileAttributes(s3Path);

        //then
        assertTrue(result.isDirectory());
        assertFalse(result.isRegularFile());
        assertNotNull(result.fileKey());
        assertNotNull(result.lastModifiedTime());
        assertEquals(0, result.size());

        // posix
        assertNotNull(result.owner());
        assertNull(result.group());
        assertNotNull(result.permissions());
    }

    public S3Object getS3ObjectSummary(final S3Path s3Path)
            throws NoSuchFileException
    {
        return new S3Utils().getS3Object(s3Path);
    }

    private Path getPathFile()
            throws IOException
    {
        final String startPath = getTestBasePath() + "/" + UUID.randomUUID() + "/";

        Path path;

        try (final FileSystem linux = MemoryFileSystemBuilder.newLinux().build(startPath.replaceAll("/", "_")))
        {
            final Path base = Files.createDirectory(linux.getPath("/base"));
            final Path file = base.resolve("file");
            Files.createFile(file);

            path = provisionedFileSystem.getPath(bucket, startPath);

            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        return path;
    }

}
