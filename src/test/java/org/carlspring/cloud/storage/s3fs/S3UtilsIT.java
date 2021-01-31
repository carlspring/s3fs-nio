package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.attribute.S3PosixFileAttributes;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.CopyDirVisitor;
import org.carlspring.cloud.storage.s3fs.util.S3Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.UUID;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@S3IntegrationTest
class S3UtilsIT
        extends BaseIntegrationTest
{

    private static final String BUCKET_NAME = ENVIRONMENT_CONFIGURATION.getBucketName();
    private static final URI URI_GLOBAL = ENVIRONMENT_CONFIGURATION.getGlobalUrl();

    private FileSystem fileSystemAmazon;

    @BeforeEach
    public void setup()
            throws IOException
    {
        fileSystemAmazon = build();
    }

    private static FileSystem build()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);

        try
        {
            FileSystems.getFileSystem(URI_GLOBAL).close();

            return createNewFileSystem();
        }
        catch (FileSystemNotFoundException e)
        {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem()
            throws IOException
    {
        return FileSystems.newFileSystem(URI_GLOBAL, ENVIRONMENT_CONFIGURATION.asMap());
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

        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path base = Files.createDirectory(linux.getPath("/base"));

            Files.createFile(base.resolve("file"));
            Files.createFile(base.resolve("file1"));

            path = fileSystemAmazon.getPath(BUCKET_NAME, startPath);

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

        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path base = Files.createDirectories(linux.getPath("/base").resolve("dir"));
            path = fileSystemAmazon.getPath(BUCKET_NAME, startPath);

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
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;

        final RequestBody requestBody = RequestBody.fromInputStream(new ByteArrayInputStream("".getBytes()), 0L);

        String bucketName = BUCKET_NAME.replace("/", "");
        String key1 = startPath + "lib/angular/";
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key1).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        String key2 = startPath + "lib/angular-dynamic-locale/";
        request = PutObjectRequest.builder().bucket(bucketName).key(key2).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = s3FileSystem.getPath(BUCKET_NAME, startPath, "lib", "angular");
        S3Object result = getS3ObjectSummary(s3Path);

        assertEquals(startPath + "lib/angular/", result.key());
    }

    @Test
    void lookupS3ObjectWhenS3PathIsADirectoryAndIsVirtual()
            throws IOException
    {
        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        final ByteArrayInputStream inputStream = new ByteArrayInputStream("content1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;

        String bucketName = BUCKET_NAME.replace("/", "");
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(BUCKET_NAME, folder);
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
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
    }

    @Test
    void lookupS3BasicFileAttributesWhenS3PathIsADirectoryAndIsVirtual()
            throws IOException
    {
        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;

        ByteArrayInputStream inputStream = new ByteArrayInputStream("content1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = BUCKET_NAME.replace("/", "");
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(BUCKET_NAME, folder);
        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertTrue(result.isDirectory());
        assertFalse(result.isRegularFile());
        assertFalse(result.isSymbolicLink());
        assertFalse(result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
    }

    @Test
    void lookupS3BasicFileAttributesWhenS3PathIsADirectoryAndIsNotVirtualAndNoContent()
            throws IOException
    {
        String folder = "folder" + UUID.randomUUID().toString();
        String key = folder + "/";

        ByteArrayInputStream inputStream = new ByteArrayInputStream("contenido1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = BUCKET_NAME.replace("/", "");
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;

        s3FileSystem.getClient().putObject(request, requestBody);

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(BUCKET_NAME, folder);

        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertTrue(result.isDirectory());
        assertFalse(result.isRegularFile());
        assertFalse(result.isSymbolicLink());
        assertFalse(result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
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
        final String folder = "angular" + UUID.randomUUID().toString();
        final String key = folder + "/content.js";

        final ByteArrayInputStream inputStream = new ByteArrayInputStream("content1".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        final String bucketName = BUCKET_NAME.replace("/", "");
        final PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        final S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        final S3Client client = s3FileSystem.getClient();

        final S3Path s3Path = (S3Path) fileSystemAmazon.getPath(BUCKET_NAME, folder);

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
        final String folder = "folder" + UUID.randomUUID().toString();
        final String key = folder + "/";

        final ByteArrayInputStream inputStream = new ByteArrayInputStream("".getBytes());
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, 0L);
        final String bucketName = BUCKET_NAME.replace("/", "");
        final PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        final S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        final S3Client client = s3FileSystem.getClient();

        final S3Path s3Path = (S3Path) fileSystemAmazon.getPath(BUCKET_NAME, folder);

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
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";

        Path path;

        try (final FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            final Path base = Files.createDirectory(linux.getPath("/base"));
            final Path file = base.resolve("file");
            Files.createFile(file);

            path = fileSystemAmazon.getPath(BUCKET_NAME, startPath);

            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        return path;
    }

}
