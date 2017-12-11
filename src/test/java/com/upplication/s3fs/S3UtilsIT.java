package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.UUID;

import com.amazonaws.services.s3.model.*;
import com.upplication.s3fs.attribute.S3BasicFileAttributes;
import com.upplication.s3fs.attribute.S3PosixFileAttributes;
import com.upplication.s3fs.util.CopyDirVisitor;
import com.upplication.s3fs.util.EnvironmentBuilder;
import com.upplication.s3fs.util.S3Utils;
import org.junit.Before;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.Test;


public class S3UtilsIT {

    private static final String bucket = EnvironmentBuilder.getBucket();
    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private FileSystem fileSystemAmazon;

    @Before
    public void setup() throws IOException {
        fileSystemAmazon = build();
    }

    private static FileSystem build() throws IOException {

        System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);
        try {
            FileSystems.getFileSystem(uriGlobal).close();
            return createNewFileSystem();
        } catch (FileSystemNotFoundException e) {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem() throws IOException {
        return FileSystems.newFileSystem(uriGlobal, EnvironmentBuilder.getRealEnv());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_file() throws IOException {

        Path path = getPathFile();

        S3Path s3Path = (S3Path) path.resolve("file");
        S3ObjectSummary result = getS3ObjectSummary(s3Path);

        assertEquals(s3Path.getKey(), result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_file_and_exists_other_starts_with_same_name() throws IOException {
        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {

            Path base = Files.createDirectory(linux.getPath("/base"));
            Files.createFile(base.resolve("file"));
            Files.createFile(base.resolve("file1"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }

        S3Path s3Path = (S3Path) path.resolve("file");
        S3ObjectSummary result = getS3ObjectSummary(s3Path);

        assertEquals(s3Path.getKey(), result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_a_directory() throws IOException {
        Path path;
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {

            Path base = Files.createDirectories(linux.getPath("/base").resolve("dir"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base.getParent(), new CopyDirVisitor(base.getParent(), path));
        }

        S3Path s3Path = (S3Path) path.resolve("dir");

        S3ObjectSummary result = getS3ObjectSummary(s3Path);

        assertEquals(s3Path.getKey() + "/", result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_a_directory_and_exists_other_directory_starts_same_name() throws IOException {

        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0L);
        s3FileSystem.getClient().putObject(bucket.replace("/", ""), startPath + "lib/angular/", new ByteArrayInputStream("".getBytes()), metadata);
        s3FileSystem.getClient().putObject(bucket.replace("/", ""), startPath + "lib/angular-dynamic-locale/", new ByteArrayInputStream("".getBytes()), metadata);

        S3Path s3Path = s3FileSystem.getPath(bucket, startPath, "lib", "angular");
        S3ObjectSummary result = getS3ObjectSummary(s3Path);

        assertEquals(startPath + "lib/angular/", result.getKey());
    }

    @Test
    public void lookup_S3Object_when_S3Path_is_a_directory_and_is_virtual() throws IOException {

        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        s3FileSystem.getClient().putObject(bucket.replace("/", ""), key, new ByteArrayInputStream("contenido1".getBytes()), new ObjectMetadata());

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);
        S3ObjectSummary result = getS3ObjectSummary(s3Path);

        assertEquals(key, result.getKey());
    }

    @Test
    public void lookup_S3BasicFileAttributes_when_S3Path_is_file() throws IOException {

        Path path = getPathFile();

        S3Path s3Path = (S3Path) path.resolve("file");
        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertEquals(false, result.isDirectory());
        assertEquals(true, result.isRegularFile());
        assertEquals(false, result.isSymbolicLink());
        assertEquals(false, result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
    }



    @Test
    public void lookup_S3BasicFileAttributes_when_S3Path_is_a_directory_and_is_virtual() throws IOException {

        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        s3FileSystem.getClient().putObject(bucket.replace("/", ""), key, new ByteArrayInputStream("contenido1".getBytes()), new ObjectMetadata());

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);
        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertEquals(true, result.isDirectory());
        assertEquals(false, result.isRegularFile());
        assertEquals(false, result.isSymbolicLink());
        assertEquals(false, result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
    }

    @Test
    public void lookup_S3BasicFileAttributes_when_S3Path_is_a_directory_and_is_not_virtual_and_no_content() throws IOException {

        String folder = "folder" + UUID.randomUUID().toString();
        String key = folder + "/";

        final ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0L);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.replace("/", ""), key, new ByteArrayInputStream("".getBytes()), om);
        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        s3FileSystem.getClient().putObject(putObjectRequest);

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);

        S3BasicFileAttributes result = new S3Utils().getS3FileAttributes(s3Path);

        assertEquals(true, result.isDirectory());
        assertEquals(false, result.isRegularFile());
        assertEquals(false, result.isSymbolicLink());
        assertEquals(false, result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
    }

    @Test
    public void lookup_S3PosixFileAttributes_when_S3Path_is_file() throws IOException {

        Path path = getPathFile();

        S3Path s3Path = (S3Path) path.resolve("file");
        S3PosixFileAttributes result = new S3Utils().getS3PosixFileAttributes(s3Path);

        assertEquals(false, result.isDirectory());
        assertEquals(true, result.isRegularFile());
        assertEquals(false, result.isSymbolicLink());
        assertEquals(false, result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
        // posix
        assertNotNull(result.owner());
        assertNull(result.group());
        assertNotNull(result.permissions());
    }

    @Test
    public void lookup_S3PosixFileAttributes_when_S3Path_is_a_directory_and_is_virtual() throws IOException {

        String folder = "angular" + UUID.randomUUID().toString();
        String key = folder + "/content.js";

        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        s3FileSystem.getClient().putObject(bucket.replace("/", ""), key, new ByteArrayInputStream("contenido1".getBytes()), new ObjectMetadata());

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);
        S3PosixFileAttributes result = new S3Utils().getS3PosixFileAttributes(s3Path);

        assertEquals(true, result.isDirectory());
        assertEquals(false, result.isRegularFile());
        assertEquals(false, result.isSymbolicLink());
        assertEquals(false, result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
        // posix
        assertNull(result.owner());
        assertNull(result.group());
        assertNull(result.permissions());
    }

    @Test
    public void lookup_S3PosixFileAttributes_when_S3Path_is_a_directory_and_is_not_virtual_and_no_content() throws IOException {

        String folder = "folder" + UUID.randomUUID().toString();
        String key = folder + "/";

        final ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0L);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.replace("/", ""), key, new ByteArrayInputStream("".getBytes()), om);
        S3FileSystem s3FileSystem = (S3FileSystem) fileSystemAmazon;
        s3FileSystem.getClient().putObject(putObjectRequest);

        S3Path s3Path = (S3Path) fileSystemAmazon.getPath(bucket, folder);
        S3PosixFileAttributes result = new S3Utils().getS3PosixFileAttributes(s3Path);

        assertEquals(true, result.isDirectory());
        assertEquals(false, result.isRegularFile());
        assertEquals(false, result.isSymbolicLink());
        assertEquals(false, result.isOther());
        assertNotNull(result.creationTime());
        assertNotNull(result.fileKey());
        assertNotNull(result.getCacheCreated());
        assertNotNull(result.lastAccessTime());
        assertNotNull(result.lastModifiedTime());
        assertNotNull(result.size());
        // posix
        assertNull(result.owner());
        assertNull(result.group());
        assertNull(result.permissions());
    }

    public S3ObjectSummary getS3ObjectSummary(S3Path s3Path) throws NoSuchFileException {
        return new S3Utils().getS3ObjectSummary(s3Path);
    }

    private Path getPathFile() throws IOException {
        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";
        Path path;
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux")) {

            Path base = Files.createDirectory(linux.getPath("/base"));
            Files.createFile(base.resolve("file"));
            path = fileSystemAmazon.getPath(bucket, startPath);
            Files.walkFileTree(base, new CopyDirVisitor(base, path));
        }
        return path;
    }
}
