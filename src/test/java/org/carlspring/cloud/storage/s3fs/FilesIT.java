package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.CopyDirVisitor;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.*;

@S3IntegrationTest
class FilesIT extends BaseIntegrationTest
{

    private static final String bucket = EnvironmentBuilder.getBucket();

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private FileSystem fileSystemAmazon;


    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);

        fileSystemAmazon = build();
    }

    private static FileSystem build()
            throws IOException
    {
        try
        {
            FileSystems.getFileSystem(uriGlobal).close();

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
        return FileSystems.newFileSystem(uriGlobal, EnvironmentBuilder.getRealEnv());
    }

    @Test
    void notExistsDir()
    {
        Path dir = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + "/");

        assertFalse(Files.exists(dir));
    }

    @Test
    void notExistsFile()
    {
        Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

        assertFalse(Files.exists(file));
    }

    @Test
    void existsFile()
            throws IOException
    {
        Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

        EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        Files.newByteChannel(file, options).close();

        assertTrue(Files.exists(file));
    }

    @Test
    void existsFileWithSpace()
            throws IOException
    {
        Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString(), "space folder");

        Files.createDirectories(file);

        for (Path p : Files.newDirectoryStream(file.getParent()))
        {
            assertTrue(Files.exists(p));
        }
    }

    @Test
    void createEmptyDirTest()
            throws IOException
    {
        Path dir = createEmptyDir();

        assertTrue(Files.exists(dir));
        assertTrue(Files.isDirectory(dir));
    }

    @Test
    void createEmptyFileTest()
            throws IOException
    {
        Path file = createEmptyFile();

        assertTrue(Files.exists(file));
        assertTrue(Files.isRegularFile(file));
    }

    @Test
    void createTempFile()
            throws IOException
    {
        Path dir = createEmptyDir();

        Path file = Files.createTempFile(dir, "file", "temp");

        assertTrue(Files.exists(file));
    }

    @Test
    void createTempFileAndWrite()
            throws IOException
    {
        Path dir = createEmptyDir();
        Path testFile = Files.createTempFile(dir, "file-", ".tmp");

        assertTrue(Files.exists(testFile));

        final String content = "sample content";

        Files.write(testFile, content.getBytes());

        assertArrayEquals(content.getBytes(), Files.readAllBytes(testFile));
    }

    @Test
    void createTempDir()
            throws IOException
    {
        Path dir = createEmptyDir();

        Path dir2 = Files.createTempDirectory(dir, "dir-temp");

        assertTrue(Files.exists(dir2));
    }

    @Test
    void deleteFile()
            throws IOException
    {
        Path file = createEmptyFile();

        Files.delete(file);

        assertTrue(Files.notExists(file));
    }

    @Test
    void deleteDir()
            throws IOException
    {
        Path dir = createEmptyDir();

        Files.delete(dir);

        assertTrue(Files.notExists(dir));
    }

    @Test
    void copyDir()
            throws IOException
    {
        Path dir = uploadDir();

        assertTrue(Files.exists(dir.resolve("assets1/")));
        assertTrue(Files.exists(dir.resolve("assets1/").resolve("index.html")));
        assertTrue(Files.exists(dir.resolve("assets1/").resolve("img").resolve("Penguins.jpg")));
        assertTrue(Files.exists(dir.resolve("assets1/").resolve("js").resolve("main.js")));
    }

    @Test
    void directoryStreamBaseBucketFindDirectoryTest()
            throws IOException
    {
        Path bucketPath = fileSystemAmazon.getPath(bucket);
        String name = "01" + UUID.randomUUID().toString();

        final Path fileToFind = Files.createDirectory(bucketPath.resolve(name));

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(bucketPath))
        {
            findFileInDirectoryStream(bucketPath, fileToFind, dirStream);
        }
    }

    @Test
    void directoryStreamBaseBucketFindFileTest()
            throws IOException
    {
        Path bucketPath = fileSystemAmazon.getPath(bucket);
        String name = "00" + UUID.randomUUID().toString();

        final Path fileToFind = Files.createFile(bucketPath.resolve(name));

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(bucketPath))
        {
            findFileInDirectoryStream(bucketPath, fileToFind, dirStream);
        }
    }

    @Test
    void directoryStreamFirstDirTest()
            throws IOException
    {
        Path dir = uploadDir();

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir))
        {
            int number = 0;

            for (Path path : dirStream)
            {
                number++;

                // only first level
                assertEquals(dir, path.getParent());
                assertEquals("assets1", path.getFileName().toString());
            }

            assertEquals(1, number);
        }
    }

    @Test
    void virtualDirectoryStreamTest()
            throws IOException
    {
        String folder = UUID.randomUUID().toString() + "/";

        String file1 = folder + "file.html";
        String file2 = folder + "file2.html";

        Path dir = fileSystemAmazon.getPath(bucket, folder);

        S3Path s3Path = (S3Path) dir;
        final S3Client client = s3Path.getFileSystem().getClient();

        // upload file without paths
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = s3Path.getFileStore().name();
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(file1).build();
        client.putObject(request, requestBody);


        // another file without paths
        request = PutObjectRequest.builder().bucket(bucketName).key(file2).build();
        client.putObject(request, requestBody);

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir))
        {
            int number = 0;

            boolean file1Find = false;
            boolean file2Find = false;

            for (Path path : dirStream)
            {
                number++;

                // check files only from first level
                assertEquals(dir, path.getParent());

                switch (path.getFileName().toString())
                {
                    case "file.html":
                        file1Find = true;
                        break;
                    case "file2.html":
                        file2Find = true;
                        break;
                    default:
                        break;
                }

            }

            assertTrue(file1Find);
            assertTrue(file2Find);
            assertEquals(2, number);
        }
    }

    @Test
    void virtualDirectoryStreamWithVirtualSubFolderTest()
            throws IOException
    {
        String folder = UUID.randomUUID().toString() + "/";

        String subFolder = folder + "subfolder/file.html";
        String file2 = folder + "file2.html";

        Path dir = fileSystemAmazon.getPath(bucket, folder);

        S3Path s3Path = (S3Path) dir;
        final S3Client client = s3Path.getFileSystem().getClient();

        // upload without paths
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = s3Path.getFileStore().name();
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(subFolder).build();
        client.putObject(request, requestBody);

        // upload another file without paths
        request = PutObjectRequest.builder().bucket(bucketName).key(file2).build();
        client.putObject(request, requestBody);

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir))
        {
            int number = 0;

            boolean subfolderFind = false;
            boolean file2Find = false;

            for (Path path : dirStream)
            {
                number++;

                // only the first level one
                assertEquals(dir, path.getParent());

                switch (path.getFileName().toString())
                {
                    case "subfolder":
                        subfolderFind = true;
                        break;
                    case "file2.html":
                        file2Find = true;
                        break;
                    default:
                        break;
                }

            }

            assertTrue(subfolderFind);
            assertTrue(file2Find);
            assertEquals(2, number);
        }
    }

    @Test
    void deleteFullDirTest()
            throws IOException
    {
        Path dir = uploadDir();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException exc)
                    throws IOException
            {
                if (exc == null)
                {
                    Files.delete(directory);

                    return FileVisitResult.CONTINUE;
                }

                throw exc;
            }
        });

        assertFalse(Files.exists(dir));
    }

    @Test
    void copyUpload()
            throws IOException
    {
        final String content = "sample content";

        Path result = uploadSingleFile(content);

        assertTrue(Files.exists(result));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(result));
    }

    @Test
    void copyDownload()
            throws IOException
    {
        Path result = uploadSingleFile(null);
        Path localResult = Files.createTempDirectory("temp-local-file");
        Path notExistLocalResult = localResult.resolve("result");

        Files.copy(result, notExistLocalResult);

        assertTrue(Files.exists(notExistLocalResult));
        assertArrayEquals(Files.readAllBytes(result), Files.readAllBytes(notExistLocalResult));
    }

    @Test
    void moveFromDifferentProviders()
            throws IOException
    {
        final String content = "sample content";

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path sourceLocal = Files.write(linux.getPath("/index.html"), content.getBytes());
            Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

            Files.move(sourceLocal, result);

            assertTrue(Files.exists(result));
            assertArrayEquals(content.getBytes(), Files.readAllBytes(result));

            Files.notExists(sourceLocal);
        }
    }

    @Test
    void move()
            throws IOException
    {
        final String content = "sample content";

        Path source = uploadSingleFile(content);
        Path dest = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

        Files.move(source, dest);

        assertTrue(Files.exists(dest));
        assertArrayEquals(content.getBytes(), Files.readAllBytes(dest));

        Files.notExists(source);
    }

    @Test
    void createFileWithFolderAndNotExistsFolders()
    {
        String fileWithFolders = UUID.randomUUID().toString() + "/folder2/file.html";

        Path path = fileSystemAmazon.getPath(bucket, fileWithFolders.split("/"));

        S3Path s3Path = (S3Path) path;
        final S3Client client = s3Path.getFileSystem().getClient();

        // upload without paths
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        String bucketName = s3Path.getFileStore().name();
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(fileWithFolders).build();
        client.putObject(request, requestBody);

        assertTrue(Files.exists(path));
        assertTrue(Files.exists(path.getParent()));
    }

    @Test
    void amazonCopyDetectContentType()
            throws IOException
    {
        try (final FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            final Path htmlFile = Files.write(linux.getPath("/index.html"),
                                              "<html><body>html file</body></html>".getBytes());

            final String fileName = UUID.randomUUID().toString() + htmlFile.getFileName().toString();
            final Path result = fileSystemAmazon.getPath(bucket, fileName);

            Files.copy(htmlFile, result);

            final S3Path resultS3 = (S3Path) result;
            final String bucketName = resultS3.getFileStore().name();
            final String key = resultS3.getKey();
            final HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
            final HeadObjectResponse response = resultS3.getFileSystem()
                                                        .getClient()
                                                        .headObject(request);

            assertEquals("text/html", response.contentType());
        }
    }

    @Test
    void amazonCopyNotDetectContentTypeSetDefault()
            throws IOException
    {
        final byte[] data = new byte[]{ (byte) 0xe0,
                                        0x4f,
                                        (byte) 0xd0,
                                        0x20,
                                        (byte) 0xea,
                                        0x3a,
                                        0x69,
                                        0x10,
                                        (byte) 0xa2,
                                        (byte) 0xd8,
                                        0x08,
                                        0x00,
                                        0x2b,
                                        0x30,
                                        0x30,
                                        (byte) 0x9d };

        try (final FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            final Path htmlFile = Files.write(linux.getPath("/index.adsadas"), data);

            final String fileName = UUID.randomUUID().toString() + htmlFile.getFileName().toString();
            final Path result = fileSystemAmazon.getPath(bucket, fileName);

            Files.copy(htmlFile, result);

            final S3Path resultS3 = (S3Path) result;
            final String bucketName = resultS3.getFileStore().name();
            final String key = resultS3.getKey();
            final HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
            final HeadObjectResponse response = resultS3.getFileSystem()
                                                        .getClient()
                                                        .headObject(request);

            assertEquals("application/octet-stream", response.contentType());
        }
    }

    @Test
    void amazonOutpuStreamDetectContentType()
            throws IOException
    {
        try (final FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            final Path htmlFile = Files.write(linux.getPath("/index.html"), "<html><body>html file</body></html>".getBytes());

            final String fileName = UUID.randomUUID().toString() + htmlFile.getFileName().toString();
            final Path result = fileSystemAmazon.getPath(bucket, fileName);

            try (final OutputStream out = Files.newOutputStream(result))
            {
                // copied from Files.write
                final byte[] bytes = Files.readAllBytes(htmlFile);

                final int len = bytes.length;
                int rem = len;

                while (rem > 0)
                {
                    final int n = Math.min(rem, 8192);

                    out.write(bytes, (len - rem), n);

                    rem -= n;
                }
            }

            final S3Path resultS3 = (S3Path) result;
            final String bucketName = resultS3.getFileStore().name();
            final String key = resultS3.getKey();
            final HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
            final HeadObjectResponse response = resultS3.getFileSystem()
                                                        .getClient()
                                                        .headObject(request);

            assertEquals("text/html", response.contentType());
        }
    }

    @Test
    void readAttributesFile()
            throws IOException
    {
        final String content = "sample content";

        Path file = uploadSingleFile(content);

        BasicFileAttributes fileAttributes = Files.readAttributes(file, BasicFileAttributes.class);

        assertNotNull(fileAttributes);
        assertTrue(fileAttributes.isRegularFile());
        assertFalse(fileAttributes.isDirectory());
        assertFalse(fileAttributes.isSymbolicLink());
        assertFalse(fileAttributes.isOther());
        assertEquals(content.length(), fileAttributes.size());
    }

    @Test
    void readAttributesString()
            throws IOException
    {
        final String content = "sample content";

        Path file = uploadSingleFile(content);

        BasicFileAttributes fileAttributes = Files.readAttributes(file, BasicFileAttributes.class);

        Map<String, Object> fileAttributesMap = Files.readAttributes(file, "*");

        assertNotNull(fileAttributes);
        assertNotNull(fileAttributesMap);
        assertEquals(fileAttributes.isRegularFile(), fileAttributesMap.get("isRegularFile"));
        assertEquals(fileAttributes.isDirectory(), fileAttributesMap.get("isDirectory"));
        assertEquals(fileAttributes.creationTime(), fileAttributesMap.get("creationTime"));
        assertEquals(fileAttributes.lastModifiedTime(), fileAttributesMap.get("lastModifiedTime"));
        assertEquals(9, fileAttributesMap.size());
    }

    @Test
    void readAttributesDirectory()
            throws IOException
    {
        Path dir;

        final String startPath = "0000example" + UUID.randomUUID().toString() + "/";

        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path dirDynamicLocale = Files.createDirectories(linux.getPath("/lib").resolve("angular-dynamic-locale"));
            Path assets = Files.createDirectories(linux.getPath("/lib").resolve("angular"));

            Files.createFile(assets.resolve("angular-locale_es-es.min.js"));
            Files.createFile(assets.resolve("angular.min.js"));
            Files.createDirectory(assets.resolve("locales"));
            Files.createFile(dirDynamicLocale.resolve("tmhDinamicLocale.min.js"));

            dir = fileSystemAmazon.getPath(bucket, startPath);

            Files.exists(assets);
            Files.walkFileTree(assets.getParent(), new CopyDirVisitor(assets.getParent().getParent(), dir));
        }

        BasicFileAttributes fileAttributes = Files.readAttributes(dir.resolve("lib").resolve("angular"),
                                                                  BasicFileAttributes.class);
        assertNotNull(fileAttributes);
        assertTrue(fileAttributes.isDirectory());
        assertEquals(startPath + "lib/angular/", fileAttributes.fileKey());
    }

    @Test
    void seekableCloseTwice()
            throws IOException
    {
        Path file = createEmptyFile();

        SeekableByteChannel seekableByteChannel = Files.newByteChannel(file);
        seekableByteChannel.close();
        seekableByteChannel.close();

        assertTrue(Files.exists(file));
    }

    @Test
    void bucketIsDirectory()
            throws IOException
    {
        Path path = fileSystemAmazon.getPath(bucket, "/");

        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

        assertEquals(0, attrs.size());
        assertNotNull(attrs.creationTime());
        assertNotNull(attrs.lastAccessTime());
        assertNotNull(attrs.lastModifiedTime());
        assertTrue(attrs.isDirectory());
    }

    @Test
    void fileIsReadableBucket()
    {
        Path path = fileSystemAmazon.getPath(bucket, "/");

        boolean readable = Files.isReadable(path);

        assertTrue(readable);
    }

    @Test
    void fileIsReadableBucketFile()
            throws IOException
    {
        Path file = createEmptyFile();

        boolean readable = Files.isReadable(file);

        assertTrue(readable);
    }

    // helpers

    private Path createEmptyDir()
            throws IOException
    {
        Path dir = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString() + "/");

        Files.createDirectory(dir);

        return dir;
    }

    private Path createEmptyFile()
            throws IOException
    {
        Path file = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

        Files.createFile(file);

        return file;
    }

    private Path uploadSingleFile(String content)
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            if (content != null)
            {
                Files.write(linux.getPath("/index.html"), content.getBytes());
            }
            else
            {
                Files.createFile(linux.getPath("/index.html"));
            }

            Path result = fileSystemAmazon.getPath(bucket, UUID.randomUUID().toString());

            Files.copy(linux.getPath("/index.html"), result);

            return result;
        }
    }

    private Path uploadDir()
            throws IOException
    {
        try (FileSystem linux = MemoryFileSystemBuilder.newLinux().build("linux"))
        {
            Path assets = Files.createDirectories(linux.getPath("/upload/assets1"));

            Files.createFile(assets.resolve("index.html"));

            Path img = Files.createDirectory(assets.resolve("img"));

            Files.createFile(img.resolve("Penguins.jpg"));
            Files.createDirectory(assets.resolve("js"));
            Files.createFile(assets.resolve("js").resolve("main.js"));

            Path dir = fileSystemAmazon.getPath(bucket, "0000example" + UUID.randomUUID().toString() + "/");

            Files.exists(assets);
            Files.walkFileTree(assets.getParent(), new CopyDirVisitor(assets.getParent(), dir));

            return dir;
        }
    }

    private void findFileInDirectoryStream(Path bucketPath, Path fileToFind, DirectoryStream<Path> dirStream)
    {
        boolean find = false;

        for (Path path : dirStream)
        {
            // check parent at first level
            assertEquals(bucketPath, path.getParent());

            if (path.equals(fileToFind))
            {
                find = true;

                break;
            }
        }

        assertTrue(find);
    }

}
