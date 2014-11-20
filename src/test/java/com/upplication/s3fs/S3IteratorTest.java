package com.upplication.s3fs;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.google.common.collect.ImmutableMap;

public class S3IteratorTest {
    public static final URI S3_GLOBAL_URI = URI.create("s3:///");
    S3FileSystemProvider provider;
    FileSystem fsMem;

    @Before
    public void cleanup() throws IOException {
        fsMem = MemoryFileSystemBuilder.newLinux().build("linux");
        try{
            FileSystems.getFileSystem(S3_GLOBAL_URI).close();
        }
        catch(FileSystemNotFoundException e){}

        provider = spy(new S3FileSystemProvider());
        // TODO: we need some real temp dir with unique path when is called
        doReturn(Files.createDirectory(fsMem.getPath("/" + UUID.randomUUID().toString())))
                .doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .doReturn(Files.createDirectory(fsMem.getPath("/"+UUID.randomUUID().toString())))
                .when(provider).createTempDir();
        doReturn(new Properties()).when(provider).loadAmazonProperties();
    }

    @After
    public void closeMemory() throws IOException{
        fsMem.close();
    	provider.getFileSystem(S3_GLOBAL_URI).close();
    }

    @Test
    public void iteratorDirectory() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem).withBucket("bucketA").withFile("dir/file1").build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());
        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir/");

        assertIterator(iterator, "file1");
    }



    @Test
    public void iteratorAnotherDirectory() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir2/file1")
                .withFile("dir2/file2")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir2/");

        assertIterator(iterator, "file1", "file2");
    }

    @Test
    public void iteratorWithFileContainsDirectoryName() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir2/dir2-file")
                .withFile("dir2-file2")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir2/");

        assertIterator(iterator, "dir2-file");
    }

    @Test
    public void iteratorWithSubFolderAndSubFiles() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir/file")
                .withFile("dir/file2")
                .withFile("dir/dir/file")
                .withFile("dir/dir2/file")
                .withFile("dir/dir2/dir3/file3")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir/");

        assertIterator(iterator, "file", "file2", "dir", "dir2");
    }

    @Test
    public void iteratorWithSubFolderAndSubFilesAtBucketLevel() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("file")
                .withFile("file2")
                .withFile("dir/file")
                .withFile("dir2/file")
                .withFile("dir2/dir3/file3")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "/");

        assertIterator(iterator, "file", "file2", "dir", "dir2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void iteratorKeyNotEndSlash() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir2/dir2-file")
                .build(provider);
        
        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir2");
    }

    @Test
    public void iteratorFileReturnEmpty() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("file1")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "file1/");

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorEmptyDirectory() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withDirectory("dir")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir/");

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorBucket() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("file1")
                .withFile("file2")
                .withFile("file3")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "/");

        assertIterator(iterator, "file1", "file2", "file3");
    }

    @Test
    public void iteratorMoreThanAmazonS3ClientLimit() throws IOException {
        AmazonS3ClientMockBuilder builder =new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA");

        String filesNameExpected[] = new String[1050];
        for (int i = 0; i < 1050; i++){
            final String name = "file-" + i;
            builder.withFile(name);
            filesNameExpected[i] = name;
        }

        builder.build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "/");

        assertIterator(iterator, filesNameExpected);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove() throws IOException {
        new AmazonS3ClientMockBuilder(fsMem)
                .withBucket("bucketA")
                .withFile("dir/file1")
                .build(provider);

        S3FileSystem s3FileSystem = (S3FileSystem) provider.newFileSystem(S3_GLOBAL_URI, buildFakeEnv());

        S3Iterator iterator = new S3Iterator(s3FileSystem, s3FileSystem.getFileStore("bucketA"), "dir/");
        iterator.remove();
    }


    private Map<String, ?> buildFakeEnv(){
        return ImmutableMap.<String, Object> builder()
                .put(S3FileSystemProvider.ACCESS_KEY, "access key")
                .put(S3FileSystemProvider.SECRET_KEY, "secret key").build();
    }

    private void assertIterator(Iterator<Path> iterator, final String ... files) throws IOException {

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        Set<String> filesNamesExpected = new HashSet<>(Arrays.asList(files));
        Set<String> filesNamesActual = new HashSet<>();

        while (iterator.hasNext()) {
            Path path = iterator.next();
            String fileName = path.getFileName().toString();
            filesNamesActual.add(fileName);
        }

        assertEquals(filesNamesExpected, filesNamesActual);
    }
}
