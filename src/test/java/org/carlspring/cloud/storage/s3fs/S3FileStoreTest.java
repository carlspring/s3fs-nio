package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.S3FileStoreAttributeView.AttrID;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;
import org.carlspring.cloud.storage.s3fs.util.UnsupportedFileStoreAttributeView;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.github.marschall.com.sun.nio.zipfs.ZipFileAttributeView;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.Owner;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3FileStoreTest
        extends S3UnitTestBase
{


    private S3FileStore fileStore;


    @BeforeEach
    public void prepareFileStore()
            throws IOException
    {
        Map<String, Object> env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access-mocked")
                                                                        .put(SECRET_KEY, "secret-mocked")
                                                                        .build();

        fileSystem = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);

        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("bucket").file("placeholder");
        client.bucket("bucket2").file("placeholder");

        S3Path path = (S3Path) fileSystem.getPath("/bucket/");

        fileStore = path.getFileStore();
    }

    @Test
    void bucketConstructor()
    {
        S3FileStore s3FileStore = new S3FileStore((S3FileSystem) fileSystem, "name");

        assertEquals("name", s3FileStore.name());
        assertEquals("Mock", s3FileStore.getOwner().displayName());
    }

    @Test
    void nameConstructorAlreadyExists()
    {
        S3FileStore s3FileStore = new S3FileStore((S3FileSystem) fileSystem, "bucket2");

        assertEquals("bucket2", s3FileStore.name());
        assertEquals("Mock", s3FileStore.getOwner().displayName());
    }

    @Test
    void getFileStoreAttributeView()
    {
        S3FileStoreAttributeView fileStoreAttributeView =
                fileStore.getFileStoreAttributeView(S3FileStoreAttributeView.class);

        assertEquals("S3FileStoreAttributeView", fileStoreAttributeView.name());
        assertEquals("bucket", fileStoreAttributeView.getAttribute(AttrID.name.name()));
        assertNotNull(fileStoreAttributeView.getAttribute(AttrID.creationDate.name()));
        assertEquals("Mock", fileStoreAttributeView.getAttribute(AttrID.ownerDisplayName.name()));
        assertEquals("1", fileStoreAttributeView.getAttribute(AttrID.ownerId.name()));
    }

    @Test
    void getUnsupportedFileStoreAttributeView()
    {
        final Class<UnsupportedFileStoreAttributeView> attributeViewClass = UnsupportedFileStoreAttributeView.class;
        final Exception exception = assertThrows(IllegalArgumentException.class,
                                                 () -> fileStore.getFileStoreAttributeView(attributeViewClass));

        assertNotNull(exception);
    }

    @Test
    void getAttributes()
    {
        assertEquals("bucket", fileStore.getAttribute(AttrID.name.name()));
        assertNotNull(fileStore.getAttribute(AttrID.creationDate.name()));
        assertEquals("Mock", fileStore.getAttribute(AttrID.ownerDisplayName.name()));
        assertEquals("1", fileStore.getAttribute(AttrID.ownerId.name()));
    }

    @Test
    void getOwner()
    {
        Owner owner = fileStore.getOwner();

        assertEquals("Mock", owner.displayName());
        assertEquals("1", owner.id());
    }

    @Test
    void getRootDirectory()
    {
        S3Path rootDirectory = fileStore.getRootDirectory();

        assertNull(rootDirectory.getFileName());
        assertTrue(rootDirectory.isAbsolute());
        assertEquals("/bucket/", rootDirectory.toAbsolutePath().toString());
        assertEquals("s3://access-mocked@s3.test.amazonaws.com/bucket/", rootDirectory.toUri().toString());
    }

    @Test
    void getSpaces()
            throws IOException
    {
        Path root = fileSystem.getPath("/newbucket");
        Files.createFile(root);
        S3FileStore newFileStore = ((S3Path) root).getFileStore();

        assertEquals("newbucket", newFileStore.name());
        assertEquals("S3Bucket", newFileStore.type());
        assertFalse(newFileStore.isReadOnly());
        assertEquals(Long.MAX_VALUE, newFileStore.getTotalSpace());
        assertEquals(Long.MAX_VALUE, newFileStore.getUsableSpace());
        assertEquals(Long.MAX_VALUE, newFileStore.getUnallocatedSpace());
        assertFalse(newFileStore.supportsFileAttributeView(ZipFileAttributeView.class));
        assertFalse(newFileStore.supportsFileAttributeView("zip"));
    }

    @Test
    void comparable()
            throws IOException
    {
        S3FileStore store = ((S3Path) fileSystem.getPath("/bucket")).getFileStore();
        S3FileStore other = ((S3Path) fileSystem.getPath("/bucket1")).getFileStore();

        FileSystem fileSystemLocalhost = FileSystems.newFileSystem(URI.create("s3://localhost/"), buildFakeEnv());
        FileSystem fileSystemDefault = FileSystems.getFileSystem(store.getFileSystem().getPath("/bucket").toUri());

        S3FileStore differentHost = ((S3Path) fileSystemLocalhost.getPath("/bucket")).getFileStore();
        S3FileStore shouldBeTheSame = ((S3Path) fileSystemDefault.getPath("/bucket")).getFileStore();
        S3FileStore noFs1 = new S3FileStore(null, "bucket");
        S3FileStore noFs2 = new S3FileStore(null, "bucket1");
        S3FileStore noFsSameAs1 = new S3FileStore(null, "bucket");
        S3FileStore noFsNoName1 = new S3FileStore(null, null);
        S3FileStore noFsNoName2 = new S3FileStore(null, null);

        Comparable<S3FileStore> s3FileStore = new S3FileStore(null, "bucket");

        assertEquals(0, store.compareTo(store));
        assertEquals(1, store.compareTo(other));
        assertEquals(0, store.compareTo(differentHost));
        assertEquals(0, store.compareTo(shouldBeTheSame));
        assertEquals(0, store.compareTo(noFs1));
        assertEquals(0, noFs1.compareTo(noFsSameAs1));
        assertEquals(0, s3FileStore.compareTo(noFsSameAs1));

        assertNotEquals(other, store);
        assertNotEquals(differentHost, store);
        assertEquals(shouldBeTheSame, store);
        assertEquals(shouldBeTheSame.getOwner(), store.getOwner());
        assertNotEquals(noFs1, store);
        assertNotEquals(store, noFs1);
        assertNotEquals(noFs2, noFs1);
        assertEquals(noFsSameAs1, noFs1);
        assertNotEquals(noFs1, noFsNoName1);
        assertEquals(noFsNoName2, noFsNoName1);
    }

    private Map<String, ?> buildFakeEnv()
    {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access-key")
                                                     .put(SECRET_KEY, "secret-key")
                                                     .build();
    }

}
