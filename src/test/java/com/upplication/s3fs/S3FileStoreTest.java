package com.upplication.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Owner;
import com.github.marschall.com.sun.nio.zipfs.ZipFileAttributeView;
import com.upplication.s3fs.S3FileStoreAttributeView.AttrID;
import com.upplication.s3fs.util.UnsupportedFileStoreAttributeView;

public class S3FileStoreTest extends S3UnitTest {
	private FileSystem fileSystem;
	private S3FileStore fileStore;

	@Before
	public void prepareFileStore() throws IOException {
		fileSystem = FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path root = (S3Path) fileSystem.getPath("/bucket");
		Files.createFile(root);
		fileStore = root.getFileStore();
		Files.createFile(fileSystem.getPath("/bucket2"));
	}

	@Test
	public void bucketConstructor() {
		Owner owen = new Owner("2", "Owen");
		Bucket bucket = new Bucket("bucketname");
		bucket.setOwner(owen);
		S3FileStore s3FileStore = new S3FileStore((S3FileSystem) fileSystem, bucket);
		assertEquals("bucketname", s3FileStore.name());
		assertEquals("Owen", s3FileStore.getOwner().getDisplayName());
	}

	@Test
	public void nameConstructor() {
		S3FileStore s3FileStore = new S3FileStore((S3FileSystem) fileSystem, "name");
		assertEquals("name", s3FileStore.name());
		assertEquals("Mock", s3FileStore.getOwner().getDisplayName());
	}

	@Test
	public void nameConstructorAlreadyExists() {
		S3FileStore s3FileStore = new S3FileStore((S3FileSystem) fileSystem, "bucket2");
		assertEquals("bucket", s3FileStore.name());
		assertEquals("Mock", s3FileStore.getOwner().getDisplayName());
	}

	@Test
	public void getFileStoreAttributeView() {
		S3FileStoreAttributeView fileStoreAttributeView = fileStore.getFileStoreAttributeView(S3FileStoreAttributeView.class);
		assertEquals("bucket", fileStoreAttributeView.getAttribute(AttrID.name.name()));
		assertNotNull(fileStoreAttributeView.getAttribute(AttrID.creationDate.name()));
		assertEquals("Mock", fileStoreAttributeView.getAttribute(AttrID.ownerDisplayName.name()));
		assertEquals("1", fileStoreAttributeView.getAttribute(AttrID.ownerId.name()));
	}

	@Test(expected=IllegalArgumentException.class)
	public void getUnsupportedFileStoreAttributeView() {
		fileStore.getFileStoreAttributeView(UnsupportedFileStoreAttributeView.class);
	}

	@Test
	public void getAttributes() throws IOException {
		assertEquals("bucket", fileStore.getAttribute(AttrID.name.name()));
		assertNotNull(fileStore.getAttribute(AttrID.creationDate.name()));
		assertEquals("Mock", fileStore.getAttribute(AttrID.ownerDisplayName.name()));
		assertEquals("1", fileStore.getAttribute(AttrID.ownerId.name()));
	}
	

	@Test
	public void getOwner() {
		Owner owner = fileStore.getOwner();
		assertEquals("Mock", owner.getDisplayName());
		assertEquals("1", owner.getId());
	}

	@Test
	public void getRootDirectory() {
		S3Path rootDirectory = fileStore.getRootDirectory();
		assertEquals("bucket", rootDirectory.getFileName().toString());
		assertEquals("/bucket/", rootDirectory.toAbsolutePath().toString());
		assertEquals("s3:///bucket/", rootDirectory.toUri().toString());
	}

	@Test
	public void getSpaces() throws IOException {
		S3Path root = (S3Path) fileSystem.getPath("/newbucket");
		Files.createFile(root);
		S3FileStore newFileStore = root.getFileStore();
		assertEquals("newbucket", newFileStore.name());
		assertEquals("S3Bucket", newFileStore.type());
		assertEquals(false, newFileStore.isReadOnly());
		assertEquals(Long.MAX_VALUE, newFileStore.getTotalSpace());
		assertEquals(Long.MAX_VALUE, newFileStore.getUsableSpace());
		assertEquals(Long.MAX_VALUE, newFileStore.getUnallocatedSpace());
		assertFalse(newFileStore.supportsFileAttributeView(ZipFileAttributeView.class));
		assertFalse(newFileStore.supportsFileAttributeView("zip"));
	}

	@Test
	public void createDirectoryInNewBucket() throws IOException {
		S3Path root = (S3Path) fileSystem.getPath("/newer-bucket");
		Path resolve = root.resolve("folder");
		Path path = Files.createDirectories(resolve);
		assertEquals("/newer-bucket/folder", path.toAbsolutePath().toString());
	}

	@Ignore
	@Test
	public void createDirectoryInExistingBucket() throws IOException {
		S3Path root = (S3Path) fileSystem.getPath("/bucket3");
		Path resolve = root.resolve("folder");
		Files.createFile(fileSystem.getPath("/bucket3/file"));
		
		Path path = Files.createDirectories(resolve);
		assertEquals("/bucket2/folder", path.toAbsolutePath().toString());
	}
	
	
}