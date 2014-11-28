package com.upplication.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.marschall.com.sun.nio.zipfs.ZipFileAttributeView;
import com.upplication.s3fs.S3FileStoreAttributeView.AttrID;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.UnsupportedFileStoreAttributeView;

public class S3FileStoreTest extends S3UnitTest {
	private S3FileSystem fileSystem;
	private S3FileStore fileStore;

	@Before
	public void prepareFileStore() throws IOException {
		fileSystem = (S3FileSystem) FileSystems.getFileSystem(S3_GLOBAL_URI);
		S3Path root = fileSystem.getPath("/bucket");
		Files.createFile(root.resolve("placeholder"));
		fileStore = root.getFileStore();
		Files.createFile(fileSystem.getPath("/bucket2").resolve("placeholder"));
	}

	@Test
	public void bucketConstructor() {
		Owner owen = new Owner("2", "Owen");
		Bucket bucket = new Bucket("bucketname");
		bucket.setOwner(owen);
		S3FileStore s3FileStore = new S3FileStore(fileSystem, bucket);
		assertEquals("bucketname", s3FileStore.name());
		assertEquals("Owen", s3FileStore.getOwner().getDisplayName());
	}

	@Test
	public void nameConstructor() {
		S3FileStore s3FileStore = new S3FileStore(fileSystem, "name");
		assertEquals("name", s3FileStore.name());
		assertEquals("Mock", s3FileStore.getOwner().getDisplayName());
	}

	@Test
	public void nameConstructorAlreadyExists() {
		S3FileStore s3FileStore = new S3FileStore(fileSystem, "bucket2");
		assertEquals("bucket2", s3FileStore.name());
		assertEquals("Mock", s3FileStore.getOwner().getDisplayName());
	}

	@Test
	public void getFileStoreAttributeView() {
		S3FileStoreAttributeView fileStoreAttributeView = fileStore.getFileStoreAttributeView(S3FileStoreAttributeView.class);
		assertEquals("S3FileStoreAttributeView", fileStoreAttributeView.name());
		assertEquals("bucket", fileStoreAttributeView.getAttribute(AttrID.name.name()));
		assertNotNull(fileStoreAttributeView.getAttribute(AttrID.creationDate.name()));
		assertEquals("Mock", fileStoreAttributeView.getAttribute(AttrID.ownerDisplayName.name()));
		assertEquals("1", fileStoreAttributeView.getAttribute(AttrID.ownerId.name()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void getUnsupportedFileStoreAttributeView() {
		fileStore.getFileStoreAttributeView(UnsupportedFileStoreAttributeView.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void bucketInputStream() throws IOException {
		S3Path bucket = fileSystem.getPath("/bucket");
		fileStore.getInputStream(bucket, StandardOpenOption.APPEND);
	}

	@Test(expected = IllegalArgumentException.class)
	public void optionsSupport() throws IOException {
		S3Path placeholder = fileSystem.getPath("/bucket/placeholder");
		fileStore.getInputStream(placeholder, StandardOpenOption.APPEND);
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
		S3Path root = fileSystem.getPath("/newbucket");
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
		S3Path root = fileSystem.getPath("/newer-bucket");
		Path resolve = root.resolve("folder");
		Path path = Files.createDirectories(resolve);
		assertEquals("/newer-bucket/folder", path.toAbsolutePath().toString());
	}

	@Test
	public void createDirectoryInExistingBucket() throws IOException {
		S3Path root = fileSystem.getPath("/bucket");
		Path path = Files.createDirectory(root.resolve("folder"));
		assertEquals("/bucket/folder", path.toAbsolutePath().toString());
	}

	@Test
	public void createDirectoryWithExistingBuckeNamet() throws IOException {
		S3FileStore s3fs = new S3FileStore(fileSystem, "bucket5");
		S3Path bucket5 = new S3Path(fileSystem, s3fs);
		S3Path root = fileSystem.getPath("/bucket5");
		S3Path folder2 = (S3Path) root.resolve("folder2");
		folder2.getFileStore().createDirectory(folder2);
		Path folder = Files.createDirectory(bucket5.resolve("folder"));
		assertEquals("/bucket5/folder", folder.toAbsolutePath().toString());
	}

	@Test
	public void createDirectoryWithEndSlash() throws IOException {
		S3Path root = fileSystem.getPath("/bucket");
		S3Path path = (S3Path) Files.createDirectory(root.resolve("nonexistingfolder/"));
		assertEquals("/bucket/nonexistingfolder", path.toAbsolutePath().toString());
	}

	@Test(expected=FileAlreadyExistsException.class)
	public void createAlreadyExistingDirectory() throws IOException {
		S3Path root = fileSystem.getPath("/bucket");
		S3Path path = (S3Path) Files.createDirectory(root.resolve("folder"));
		path.getFileStore().createDirectory(path);
		assertEquals("/bucket/folder", path.toAbsolutePath().toString());
	}

	@Test
	public void getS3ObjectSummary() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		S3Path file1 = (S3Path) root.resolve("file1");
		OutputStream outputStream = Files.newOutputStream(file1, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		String contentString = "Some content String";
		outputStream.write(contentString.getBytes());
		outputStream.close();
		S3ObjectSummary file1ObjectSummary = file1.getFileStore().getS3ObjectSummary(file1);
		assertEquals("bucket4", file1ObjectSummary.getBucketName());
		assertEquals(null, file1ObjectSummary.getETag());
		assertEquals("file1", file1ObjectSummary.getKey());
		assertNotNull(file1ObjectSummary.getLastModified());
		Owner owner = file1ObjectSummary.getOwner();
		assertNotNull(owner);
		assertEquals("Mock", owner.getDisplayName());
		assertEquals("1", owner.getId());
		assertEquals(19, file1ObjectSummary.getSize());
	}

	@Test
	public void getFileContent() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		S3Path content = (S3Path) root.resolve("content");
		OutputStream outputStream = Files.newOutputStream(content, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		String contentString = "Some content String";
		outputStream.write(contentString.getBytes());
		outputStream.close();
		S3Path path = (S3Path) Paths.get(content.toUri());
		byte[] bytes = path.readAllBytes();
		assertEquals(contentString, new String(bytes));
	}

	@Test(expected = AmazonS3Exception.class)
	public void getFileContentFail() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		S3Path content = (S3Path) root.resolve("content");
		OutputStream outputStream = Files.newOutputStream(content, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		String contentString = "Some content String";
		outputStream.write(contentString.getBytes());
		outputStream.close();

		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		AmazonS3Exception toBeThrown = new AmazonS3Exception("We messed up");
		toBeThrown.setStatusCode(500);
		doThrow(toBeThrown).when(client).getObject("bucket4", "content");
		S3Path path = (S3Path) Paths.get(content.toUri());
		path.readAllBytes();
	}

	@Test(expected = NoSuchFileException.class)
	public void getS3ObjectSummary404() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		S3Path file1 = (S3Path) root.resolve("file1");
		file1.getFileStore().getS3ObjectSummary(file1);
	}

	@Test(expected = AmazonS3Exception.class)
	public void getS3ObjectSummary500() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		AmazonS3Exception toBeThrown = new AmazonS3Exception("We messed up");
		toBeThrown.setStatusCode(500);
		doThrow(toBeThrown).when(client).getObjectAcl("bucket4", "file2");
		S3Path root = fileSystem.getPath("/bucket4");
		S3Path file2 = (S3Path) root.resolve("file2");
		Files.createFile(file2);
		file2.getFileStore().getS3ObjectSummary(file2);
	}

	@Test
	public void readAttributes() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		S3Path folder = (S3Path) root.resolve("folder/");
		Files.createDirectory(folder);
		BasicFileAttributes attributes = folder.readAttributes(BasicFileAttributes.class);
		assertNotNull(attributes);
		assertEquals("folder/", attributes.fileKey());
		assertTrue(attributes.isDirectory());
		assertEquals(0, attributes.size());
		assertFalse(attributes.isOther());
		assertFalse(attributes.isRegularFile());
		assertFalse(attributes.isSymbolicLink());
		assertNotNull(attributes.creationTime());
	}

	@Test
	public void readAttributesVirtualFolder() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		Files.createDirectory(root.resolve("folder/subfolder"));
		S3Path folder = (S3Path) root.resolve("folder");
		BasicFileAttributes attributes = folder.readAttributes(BasicFileAttributes.class);
		assertNotNull(attributes);
		assertEquals("folder/", attributes.fileKey());
		assertTrue(attributes.isDirectory());
		assertEquals(0, attributes.size());
		assertFalse(attributes.isOther());
		assertFalse(attributes.isRegularFile());
		assertFalse(attributes.isSymbolicLink());
		assertNotNull(attributes.creationTime());
	}

	@Test
	public void readAttributesFile() throws IOException {
		S3Path root = fileSystem.getPath("/bucket4");
		Files.createDirectory(root.resolve("folder/"));
		Files.createFile(root.resolve("folder/file"));
		S3Path file = (S3Path) root.resolve("folder/file");
		BasicFileAttributes attributes = file.readAttributes(BasicFileAttributes.class);
		assertNotNull(attributes);
		assertEquals("folder/file", attributes.fileKey());
		assertFalse(attributes.isDirectory());
		assertEquals(0, attributes.size());
		assertFalse(attributes.isOther());
		assertTrue(attributes.isRegularFile());
		assertFalse(attributes.isSymbolicLink());
		assertNotNull(attributes.creationTime());
	}

	@Test
	public void compareable() {
		S3FileStore store = fileSystem.getPath("/bucket").getFileStore();
		S3FileStore other = fileSystem.getPath("/bucket1").getFileStore();
		S3FileStore differentHost = ((S3Path) Paths.get(URI.create("s3://localhost/bucket"))).getFileStore();
		S3FileStore shouldBeTheSame = ((S3Path) Paths.get(URI.create("s3:///bucket"))).getFileStore();
		S3FileStore noFs1 = new S3FileStore(null, new Bucket("bucket"));
		S3FileStore noFs2 = new S3FileStore(null, new Bucket("bucket1"));
		S3FileStore noFsSameAs1 = new S3FileStore(null, new Bucket("bucket"));
		S3FileStore noFsNoName1 = new S3FileStore(null, new Bucket(null));
		S3FileStore noFsNoName2 = new S3FileStore(null, new Bucket(null));
		Comparable<S3FileStore> s3FileStore = new S3FileStore(null, new Bucket("bucket"));

		assertEquals(0, store.compareTo(store));
		assertEquals(1, store.compareTo(other));
		assertEquals(0, store.compareTo(differentHost));
		assertEquals(0, store.compareTo(shouldBeTheSame));
		assertEquals(0, store.compareTo(noFs1));
		assertEquals(0, noFs1.compareTo(noFsSameAs1));
		assertEquals(0, s3FileStore.compareTo(noFsSameAs1));

		assertEquals(1685134831, store.hashCode());
		assertEquals(1450672708, other.hashCode());
		assertEquals(-2000105814, differentHost.hashCode());
		assertEquals(1685134831, shouldBeTheSame.hashCode());
		assertEquals(1342376576, noFs2.hashCode());
		assertEquals(459819936, noFsSameAs1.hashCode());
		assertEquals(953312, noFsNoName1.hashCode());
		assertEquals(953312, noFsNoName2.hashCode());

		assertFalse(store.equals(other));
		assertFalse(store.equals(differentHost));
		assertTrue(store.equals(shouldBeTheSame));
		assertFalse(store.equals(shouldBeTheSame.getOwner()));
		assertFalse(store.equals(noFs1));
		assertFalse(noFs1.equals(store));
		assertFalse(noFs1.equals(noFs2));
		assertTrue(noFs1.equals(noFsSameAs1));
		assertFalse(noFsNoName1.equals(noFs1));
		assertTrue(noFsNoName1.equals(noFsNoName2));
	}
}