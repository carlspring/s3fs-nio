package com.upplication.s3fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.MockBucket;

public class S3WalkerTest extends S3UnitTest {

	class RegisteringVisitor implements FileVisitor<Path> {
		List<String> visitOrder = new ArrayList<>();

		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			visitOrder.add("preVisitDirectory(" + dir.toAbsolutePath().toString() + ")");
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			visitOrder.add("visitFile(" + file.toAbsolutePath().toString() + ")");
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
			visitOrder.add("visitFileFailed(" + file.toAbsolutePath().toString() + ", " + exc.getClass().getSimpleName() + "(" + exc.getMessage() + "))");
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			visitOrder.add("postVisitDirectory(" + dir.toAbsolutePath().toString() + ")");
			return FileVisitResult.CONTINUE;
		}

		public List<String> getVisitOrder() {
			return visitOrder;
		}
	}

	class CheckVisitor implements FileVisitor<Path> {
		private Iterator<String> iterator;

		public CheckVisitor(Iterator<String> iterator) {
			this.iterator = iterator;
		}

		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			assertTrue(iterator.hasNext());
			assertEquals(iterator.next(), "preVisitDirectory(" + dir.toAbsolutePath().toString() + ")");
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			assertTrue(iterator.hasNext());
			assertEquals(iterator.next(), "visitFile(" + file.toAbsolutePath().toString() + ")");
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
			assertTrue(iterator.hasNext());
			assertEquals(iterator.next(), "visitFileFailed(" + file.toAbsolutePath().toString() + ", " + exc.getClass().getSimpleName() + "(" + exc.getMessage() + "))");
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			assertTrue(iterator.hasNext());
			assertEquals(iterator.next(), "postVisitDirectory(" + dir.toAbsolutePath().toString() + ")");
			return FileVisitResult.CONTINUE;
		}
	}

	@Test
	public void walkFileTree() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		MockBucket mocket = client.bucket("/tree");
		mocket.dir("folder", "folder/subfolder1");
		mocket.file("folder/subfolder1/file1.1", "folder/subfolder1/file1.2", "folder/subfolder1/file1.3", "folder/subfolder1/file1.4");
		mocket.file("folder/subfolder2/file2.1", "folder/subfolder2/file2.2", "folder/subfolder2/file2.3", "folder/subfolder2/file2.4");

		S3Path folder = (S3Path) Paths.get(URI.create(S3_GLOBAL_URI + "tree/folder"));
		reset(client);
		RegisteringVisitor registrar = new RegisteringVisitor();
		Files.walkFileTree(folder, registrar);
		verify(client, times(6)).listObjects(any(ListObjectsRequest.class));

		final Iterator<String> iterator = registrar.getVisitOrder().iterator();
		reset(client);
		folder.walkFileTree(new CheckVisitor(iterator));
		assertFalse("Iterator should have been  exhausted.", iterator.hasNext());
		verify(client, times(1)).listObjects(any(ListObjectsRequest.class));

		reset(client);
		Iterator<String> iter = registrar.getVisitOrder().iterator();
		folder.walkFileTree(new CheckVisitor(iter), 20);
		assertFalse("Iterator should have been  exhausted.", iter.hasNext());
		verify(client, times(6)).listObjects(any(ListObjectsRequest.class));
	}

	@Test
	public void walkEmptyFileTree() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("tree").dir("folder");

		S3Path folder = (S3Path) Paths.get(URI.create(S3_GLOBAL_URI + "tree/folder"));
		RegisteringVisitor registrar = new RegisteringVisitor();
		ObjectListing empty = new ObjectListing();
		empty.setBucketName("tree");
		empty.setPrefix("/tree/folder");
		empty.getObjectSummaries().clear();

		reset(client);
		Files.walkFileTree(folder, registrar);
		verify(client, times(2)).listObjects(any(ListObjectsRequest.class));

		final Iterator<String> iterator = registrar.getVisitOrder().iterator();
		reset(client);

		folder.walkFileTree(new CheckVisitor(iterator));
		assertFalse("Iterator should have been  exhausted.", iterator.hasNext());
		verify(client, times(1)).listObjects(any(ListObjectsRequest.class));
	}

	@Test
	public void walkLargeFileTree() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		MockBucket mocket = client.bucket("/tree");
		for (int i = 0; i < 21; i++) {
			for (int j = 0; j < 50; j++) {
				StringBuilder filename = new StringBuilder("folder/subfolder");
				if (i < 10)
					filename.append("0");
				filename.append(i);
				filename.append("/file");
				if (j < 10)
					filename.append("0");
				filename.append(j);
				mocket.file(filename.toString());
			}
		}
		S3Path folder = (S3Path) Paths.get(URI.create(S3_GLOBAL_URI + "tree/folder"));

		reset(client);
		RegisteringVisitor registrar = new RegisteringVisitor();
		Files.walkFileTree(folder, registrar);
		assertEquals(1094, registrar.getVisitOrder().size());
		verify(client, times(44)).listObjects(any(ListObjectsRequest.class));

		final Iterator<String> iterator = registrar.getVisitOrder().iterator();
		reset(client);
		folder.walkFileTree(new CheckVisitor(iterator));
		assertFalse("Iterator should have been  exhausted.", iterator.hasNext());
		verify(client, times(1)).listObjects(any(ListObjectsRequest.class));
	}

	@Test
	public void noSuchElementTreeWalk() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		client.bucket("/tree");
		S3Path folder = (S3Path) Paths.get(URI.create(S3_GLOBAL_URI + "tree/folder"));
		reset(client);
		RegisteringVisitor registrar = new RegisteringVisitor();
		Files.walkFileTree(folder, registrar);
		assertEquals(1, registrar.getVisitOrder().size());
		verify(client, times(1)).listObjects(any(ListObjectsRequest.class));

		final Iterator<String> iterator = registrar.getVisitOrder().iterator();
		reset(client);
		folder.walkFileTree(new CheckVisitor(iterator));
		assertFalse("Iterator should have been  exhausted.", iterator.hasNext());
		verify(client, times(1)).listObjects(any(ListObjectsRequest.class));
	}

	@Test
	public void skippingWalk() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		MockBucket mocket = client.bucket("/tree");
		mocket.dir("folder", "folder/subfolder1");
		mocket.file("folder/subfolder1/file1.1", "folder/subfolder1/file1.2");
		mocket.file("folder/subfolder2/file2.1", "folder/subfolder2/file2.2");
		mocket.file("folder/subfolder3/file3.1", "folder/subfolder3/file3.2");
		mocket.file("folder/subfolder4/file4.1", "folder/subfolder4/file4.2");

		S3Path folder = (S3Path) Paths.get(URI.create(S3_GLOBAL_URI + "tree/folder"));
		reset(client);
		final List<String> visitation = new ArrayList<String>();
		FileVisitor<Path> visitor = new FileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				String fileName = dir.getFileName().toString();
				if(fileName.equals("subfolder2"))
					return FileVisitResult.SKIP_SUBTREE;
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				String name = file.getFileName().toString();
				visitation.add(name);
				if(name.equals("file3.1"))
					return FileVisitResult.SKIP_SIBLINGS;
				if(name.equals("file4.1"))
					return FileVisitResult.TERMINATE;
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				return FileVisitResult.CONTINUE;
			}
		};
		Files.walkFileTree(folder, visitor);
		assertEquals(Arrays.asList("file1.1", "file1.2", "file3.1", "file4.1"), visitation);
		
		visitation.clear();
		folder.walkFileTree(visitor);
		assertEquals(Arrays.asList("file1.1", "file1.2", "file3.1", "file4.1"), visitation);
	}
}