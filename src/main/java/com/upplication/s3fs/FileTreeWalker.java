package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

class FileTreeWalker {
	private S3FileStore fileStore;
	private FileVisitor<? super Path> visitor;
	private int maxDepth;

	public FileTreeWalker(S3FileStore fileStore, FileVisitor<? super Path> visitor, int maxDepth) {
		this.fileStore = fileStore;
		this.visitor = visitor;
		this.maxDepth = maxDepth;
	}

	/**
	 * Walk file tree starting at the given file
	 */
	void walk(S3Path start) throws IOException {
		if(maxDepth != Integer.MAX_VALUE) {
			Files.walkFileTree(start, visitor);
			return;
		}

		List<S3Path> listPath = gatherResults(start);

		Iterator<S3Path> iterator = listPath.iterator();
		PeekingIterator<S3Path> peekingIterator = Iterators.peekingIterator(iterator);
		FileVisitResult result = walk(start, peekingIterator);
		Objects.requireNonNull(result, "FileVisitor returned null");
	}

	protected List<S3Path> gatherResults(S3Path start) {
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(fileStore.name());
		String key = start.getKey();
		request.setPrefix(key);
		
		List<S3Path> listPath = Lists.newArrayList();
		// iterator over this list
		ObjectListing current = fileStore.listObjects(request);

		while (current.isTruncated()) {
			// parse the elements
			fileStore.parseObjects(listPath, current);
			// continue
			current = fileStore.listNextBatchOfObjects(current);
		}

		fileStore.parseObjects(listPath, current);
		return listPath;
	}

	/**
	 * @param   parent
	 *          the directory to visit
	 * @param   iterator
	 *          depth remaining
	 */
	private FileVisitResult walk(S3Path parent, PeekingIterator<S3Path> iterator) throws IOException {
		if(!iterator.hasNext())
			return FileVisitResult.CONTINUE;
		S3Path current = iterator.next();

		IOException exc = null;
		BasicFileAttributes attrs;
		try {
			attrs = current.getBasicFileAttributes(true);
		} catch (IOException e) {
			return visitor.visitFileFailed(current, e);
		}

		// file is not a directory
		if (attrs != null && !attrs.isDirectory())
			return visitor.visitFile(current, attrs);

		FileVisitResult result = visitor.preVisitDirectory(current, attrs);
		if (result != FileVisitResult.CONTINUE) {
			return result;
		}
		// visit the 'directory'
		while(iterator.hasNext() && iterator.peek().getParent().equals(current)) {
			walk(current, iterator);
		}
		return visitor.postVisitDirectory(current, exc);
	}
}