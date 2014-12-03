package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Objects;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

class S3Walker {
	private S3FileStore fileStore;
	private FileVisitor<? super Path> visitor;
	private int maxDepth;

	public S3Walker(S3FileStore fileStore, FileVisitor<? super Path> visitor, int maxDepth) {
		this.fileStore = fileStore;
		this.visitor = visitor;
		this.maxDepth = maxDepth;
	}

	/**
	 * Walk file tree starting at the given file
	 */
	void walk(S3Path start) throws IOException {
		// Limiting depth isn't supported at the moment. Call default walkFileTree for that.
		if(maxDepth != Integer.MAX_VALUE) {
			Files.walkFileTree(start, visitor);
			return;
		}

		List<S3Path> listPath = gatherResults(start);

		FileVisitResult result = walk(Iterators.peekingIterator(listPath.iterator()));
		Objects.requireNonNull(result, "FileVisitor returned null");
	}

	protected List<S3Path> gatherResults(S3Path start) {
		List<S3Path> listPath = Lists.newArrayList();
		// iterator over this list
		ObjectListing current = fileStore.listObjects(new ListObjectsRequest(fileStore.name(), start.getKey(), start.getKey(), null, Integer.MAX_VALUE));

		while (current.isTruncated()) {
			fileStore.parseObjects(listPath, current);// parse the elements
			current = fileStore.listNextBatchOfObjects(current);// continue
		}
		fileStore.parseObjects(listPath, current);
		return listPath;
	}

	private FileVisitResult walk(PeekingIterator<S3Path> iterator) throws IOException {
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
		if (result != FileVisitResult.CONTINUE)
			return result;
		// visit the 'directory'
		while(iterator.hasNext() && iterator.peek().getParent().equals(current))
			walk(iterator);
		return visitor.postVisitDirectory(current, exc);
	}
}