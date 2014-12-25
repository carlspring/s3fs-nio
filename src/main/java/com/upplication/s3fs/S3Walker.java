package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

class S3Walker {
	private FileVisitor<? super Path> visitor;
	private int maxDepth;

	public S3Walker(FileVisitor<? super Path> visitor, int maxDepth) {
		this.visitor = visitor;
		this.maxDepth = maxDepth;
	}

	/**
	 * Walk file tree starting at the given file
	 */
	void walk(S3Path start) throws IOException {
		// Limiting depth isn't supported at the moment. Call default walkFileTree for that.
		if (maxDepth != Integer.MAX_VALUE) {
			Files.walkFileTree(start, visitor);
			return;
		}
		S3Iterator iter = new S3Iterator(start, true);
		if (!iter.hasNext())
			visitor.visitFileFailed(start, new NoSuchFileException(start.getFileStore().name() + "/" + start.getKey()));
		FileVisitResult result = walk(Iterators.peekingIterator(iter));
		Objects.requireNonNull(result, "FileVisitor returned null");
	}

	private FileVisitResult walk(PeekingIterator<Path> iterator) throws IOException {
		if (!iterator.hasNext())
			return FileVisitResult.CONTINUE;
		S3Path current = (S3Path) iterator.next();
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
		if (result == FileVisitResult.TERMINATE)
			return result;
		// visit the 'directory'
		FileVisitResult subresult = null;
		while (iterator.hasNext() && iterator.peek().getParent().equals(current)) {
			if(subresult == FileVisitResult.TERMINATE)
				return subresult;
			if (result == FileVisitResult.SKIP_SUBTREE || (subresult != null && subresult == FileVisitResult.SKIP_SIBLINGS))
				iterator.next();
			else
				subresult = walk(iterator);
		}
		return visitor.postVisitDirectory(current, exc);
	}
}