package com.upplication.s3fs;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.collect.Lists;

/**
 * S3 iterator over folders at first level.
 * Future verions of this class should be return the elements
 * in a incremental way when the #next() method is called.
 */
public class S3Iterator implements Iterator<Path> {
	S3FileSystem s3FileSystem;
	private S3FileStore fileStore;
	String key;

	private Iterator<S3Path> it;

	public S3Iterator(S3Path path) {
		this.fileStore = path.getFileStore();
		this.key = path.getKey().length() == 0 ? "" : path.getKey() + "/";
		this.s3FileSystem = path.getFileSystem();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public S3Path next() {
		return getIterator().next();
	}

	@Override
	public boolean hasNext() {
		return getIterator().hasNext();
	}

	private Iterator<S3Path> getIterator() {
		if (it == null) {
			List<S3Path> listPath = Lists.newArrayList();
			// iterator over this list
			ObjectListing current = fileStore.listObjects(fileStore.buildRequest(key));

			while (current.isTruncated()) {
				// parse the elements
				fileStore.parseObjectListing(key, listPath, current);
				// continue
				current = fileStore.listNextBatchOfObjects(current);
			}

			fileStore.parseObjectListing(key, listPath, current);

			it = listPath.iterator();
		}

		return it;
	}
}
