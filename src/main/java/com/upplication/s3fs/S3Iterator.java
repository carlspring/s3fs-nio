package com.upplication.s3fs;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.collect.Lists;

/**
 * S3 iterator over folders at first level.
 * Future verions of this class should be return the elements
 * in a incremental way when the #next() method is called.
 */
public class S3Iterator implements Iterator<Path> {
	private S3FileStore fileStore;
	private String key;
	private List<S3Path> items = Lists.newArrayList();
	private ObjectListing current;
	private int cursor; // index of next element to return
	private int size;
	private boolean recursive;

	public S3Iterator(S3Path path) {
		this(path, false);
	}

	public S3Iterator(S3Path path, boolean recursive) {
		this(path.getFileStore(), path.getKey().length() == 0 ? "" : (path.getKey() + (recursive ? "" : "/")), recursive);
	}
	
	public S3Iterator(S3FileStore fileStore, String key, boolean recursive) {
		this(fileStore, key, fileStore.buildRequest(key, recursive), recursive);
	}
	
	public S3Iterator(S3FileStore fileStore, String key, ListObjectsRequest listObjectsRequest, boolean recursive) {
		this.fileStore = fileStore;
		this.key = key;
		this.current = fileStore.listObjects(listObjectsRequest);
		this.recursive = recursive;
		loadObjects();
	}

	private void loadObjects() {
		if(recursive)
			this.fileStore.parseObjects(items, current);
		else
			this.fileStore.parseObjectListing(key, items, current);
		this.items = items.subList(this.size, items.size());
		this.size = items.size();
		this.cursor = 0;
	}

	@Override
	public boolean hasNext() {
		return cursor != size || current.isTruncated();
	}

	@Override
	public S3Path next() {
		if(cursor == size && current.isTruncated()) {
			this.current = fileStore.listNextBatchOfObjects(current);
			loadObjects();
		}
		if(cursor == size)
			throw new NoSuchElementException();
		return items.get(cursor++);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}