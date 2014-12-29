package com.upplication.s3fs;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * S3 iterator over folders at first level.
 * Future verions of this class should be return the elements
 * in a incremental way when the #next() method is called.
 */
public class S3Iterator implements Iterator<Path> {
	private S3FileStore fileStore;
	private String key;
	private List<S3Path> items = Lists.newArrayList();
	private Set<S3Path> addedVirtualDirectories = Sets.newHashSet();
	private ObjectListing current;
	private int cursor; // index of next element to return
	private int size;
	private boolean incremental;

	public S3Iterator(S3Path path) {
		this(path, false);
	}

	public S3Iterator(S3Path path, boolean incremental) {
		this(path.getFileStore(), path.getKey().length() == 0 ? "" : (path.getKey() + (incremental ? "" : "/")), incremental);
	}
	
	public S3Iterator(S3FileStore fileStore, String key, boolean incremental) {
		this(fileStore, key, fileStore.buildRequest(key, incremental), incremental);
	}
	
	public S3Iterator(S3FileStore fileStore, String key, ListObjectsRequest listObjectsRequest, boolean incremental) {
		this.fileStore = fileStore;
		this.key = key;
		this.current = fileStore.listObjects(listObjectsRequest);
		this.incremental = incremental;
		loadObjects();
	}

	private void loadObjects() {
		this.items.clear();
		if (incremental)
			parseObjects();
		else
			this.fileStore.parseObjectListing(key, items, current);
		this.size = items.size();
		this.cursor = 0;
	}
	
	private void parseObjects() {
		S3FileSystem fileSystem = fileStore.getFileSystem();
		for (final S3ObjectSummary objectSummary : current.getObjectSummaries()) {
			final String objectSummaryKey = objectSummary.getKey();
			String[] keyParts = fileSystem.key2Parts(objectSummaryKey);
			addParentPaths(keyParts, objectSummary);
			S3Path path = new S3Path(fileSystem, fileStore, keyParts);
			path.setBasicFileAttributes(fileStore.buildFileS3Attributes(objectSummaryKey, objectSummary));
			if (!items.contains(path)) {
				items.add(path);
			}
		}
	}

	private void addParentPaths(String[] keyParts, S3ObjectSummary objectSummary) {
		if(keyParts.length <= 1)
			return;
		S3FileSystem fileSystem = fileStore.getFileSystem();
		String[] subParts = Arrays.copyOf(keyParts, keyParts.length-1);
		List<S3Path> parentPaths = new ArrayList<>();
		while (subParts.length > 0) {
			S3Path path = new S3Path(fileSystem, fileStore, subParts);
			String prefix = current.getPrefix();
			
			String parentKey = path.getKey();
			if(prefix.length() > parentKey.length() && prefix.contains(parentKey))
				break;
			if (items.contains(path) || addedVirtualDirectories.contains(path)) {
				subParts = Arrays.copyOf(subParts, subParts.length-1);
				continue;
			}
			path.setBasicFileAttributes(fileStore.buildFileS3Attributes(path.getKey()+"/", objectSummary));
			parentPaths.add(path);
			addedVirtualDirectories.add(path);
			subParts = Arrays.copyOf(subParts, subParts.length-1);
		}
		Collections.reverse(parentPaths);
		items.addAll(parentPaths);
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