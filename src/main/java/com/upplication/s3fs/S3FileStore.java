package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Owner;

public class S3FileStore extends FileStore implements Comparable<S3FileStore> {
	private S3FileSystem fileSystem;
	private Bucket bucket;
	private String name;

	public S3FileStore(S3FileSystem s3FileSystem, Bucket bucket) {
		this.fileSystem = s3FileSystem;
		this.bucket = bucket;
		this.name = bucket.getName();
	}

	public S3FileStore(S3FileSystem s3FileSystem, String name) {
		this.fileSystem = s3FileSystem;
		this.name = name;
		this.bucket = null;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public String type() {
		return "S3Bucket";
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public long getTotalSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUsableSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUnallocatedSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsFileAttributeView(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
		if(type != S3FileStoreAttributeView.class)
			throw new FileStoreAttributeViewTypeNotSupportedException(type);
		Bucket buck = getBucket();
		Owner owner = buck.getOwner();
		return (V) new S3FileStoreAttributeView(buck.getCreationDate(), buck.getName(), owner.getId(), owner.getDisplayName());
	}

	@Override
	public Object getAttribute(String attribute) throws IOException {
		return getFileStoreAttributeView(S3FileStoreAttributeView.class).getAttribute(attribute);
	}
	
	private Bucket getBucket() {
		if(bucket == null) {
			bucket = fileSystem.getBucket(name);
		}
		return bucket;
	}

	@Override
	public int compareTo(S3FileStore o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		S3FileStore other = (S3FileStore) obj;
		if(other.fileSystem == null && fileSystem == null)
			return other.name().equals(name());
		if(other.fileSystem == null || fileSystem == null)
			return false;
		if(other.fileSystem.getEndpoint() == null && fileSystem.getEndpoint() == null)
			return other.name().equals(name());
		if(other.fileSystem.getEndpoint() == null || fileSystem.getEndpoint() == null)
			return false;
		if(!other.fileSystem.getEndpoint().equals(fileSystem.getEndpoint()))
			return false;
		return other.name().equals(name());
	}
}