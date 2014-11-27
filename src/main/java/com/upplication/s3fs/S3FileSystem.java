package com.upplication.s3fs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class S3FileSystem extends FileSystem implements Comparable<S3FileSystem> {
	private final S3FileSystemProvider provider;
	private final String key;
	private final AmazonS3 client;
	private final String endpoint;

	public S3FileSystem(S3FileSystemProvider provider, String key, AmazonS3 client, String endpoint) {
		this.provider = provider;
		this.key = key;
		this.client = client;
		this.endpoint = endpoint;
	}

	@Override
	public FileSystemProvider provider() {
		return provider;
	}

	public String getKey() {
		return key;
	}

	@Override
	public void close() throws IOException {
		this.provider.close(this);
	}

	@Override
	public boolean isOpen() {
		return this.provider.isOpen(this);
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public String getSeparator() {
		return S3Path.PATH_SEPARATOR;
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		ImmutableList.Builder<Path> builder = ImmutableList.builder();
		for (FileStore fileStore : getFileStores()) {
			builder.add(((S3FileStore) fileStore).getRootDirectory());
		}
		return builder.build();
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		ImmutableList.Builder<FileStore> builder = ImmutableList.builder();
		for (Bucket bucket : client.listBuckets()) {
			builder.add(new S3FileStore(this, bucket));
		}
		return builder.build();
	}

	public S3FileStore getFileStore(String bucket) {
		return new S3FileStore(this, bucket);
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		return ImmutableSet.of("basic");
	}

	@Override
	public S3Path getPath(String first, String... more) {
		if (more.length == 0) {
			return new S3Path(this, first);
		}

		return new S3Path(this, first, more);
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchService newWatchService() throws IOException {
		throw new UnsupportedOperationException();
	}

	public AmazonS3 getClient() {
		return client;
	}

	/**
	 * get the endpoint associated with this fileSystem.
	 * 
	 * @see <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html">http://docs.aws.amazon.com/general/latest/gr/rande.html</a>
	 * @return string
	 */
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof S3FileSystem))
			return false;
		S3FileSystem other = (S3FileSystem) obj;
		if (endpoint == null) {
			if (other.endpoint != null)
				return false;
		} else if (!endpoint.equals(other.endpoint))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public int compareTo(S3FileSystem o) {
		return key.compareTo(o.getKey());
	}
}