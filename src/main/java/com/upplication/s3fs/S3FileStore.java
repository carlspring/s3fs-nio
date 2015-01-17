package com.upplication.s3fs;

import static java.lang.String.format;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitor;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
		this.bucket = getBucket(name);
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
		return false;
	}

	@Override
	public boolean supportsFileAttributeView(String attributeViewName) {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
		if (type != S3FileStoreAttributeView.class)
			throw new IllegalArgumentException("FileStoreAttributeView of type '" + type.getName() + "' is not supported.");
		Bucket buck = getBucket(true);
		Owner owner = buck.getOwner();
		return (V) new S3FileStoreAttributeView(buck.getCreationDate(), buck.getName(), owner.getId(), owner.getDisplayName());
	}

	@Override
	public Object getAttribute(String attribute) throws IOException {
		return getFileStoreAttributeView(S3FileStoreAttributeView.class).getAttribute(attribute);
	}
	
	public S3FileSystem getFileSystem() {
		return fileSystem;
	}

	private Bucket getBucket(boolean force) {
		if (bucket == null)
			bucket = getBucket(name);
		if (bucket == null && force)
			bucket = createBucket();
		return bucket;
	}

    public Bucket getBucket(){
        return getBucket(name);
    }

	private Bucket getBucket(String bucketName) {
		for (Bucket buck : getClient().listBuckets())
			if (buck.getName().equals(bucketName))
				return buck;
		return null;
	}

	private Bucket createBucket() {
		return getClient().createBucket(name);
	}

	public S3Path getRootDirectory() {
		return new S3Path(fileSystem, this, ImmutableList.<String> of());
	}

	public S3AccessControlList getAccessControlList(S3Path path) throws NoSuchFileException {
		String key = getS3ObjectSummary(path).getKey();
		return new S3AccessControlList(name, key, getClient().getObjectAcl(name, key), getOwner());
	}

	private AmazonS3 getClient() {
		return fileSystem.getClient();
	}

	public Owner getOwner() {
		Bucket buck = getBucket(false);
		if (buck != null)
			return buck.getOwner();
		return fileSystem.getClient().getS3AccountOwner();
	}


	ObjectListing listObjects(ListObjectsRequest request) {
		return getClient().listObjects(request);
	}

	/**
	 * Get the {@link com.amazonaws.services.s3.model.S3ObjectSummary} that represent this Path or her first child if this path not exists
	 * @param s3Path {@link com.upplication.s3fs.S3Path}
	 * @return {@link com.amazonaws.services.s3.model.S3ObjectSummary}
	 * @throws java.nio.file.NoSuchFileException if not found the path and any child
	 */
	public S3ObjectSummary getS3ObjectSummary(S3Path s3Path) throws NoSuchFileException {
		String key = s3Path.getKey();
        AmazonS3 client = getClient();
		try {
			ObjectMetadata metadata = client.getObjectMetadata(name, key);
			S3ObjectSummary result = new S3ObjectSummary();
			result.setBucketName(name);
			result.setETag(metadata.getETag());
			result.setKey(key);
			result.setLastModified(metadata.getLastModified());
			result.setSize(metadata.getContentLength());
			AccessControlList objectAcl = client.getObjectAcl(name, key);
			result.setOwner(objectAcl.getOwner());
			return result;
		} catch (AmazonS3Exception e) {
			if (e.getStatusCode() != 404)
				throw e;
		}

		try {
			// is a virtual directory
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(name);
            request.setPrefix(key + "/");
            request.setMaxKeys(1);
            ObjectListing current = client.listObjects(request);
			if (!current.getObjectSummaries().isEmpty())
				return current.getObjectSummaries().get(0);
		} catch (Exception e) {
			//
		}
		throw new NoSuchFileException(name + S3Path.PATH_SEPARATOR + key);
	}

	public boolean exists(S3Path s3Path) {
		try {
			getS3ObjectSummary(s3Path);
			return true;
		} catch (NoSuchFileException e) {
			return false;
		}
	}

	/**
	 * @param options  
	 */
	public <A extends BasicFileAttributes> A readAttributes(S3Path path, Class<A> type, LinkOption... options) throws IOException {
		if (type == BasicFileAttributes.class) {
			String key = path.getKey();
			S3ObjectSummary objectSummary = getS3ObjectSummary(path);
			return type.cast(buildFileS3Attributes(key, objectSummary));
		}
		throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
	}

	protected S3FileAttributes buildFileS3Attributes(String key, S3ObjectSummary objectSummary) {
		// parse the data to BasicFileAttributes.
		FileTime lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
		long size = objectSummary.getSize();
		boolean directory = false;
		boolean regularFile = false;
		String resolvedKey = objectSummary.getKey();
		// check if is a directory and exists the key of this directory at amazon s3
		if (key.endsWith("/") && resolvedKey.equals(key) || resolvedKey.equals(key + "/")) {
			directory = true;
		} else if (!resolvedKey.equals(key) && resolvedKey.startsWith(key)) { // is a directory but not exists at amazon s3
			directory = true;
			// no metadata, we fake one
			size = 0;
			// delete extra part
			resolvedKey = key + "/";
		} else
			regularFile = true;
		return new S3FileAttributes(resolvedKey, lastModifiedTime, size, directory, regularFile);
	}

    /**
	 * @param options  
	 */
	public InputStream getInputStream(S3Path path, OpenOption... options) throws IOException {
		checkSupported(options);
		Preconditions.checkArgument(!path.getKey().equals(""), "cannot create InputStream for root directory: %s", path);
		S3Object object = getObject(path.getKey());
		InputStream res = object.getObjectContent();
		if (res == null)
			throw new IOException("path is a directory");
		return res;
	}

	private void checkSupported(OpenOption... options) {
		Preconditions.checkArgument(options.length == 0, "OpenOptions not yet supported: %s", ImmutableList.copyOf(options)); // TODO
	}

	S3Object getObject(String key) {
		return getClient().getObject(name, key);
	}

	void putObject(String key, InputStream input, ObjectMetadata metadata) {
		Bucket buck = getBucket(true);
		getClient().putObject(buck.getName(), key, input, metadata);
	}

	/**
	 * The current #buildRequest() get all subdirectories and her content.
	 * This method filter the keyChild and check if is a inmediate
	 * descendant of the keyParent parameter
	 * @param keyParent String
	 * @param keyChild String
	 * @return String parsed
	 *  or null when the keyChild and keyParent are the same and not have to be returned
	 */
	private String getImmediateDescendant(String keyParent, String keyChild) {
		keyParent = deleteExtraPath(keyParent);
		keyChild = deleteExtraPath(keyChild);
		final int parentLen = keyParent.length();
		final String childWithoutParent = deleteExtraPath(keyChild.substring(parentLen));
		String[] parts = childWithoutParent.split("/");
		if (parts.length > 0 && !parts[0].isEmpty())
			return keyParent + "/" + parts[0];
		return null;

	}

	private String deleteExtraPath(String keyChild) {
		if (keyChild.startsWith("/"))
			keyChild = keyChild.substring(1);
		if (keyChild.endsWith("/"))
			keyChild = keyChild.substring(0, keyChild.length() - 1);
		return keyChild;
	}
	
	ListObjectsRequest buildRequest(String key, boolean incremental) {
		return buildRequest(key, incremental, null);
	}

	ListObjectsRequest buildRequest(String key, boolean incremental, Integer maxKeys) {
		if(incremental)
			return new ListObjectsRequest(name(), key, null, null, maxKeys);
		return new ListObjectsRequest(name(), key, key, "/", maxKeys);
	}

	/**
	 * add to the listPath the elements at the same level that s3Path
	 * @param key the uri to parse
	 * @param listPath List not null list to add
	 * @param current ObjectListing to walk
	 */
	void parseObjectListing(String key, List<S3Path> listPath, ObjectListing current) {
		for (String commonPrefix : current.getCommonPrefixes()){
            if (!commonPrefix.equals("/")){
                listPath.add(new S3Path(fileSystem, this, fileSystem.key2Parts(commonPrefix)));
            }
		}
		// TODO: figure our a way to efficiently preprocess commonPrefix basicFileAttributes
		for (final S3ObjectSummary objectSummary : current.getObjectSummaries()) {
			final String objectSummaryKey = objectSummary.getKey();
			// we only want the first level
			String immediateDescendantKey = getImmediateDescendant(key, objectSummaryKey);
			if (immediateDescendantKey != null) {
				S3Path descendentPart = new S3Path(fileSystem, this, fileSystem.key2Parts(immediateDescendantKey));
				if (immediateDescendantKey.equals(objectSummaryKey))
					descendentPart.setBasicFileAttributes(buildFileS3Attributes(objectSummaryKey, objectSummary));
				if (!listPath.contains(descendentPart)) {
					listPath.add(descendentPart);
				}
			}
		}
	}
	
	public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) {
		return getClient().listNextBatchOfObjects(previousObjectListing);
	}

	public void walkFileTree(S3Path start, FileVisitor<? super Path> visitor) throws IOException {
		walkFileTree(start, visitor, Integer.MAX_VALUE);
	}

	public void walkFileTree(S3Path start, FileVisitor<? super Path> visitor, int maxDepth) throws IOException {
		new S3Walker(visitor, maxDepth).walk(start);
	}

	@Override
	public int compareTo(S3FileStore o) {
		if (this == o)
			return 0;
		return o.name().compareTo(name);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bucket == null) ? 0 : hashCode(bucket));
		result = prime * result + ((fileSystem == null) ? 0 : fileSystem.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	private int hashCode(Bucket buck) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((buck.getName() == null) ? 0 : buck.getName().hashCode());
		result = prime * result + ((buck.getOwner() == null) ? 0 : buck.getOwner().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof S3FileStore))
			return false;
		S3FileStore other = (S3FileStore) obj;

		if (fileSystem == null) {
			if (other.fileSystem != null)
				return false;
		} else if (!fileSystem.equals(other.fileSystem))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}