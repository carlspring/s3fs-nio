package com.upplication.s3fs;

import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.CopyOption;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.upplication.s3fs.util.FileTypeDetector;
import com.upplication.s3fs.util.IOUtils;

public class S3FileStore extends FileStore implements Comparable<S3FileStore> {
    private final FileTypeDetector fileTypeDetector = new com.upplication.s3fs.util.FileTypeDetector();
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
		if(getClient().doesBucketExist(name)) {
			this.bucket = getBucket(name);
		} else {
			this.bucket = null;
		}
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
		if(bucket == null)
			bucket = fileSystem.getBucket(name);
		return bucket;
	}

	private Bucket getBucket(String bucketName) {
		for (Bucket buck : getClient().listBuckets())
			if(buck.getName().equals(bucketName))
				return buck;
		return null;
	}

	public S3Path getRootDirectory() {
		return new S3Path(fileSystem, this, ImmutableList.<String> of());
	}

	public void delete(S3Path path) {
		String key = path.getKey();
		// we delete the two objects (sometimes exists the key '/' and sometimes not)
		getClient().deleteObject(name, key);
		getClient().deleteObject(name, key + "/");
	}

	/**
	 * @param options  
	 */
	public void copy(String key, S3Path target, CopyOption[] options) {
		getClient().copyObject(name, key, target.getFileStore().name(), target.getKey());
	}

	/**
	 * @param attrs  
	 */
	public void createDirectory(String key, FileAttribute<?>[] attrs) {
		if(bucket == null) {
			// check if bucket exists.
			if(getClient().doesBucketExist(name))
				bucket = getBucket(name);
			// ifnot try to create it.
			if(bucket == null)
				bucket = getClient().createBucket(name);
		}
		// FIXME: throw exception if the same key already exists at amazon s3
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		StringBuilder keyName = new StringBuilder(key);
		if(!key.endsWith("/"))
			keyName.append("/");
		getClient().putObject(name, keyName.toString(), new ByteArrayInputStream(new byte[0]), metadata);
	}

	public S3AccessControlList getAccessControlList(S3Path path) throws NoSuchFileException {
		String key = getS3ObjectSummary(path).getKey();
        return new S3AccessControlList(name, key, getClient().getObjectAcl(name, key), getOwner());
	}

	private AmazonS3 getClient() {
		return fileSystem.getClient();
	}

	public Owner getOwner() {
		Bucket buck = getBucket();
		if(buck != null)
			return buck.getOwner();
		return fileSystem.getClient().getS3AccountOwner();
	}

    /**
     * @param key String key
     * @return ObjectMetadata
     */
	private ObjectMetadata getObjectMetadata(String key) {
        try {
        	return getClient().getObjectMetadata(name, key);
        } catch (AmazonS3Exception e){
            if (e.getStatusCode() == 404)
            	return null;
			throw e;
        }
	}

	public ObjectListing listObjects(String key) {
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(name);
		request.setPrefix(key);
		request.setMaxKeys(1);
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
		ObjectMetadata metadata = getObjectMetadata(key);
		if (metadata != null) {
            S3ObjectSummary result = new S3ObjectSummary();
            result.setBucketName(name);
            result.setETag(metadata.getETag());
            result.setKey(key);
            result.setLastModified(metadata.getLastModified());
            result.setSize(metadata.getContentLength());
            return result;
        }
		
		// is a virtual directory
        ObjectListing current = listObjects(key + "/");
        if (!current.getObjectSummaries().isEmpty())
			return current.getObjectSummaries().get(0);
        throw new NoSuchFileException(name+S3Path.PATH_SEPARATOR+key);
	}

	public boolean exists(S3Path s3Path) {
		try {
			getS3ObjectSummary(s3Path);
			return true;
		}
        catch(NoSuchFileException e) {
			return false;
		}
	}
	
	/**
	 * @param options  
	 */
	public <A extends BasicFileAttributes> A readAttributes(S3Path path, Class<A> type, LinkOption... options) throws IOException {
		if (type == BasicFileAttributes.class) {
			S3ObjectSummary objectSummary = getS3ObjectSummary(path);
			// parse the data to BasicFileAttributes.
			FileTime lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
			long size =  objectSummary.getSize();
			boolean directory = false;
			boolean regularFile = false;
			String resolvedKey = objectSummary.getKey();
			String key = path.getKey();
            // check if is a directory and exists the key of this directory at amazon s3
			if (objectSummary.getKey().equals(key + "/") && objectSummary.getKey().endsWith("/")) {
				directory = true;
			}
			// is a directory but not exists at amazon s3
			else if (!objectSummary.getKey().equals(key) && objectSummary.getKey().startsWith(key)){
				directory = true;
				// no metadata, we fake one
				size = 0;
                // delete extra part
                resolvedKey = key + "/";
			} else
				regularFile = true;
			return type.cast(new S3FileAttributes(resolvedKey, lastModifiedTime, size, directory, regularFile));
		}
		throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
	}

	/**
	 * @param attrs  
	 */
	public SeekableByteChannel newByteChannel(final S3Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		String key = path.getKey();
		// we resolve to a file inside the temp folder with the s3path name
        final Path tempFile = fileSystem.createTempDir().resolve(key.replaceAll("/", "_"));
        try (S3Object object = getClient().getObject(name, key)) {

        	InputStream is = getClient().getObject(name, key).getObjectContent();
            Files.write(tempFile, IOUtils.toByteArray(is));
        } catch (AmazonServiceException e) {
			// key doesn't exist on server. That's ok.
		}
        // and we can use the File SeekableByteChannel implementation
		final SeekableByteChannel seekable = Files.newByteChannel(tempFile, options);

		return new SeekableByteChannel() {
			@Override
			public boolean isOpen() {
				return seekable.isOpen();
			}

			@Override
			public void close() throws IOException {
				try {
	                if (!seekable.isOpen())
						return;
					seekable.close();
					// upload the content where the seekable ends (close)
	                if (Files.exists(tempFile)) {
	                    ObjectMetadata metadata = new ObjectMetadata();
	                    metadata.setContentLength(Files.size(tempFile));
	                    // FIXME: #20 ServiceLoader cant load com.upplication.s3fs.util.FileTypeDetector when this library is used inside a ear :(
	                    metadata.setContentType(fileTypeDetector.probeContentType(tempFile));
	
	                    try (InputStream stream = Files.newInputStream(tempFile)) {
	                        /*
	                         FIXME: if the stream is {@link InputStream#markSupported()} i can reuse the same stream
	                         and evict the close and open methods of probeContentType. By this way:
	                         metadata.setContentType(new Tika().detect(stream, tempFile.getFileName().toString()));
	                        */
	                        getClient().putObject(name, path.getKey(), stream, metadata);
	                    }
	                } else { // delete: check option delete_on_close
	                	delete(path);
	                }
				} finally {
					try {
						// and delete the temp dir
		                Files.deleteIfExists(tempFile);
		                Files.deleteIfExists(tempFile.getParent());
					} catch (Throwable t) {
						// is ok.
					}
				}
			}

			@Override
			public int write(ByteBuffer src) throws IOException {
				return seekable.write(src);
			}

			@Override
			public SeekableByteChannel truncate(long size) throws IOException {
				return seekable.truncate(size);
			}

			@Override
			public long size() throws IOException {
				return seekable.size();
			}

			@Override
			public int read(ByteBuffer dst) throws IOException {
				return seekable.read(dst);
			}

			@Override
			public SeekableByteChannel position(long newPosition) throws IOException {
				return seekable.position(newPosition);
			}

			@Override
			public long position() throws IOException {
				return seekable.position();
			}
		};
	}

	@Override
	public int compareTo(S3FileStore o) {
		if(this == o)
			return 0;
		// TODO: actually compare this S3FileStore with the o(ther).
		return 0;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
		result = prime * result + ((fileSystem == null) ? 0 : fileSystem.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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