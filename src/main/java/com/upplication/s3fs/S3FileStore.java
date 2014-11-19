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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
		if(bucket == null)
			bucket = fileSystem.getBucket(name);
		return bucket;
	}

	@Override
	public int compareTo(S3FileStore o) {
		if(this == o)
			return 0;
		// TODO: actually compare this S3FileStore with the o(ther).
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

	public void delete(String key) {
		// we delete the two objects (sometimes exists the key '/' and sometimes not)
		getClient().deleteObject(name, key);
		getClient().deleteObject(name, key + "/");
	}

	public void copy(String key, S3Path target, CopyOption[] options) {
		getClient().copyObject(name, key, target.getFileStore().name(), target.getKey());
	}

	public void createDirectory(String key, FileAttribute<?>[] attrs) {
		// FIXME: throw exception if the same key already exists at amazon s3
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		StringBuilder keyName = new StringBuilder(key);
		if(!key.endsWith("/"))
			keyName.append("/");
		getClient().putObject(name, keyName.toString(), new ByteArrayInputStream(new byte[0]), metadata);
	}

	public S3AccessControlList getAccessControlList(String key) throws NoSuchFileException {
		key = getS3ObjectSummary(key).getKey();
        return new S3AccessControlList(name, key, getClient().getObjectAcl(name, key), getOwner());
	}

	private AmazonS3Client getClient() {
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
	public S3ObjectSummary getS3ObjectSummary(String key) throws NoSuchFileException {
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

	public boolean exists(String key) {
		try {
			getS3ObjectSummary(key);
			return true;
		}
        catch(NoSuchFileException e) {
			return false;
		}
	}
	
	public <A extends BasicFileAttributes> A readAttributes(String key, Class<A> type, LinkOption... options) throws IOException {
		if (type == BasicFileAttributes.class) {
			S3ObjectSummary objectSummary = getS3ObjectSummary(key);
			// parse the data to BasicFileAttributes.
			FileTime lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
			long size =  objectSummary.getSize();
			boolean directory = false;
			boolean regularFile = false;
			String resolvedKey = objectSummary.getKey();
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

	public SeekableByteChannel newByteChannel(final String key, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		// we resolve to a file inside the temp folder with the s3path name
        final Path tempFile = fileSystem.createTempDir().resolve(key.replaceAll("/", "_"));
        
        if (exists(key)){
        	InputStream is = getClient().getObject(name, key).getObjectContent();
            Files.write(tempFile, IOUtils.toByteArray(is));
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
	                        getClient().putObject(name, key, stream, metadata);
	                    }
	                } else { // delete: check option delete_on_close
	                	delete(key);
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
}