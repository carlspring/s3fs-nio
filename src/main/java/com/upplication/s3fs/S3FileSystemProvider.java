package com.upplication.s3fs;

import static com.google.common.collect.Sets.difference;
import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.CONNECTION_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.MAX_CONNECTIONS;
import static com.upplication.s3fs.AmazonS3Factory.MAX_ERROR_RETRY;
import static com.upplication.s3fs.AmazonS3Factory.PROTOCOL;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_DOMAIN;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_HOST;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PASSWORD;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PORT;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_USERNAME;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_WORKSTATION;
import static com.upplication.s3fs.AmazonS3Factory.REQUEST_METRIC_COLLECTOR_CLASS;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_RECEIVE_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_SEND_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.USER_AGENT;
import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Spec:
 * 
 * URI: s3://[endpoint]/{bucket}/{key} If endpoint is missing, it's assumed to
 * be the default S3 endpoint (s3.amazonaws.com)
 * 
 * FileSystem roots: /{bucket}/
 * 
 * Treatment of S3 objects: - If a key ends in "/" it's considered a directory
 * *and* a regular file. Otherwise, it's just a regular file. - It is legal for
 * a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a
 * directory. - If a file "a/b/c" exists but there's no "a" or "a/b/", these are
 * considered "implicit" directories. They can be listed, traversed and deleted.
 * 
 * Deviations from FileSystem provider API: - Deleting a file or directory
 * always succeeds, regardless of whether the file/directory existed before the
 * operation was issued i.e. Files.delete() and Files.deleteIfExists() are
 * equivalent.
 * 
 * 
 * Future versions of this provider might allow for a strict mode that mimics
 * the semantics of the FileSystem provider API on a best effort basis, at an
 * increased processing cost.
 * 
 * 
 */
public class S3FileSystemProvider extends FileSystemProvider {
	public static final String AMAZON_S3_FACTORY_CLASS = "amazon_s3_factory";
	private static final ConcurrentMap<String, S3FileSystem> fileSystems = new ConcurrentHashMap<>();

	@Override
	public String getScheme() {
		return "s3";
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		validateUri(uri);
		// first try to load amazon props
		Properties props = getProperties(uri, env);
		validateProperties(props);
		String key = this.getFileSystemKey(uri, props);
		if(fileSystems.containsKey(key))
			throw new FileSystemAlreadyExistsException("File system " + uri.getScheme() + ':' + key + " already exists");
		S3FileSystem fileSystem = createFileSystem(uri, props);
		fileSystems.put(fileSystem.getKey(), fileSystem);
		return fileSystem;
	}

	private void validateProperties(Properties props) {
		Preconditions.checkArgument((props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
				|| (props.getProperty(ACCESS_KEY) != null && props.getProperty(SECRET_KEY) != null),
				"%s and %s should both be provided or should both be omitted",
				ACCESS_KEY, SECRET_KEY);
	}
	
	private Properties getProperties(URI uri, Map<String, ?> env) {
		Properties props = loadAmazonProperties();
		// but can be overloaded by envs vars
		overloadProperties(props, env);
		String userInfo = uri.getUserInfo();
		if(userInfo != null) {
			String[] keys = userInfo.split(":");
			props.setProperty(ACCESS_KEY, keys[0]);
			props.setProperty(SECRET_KEY, keys[1]);
		}
		return props;
	}

	private String getFileSystemKey(URI uri) {
		return getFileSystemKey(uri, getProperties(uri, null));
	}

	protected String getFileSystemKey(URI uri, Properties props) {
		String host = uri.getHost();
		String accessKey = (String) props.get(ACCESS_KEY);
		return accessKey + "@" + host;
	}
	
	protected void validateUri(URI uri) {
		Preconditions.checkNotNull(uri, "uri is null");
		Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "uri scheme must be 's3': '%s'", uri);
	}

	private void overloadProperties(Properties props, Map<String, ?> env) {
		if(env == null)
			env = new HashMap<>();
		for (String key : new String[] { ACCESS_KEY, SECRET_KEY, REQUEST_METRIC_COLLECTOR_CLASS, CONNECTION_TIMEOUT, MAX_CONNECTIONS, MAX_ERROR_RETRY, PROTOCOL, PROXY_DOMAIN, PROXY_HOST, PROXY_PASSWORD,
				PROXY_PORT, PROXY_USERNAME, PROXY_WORKSTATION, SOCKET_SEND_BUFFER_SIZE_HINT, SOCKET_RECEIVE_BUFFER_SIZE_HINT, SOCKET_TIMEOUT, USER_AGENT, AMAZON_S3_FACTORY_CLASS })
			overloadProperty(props, env, key);
	}

	private void overloadProperty(Properties props, Map<String, ?> env, String key) {
		if (env.get(key) != null && env.get(key) instanceof String)
			props.setProperty(key, (String) env.get(key));
		else if(System.getProperty(key) != null)
			props.setProperty(key, System.getProperty(key));
		else if(System.getenv(key) != null)
			props.setProperty(key, System.getenv(key));
	}

	public FileSystem getFileSystem(URI uri, Map<String, ?> env) {
		validateUri(uri);
		String key = this.getFileSystemKey(uri);
		if(!fileSystems.containsKey(key)) {
			try {
				newFileSystem(uri, env);
			} catch (IOException e) {
				throw new FileSystemNotFoundException(e.getMessage());
			}
		}
	    FileSystem fileSystem = fileSystems.get(key);
	    if (fileSystem == null) {
	      throw new FileSystemNotFoundException("File system " + uri.getScheme() + ':' + key + " does not exist");
	    }
	    return fileSystem;
	}

	@Override
	public S3FileSystem getFileSystem(URI uri) {
		validateUri(uri);
		String key = this.getFileSystemKey(uri);
		if(!fileSystems.containsKey(key)) {
			try {
				newFileSystem(uri, null);
			} catch (IOException e) {
				throw new FileSystemNotFoundException(e.getMessage());
			}
		}
		S3FileSystem fileSystem = fileSystems.get(key);
	    if (fileSystem == null) {
	      throw new FileSystemNotFoundException("File system " + uri.getScheme() + ':' + key + " does not exist");
	    }
	    return fileSystem;
	}

	private S3Path toS3Path(Path path) {
		Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
		return (S3Path) path;
	}

	/**
	 * Deviation from spec: throws FileSystemNotFoundException if FileSystem
	 * hasn't yet been initialized. Call newFileSystem() first.
	 * Need credentials. Maybe set credentials after? how?
	 */
	@Override
	public Path getPath(URI uri) {
	    FileSystem fileSystem = getFileSystem(uri);
		/**
		 * TODO: set as a list. one s3FileSystem by region
		 */
		return fileSystem.getPath(uri.getPath());
	}

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        final S3Path s3Path = toS3Path(dir);
        return new DirectoryStream<Path>() {
            @Override
            public void close() throws IOException {
                // nothing to do here
            }
            @Override
            public Iterator<Path> iterator() {
                return new S3Iterator(s3Path);
            }
        };
    }

	@Override
	public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
		return toS3Path(path).getInputStream(options);
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		return toS3Path(path).newByteChannel(options, attrs);
	}

	/**
	 * Deviations from spec: Does not perform atomic check-and-create. Since a
	 * directory is just an S3 object, all directories in the hierarchy are
	 * created or it already existed.
	 */
	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		S3Path s3Path = toS3Path(dir);
		Preconditions.checkArgument(attrs.length == 0, "attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO
		s3Path.createDirectory(attrs);
	}

	@Override
	public void delete(Path path) throws IOException {
		toS3Path(path).delete();
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		if (isSameFile(source, target))
			return;

		S3Path s3Source = toS3Path(source);
		S3Path s3Target = toS3Path(target);
		/*
		 * Preconditions.checkArgument(!s3Source.isDirectory(),
		 * "copying directories is not yet supported: %s", source); // TODO
		 * Preconditions.checkArgument(!s3Target.isDirectory(),
		 * "copying directories is not yet supported: %s", target); // TODO
		 */
		ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
		verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING), actualOptions);

		if (exists(s3Target) && !actualOptions.contains(StandardCopyOption.REPLACE_EXISTING))
			throw new FileAlreadyExistsException(format("target already exists: %s", target));
		s3Source.copyTo(s3Target, options);
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameFile(Path path1, Path path2) throws IOException {
		return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		return false;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		S3Path s3Path = toS3Path(path);
		Preconditions.checkArgument(s3Path.isAbsolute(), "path must be absolute: %s", s3Path);
		s3Path.checkAccess(modes);
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
		return toS3Path(path).readAttributes(type, options);
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	// ~~
	/**
	 * Create the fileSystem
	 * @param uri URI
	 * @param accessKey Object maybe null for anonymous authentication
	 * @param secretKey Object maybe null for anonymous authentication
	 * @return S3FileSystem never null
	 */
	protected S3FileSystem createFileSystem(URI uri, Properties props) {
		return new S3FileSystem(this, getFileSystemKey(uri, props), getAmazonS3(uri, props), uri.getHost());
	}

	protected AmazonS3 getAmazonS3(URI uri, Properties props) {
		return getAmazonS3Factory(props).getAmazonS3(uri, props);
	}

	protected AmazonS3Factory getAmazonS3Factory(Properties props) {
		if(props.containsKey(AMAZON_S3_FACTORY_CLASS)) {
			String amazonS3FactoryClass = props.getProperty(AMAZON_S3_FACTORY_CLASS);
			try {
				return (AmazonS3Factory) Class.forName(amazonS3FactoryClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
				throw new S3FileSystemConfigurationException("Configuration problem, couldn't instantiate AmazonS3Factory ("+amazonS3FactoryClass+"): ", e);
			}
		}
		return new AmazonS3ClientFactory();
	}

	/**
	 * find /amazon.properties in the classpath
	 * @return Properties amazon.properties
	 */
	protected Properties loadAmazonProperties() {
		Properties props = new Properties();
		// http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html
		// http://www.javaworld.com/javaqa/2003-08/01-qa-0808-property.html
		try(InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon.properties")){
			if (in != null)
				props.load(in);
		} catch (IOException e) {
			// If amazon.properties can't be loaded that's ok. 
		}
		return props;
	}
	
	// ~~~

	private <T> void verifySupportedOptions(Set<? extends T> allowedOptions, Set<? extends T> actualOptions) {
		Sets.SetView<? extends T> unsupported = difference(actualOptions, allowedOptions);
		Preconditions.checkArgument(unsupported.isEmpty(), "the following options are not supported: %s", unsupported);
	}
	
	/**
	 * check that the paths exists or not
	 * @param path S3Path
	 * @return true if exists
	 */
	private boolean exists(S3Path path) {
		return path.exists();
	}

    /**
     * create a temporal directory to create streams
     * @return Path temporal folder
     * @throws IOException
     */
	public Path createTempDir() throws IOException {
		return Files.createTempDirectory("temp-s3-");
	}

	public void close(S3FileSystem fileSystem) {
		fileSystems.remove(fileSystem.getKey());
	}

	public boolean isOpen(S3FileSystem s3FileSystem) {
		return fileSystems.containsKey(s3FileSystem.getKey());
	}
	
	public static ConcurrentMap<String, S3FileSystem> getFilesystems() {
		return fileSystems;
	}
}