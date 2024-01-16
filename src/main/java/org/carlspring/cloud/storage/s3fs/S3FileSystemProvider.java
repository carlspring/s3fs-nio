package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributeView;
import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.attribute.S3PosixFileAttributeView;
import org.carlspring.cloud.storage.s3fs.attribute.S3PosixFileAttributes;
import org.carlspring.cloud.storage.s3fs.util.AttributesUtils;
import org.carlspring.cloud.storage.s3fs.util.Cache;
import org.carlspring.cloud.storage.s3fs.util.Constants;
import org.carlspring.cloud.storage.s3fs.util.S3Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessMode;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.StringUtils;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.CONNECTION_TIMEOUT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.MAX_CONNECTIONS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.MAX_ERROR_RETRY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PATH_STYLE_ACCESS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROTOCOL;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_DOMAIN;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_HOST;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_PASSWORD;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_PORT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_USERNAME;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_WORKSTATION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REQUEST_METRIC_COLLECTOR_CLASS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SIGNER_OVERRIDE;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SOCKET_RECEIVE_BUFFER_SIZE_HINT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SOCKET_SEND_BUFFER_SIZE_HINT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SOCKET_TIMEOUT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.USER_AGENT;
import static software.amazon.awssdk.http.Header.CONTENT_TYPE;
import static software.amazon.awssdk.http.HttpStatusCode.NOT_FOUND;

/**
 * Spec:
 * <p>
 * URI: s3://[endpoint]/{bucket}/{key} If endpoint is missing, it's assumed to
 * be the default S3 endpoint (s3.amazonaws.com)
 * </p>
 * <p>
 * FileSystem roots: /{bucket}/
 * </p>
 * <p>
 * Treatment of S3 objects: - If a key ends in "/" it's considered a directory
 * *and* a regular file. Otherwise, it's just a regular file. - It is legal for
 * a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a
 * directory. - If a file "a/b/c" exists but there's no "a" or "a/b/", these are
 * considered "implicit" directories. They can be listed, traversed and deleted.
 * </p>
 * <p>
 * Deviations from FileSystem provider API: - Deleting a file or directory
 * always succeeds, regardless of whether the file/directory existed before the
 * operation was issued i.e. Files.delete() and Files.deleteIfExists() are
 * equivalent.
 * </p>
 * <p>
 * Future versions of this provider might allow for a strict mode that mimics
 * the semantics of the FileSystem provider API on a best effort basis, at an
 * increased processing cost.
 * </p>
 */
public class S3FileSystemProvider
        extends FileSystemProvider
{


    public static final String CHARSET_KEY = "s3fs.charset";

    public static final String S3_FACTORY_CLASS = "s3fs.amazon.s3.factory.class";

    private volatile static ConcurrentMap<String, S3FileSystem> fileSystems = new ConcurrentHashMap<>();

    private static final List<String> PROPS_TO_OVERLOAD = Arrays.asList(ACCESS_KEY,
                                                                        SECRET_KEY,
                                                                        REQUEST_METRIC_COLLECTOR_CLASS,
                                                                        CONNECTION_TIMEOUT,
                                                                        MAX_CONNECTIONS,
                                                                        MAX_ERROR_RETRY,
                                                                        PROTOCOL,
                                                                        PROXY_DOMAIN,
                                                                        PROXY_HOST,
                                                                        PROXY_PASSWORD,
                                                                        PROXY_PORT,
                                                                        PROXY_USERNAME,
                                                                        PROXY_WORKSTATION,
                                                                        REGION,
                                                                        SOCKET_SEND_BUFFER_SIZE_HINT,
                                                                        SOCKET_RECEIVE_BUFFER_SIZE_HINT,
                                                                        SOCKET_TIMEOUT,
                                                                        USER_AGENT,
                                                                        S3_FACTORY_CLASS,
                                                                        SIGNER_OVERRIDE,
                                                                        PATH_STYLE_ACCESS);

    private static final int MAX_OBJECT_PER_REQUEST = 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileSystemProvider.class);

    private final S3Utils s3Utils = new S3Utils();

    private Cache cache = new Cache();

    private final Object mutex = new Object();

    @Override
    public String getScheme()
    {
        return "s3";
    }

    @Override
    public FileSystem newFileSystem(URI uri,
                                    Map<String, ?> env)
    {
        synchronized (mutex)
        {
            validateUri(uri);

            // get properties for the env or properties or system
            Properties props = getProperties(uri, env);

            validateProperties(props);

            // try to get the filesystem by the key
            String key = getFileSystemKey(uri, props);
            if (fileSystems.containsKey(key))
            {
                String safeName = uri.getScheme() + "://";
                String userInfo = uri.getUserInfo();
                if (userInfo != null)
                {
                    safeName += uri.getUserInfo().split(":")[0] + ":__REDACTED__@";
                }
                safeName += uri.getHost() + (uri.getPort() > -1 ? ":" + uri.getPort() : "") + uri.getPath();
                throw new FileSystemAlreadyExistsException("File system " + safeName + " already exists");
            }

            // create the filesystem with the final properties, store and return
            S3FileSystem fileSystem = createFileSystem(uri, props);

            fileSystems.put(fileSystem.getKey(), fileSystem);

            return fileSystem;
        }
    }

    private void validateProperties(Properties props)
    {
        Preconditions.checkArgument((props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null) ||
                                    (props.getProperty(ACCESS_KEY) != null && props.getProperty(SECRET_KEY) != null),
                                    "%s and %s should both be provided or should both be omitted",
                                    ACCESS_KEY,
                                    SECRET_KEY);
    }

    private Properties getProperties(URI uri,
                                     Map<String, ?> env)
    {
        Properties props = loadAmazonProperties();

        addEnvProperties(props, env);

        // and access key and secret key can be override
        String userInfo = uri.getUserInfo();
        if (userInfo != null)
        {
            String[] keys = userInfo.split(":");

            props.setProperty(ACCESS_KEY, keys[0]);

            if (keys.length > 1)
            {
                props.setProperty(SECRET_KEY, keys[1]);
            }
        }

        return props;
    }

    private String getFileSystemKey(URI uri)
    {
        return getFileSystemKey(uri, getProperties(uri, null));
    }

    /**
     * get the file system key represented by: the access key @ endpoint.
     * Example: access-key@s3.amazonaws.com
     * If uri host is empty then s3.amazonaws.com are used as host
     *
     * @param uri   URI with the endpoint
     * @param props with the access key property
     * @return String
     */
    protected String getFileSystemKey(URI uri,
                                      Properties props)
    {
        // we don`t use uri.getUserInfo and uri.getHost because secret key and access key have special chars
        // and dont return the correct strings
        String uriString = uri.toString().replaceAll("s3://", "");
        String authority = null;

        int authoritySeparator = uriString.indexOf("@");

        if (authoritySeparator > 0)
        {
            authority = uriString.substring(0, authoritySeparator);
        }

        if (authority != null)
        {
            String host = uriString.substring(uriString.indexOf("@") + 1);

            int lastPath = host.indexOf("/");
            if (lastPath > -1)
            {
                host = host.substring(0, lastPath);
            }

            if (host.length() == 0)
            {
                host = Constants.S3_HOSTNAME;
            }

            return authority + "@" + host;
        }
        else
        {
            String accessKey = (String) props.get(ACCESS_KEY);

            return (accessKey != null ? accessKey + "@" : "") +
                   (uri.getHost() != null ? uri.getHost() : Constants.S3_HOSTNAME);
        }
    }

    protected void validateUri(URI uri)
    {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "uri scheme must be 's3': '%s'", uri);
    }

    protected void addEnvProperties(Properties props,
                                    Map<String, ?> env)
    {
        if (env == null)
        {
            env = new HashMap<>();
        }

        for (String key : PROPS_TO_OVERLOAD)
        {
            // but can be overloaded by envs vars
            overloadProperty(props, env, key);
        }

        for (Map.Entry<String, ?> entry : env.entrySet())
        {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            if (!PROPS_TO_OVERLOAD.contains(key))
            {
                props.put(key, value);
            }
        }
    }

    /**
     * try to override the properties props with:
     * <ol>
     * <li>the map or if not setted:</li>
     * <li>the system property or if not setted:</li>
     * <li>the system vars</li>
     * </ol>
     *
     * @param props Properties to override
     * @param env   Map the first option
     * @param key   String the key
     */
    private void overloadProperty(Properties props,
                                  Map<String, ?> env,
                                  String key)
    {
        boolean overloaded = overloadPropertiesWithEnv(props, env, key);

        if (!overloaded)
        {
            overloaded = overloadPropertiesWithSystemProps(props, key);
        }

        if (!overloaded)
        {
            overloadPropertiesWithSystemEnv(props, key);
        }
    }

    /**
     * @return true if the key are overloaded by the map parameter
     */
    protected boolean overloadPropertiesWithEnv(Properties props,
                                                Map<String, ?> env,
                                                String key)
    {
        if (env.get(key) instanceof String)
        {
            props.setProperty(key, (String) env.get(key));

            return true;
        }

        return false;
    }

    /**
     * @return true if the key are overloaded by a system property
     */
    public boolean overloadPropertiesWithSystemProps(Properties props,
                                                     String key)
    {
        if (System.getProperty(key) != null)
        {
            props.setProperty(key, System.getProperty(key));

            return true;
        }

        return false;
    }

    /**
     * The system envs have preference over the properties files.
     * So we overload it
     *
     * @param props Properties
     * @param key   String
     * @return true if the key are overloaded by a system property
     */
    public boolean overloadPropertiesWithSystemEnv(Properties props,
                                                   String key)
    {
        if (systemGetEnv(key) != null)
        {
            props.setProperty(key, systemGetEnv(key));

            return true;
        }

        return false;
    }

    /**
     * Get the system env with the key param
     *
     * @param key String
     * @return String or null
     */
    public String systemGetEnv(String key)
    {
        return System.getenv(key);
    }

    /**
     * Get existing filesystem based on a combination of URI and env settings. Create new filesystem otherwise.
     *
     * @param uri URI of existing, or to be created filesystem.
     * @param env environment settings.
     * @return new or existing filesystem.
     */
    public FileSystem getFileSystem(URI uri,
                                    Map<String, ?> env)
    {
        validateUri(uri);

        Properties props = getProperties(uri, env);

        String key = this.getFileSystemKey(uri, props); // s3fs.access.key is part of the key here.

        if (fileSystems.containsKey(key))
        {
            return fileSystems.get(key);
        }

        return newFileSystem(uri, env);
    }

    @Override
    public S3FileSystem getFileSystem(URI uri)
    {
        validateUri(uri);

        String key = this.getFileSystemKey(uri);

        if (fileSystems.containsKey(key))
        {
            return fileSystems.get(key);
        }
        else
        {
            throw new FileSystemNotFoundException("S3 filesystem not yet created. Use newFileSystem() instead");
        }
    }

    private S3Path toS3Path(Path path)
    {
        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());

        return (S3Path) path;
    }

    /**
     * Deviation from spec: throws FileSystemNotFoundException if FileSystem
     * hasn't yet been initialized. Call newFileSystem() first.
     * Need credentials. Maybe set credentials after? how?
     * TODO: we can create a new one if the credentials are present by:
     * s3://access-key:secret-key@endpoint.com/
     */
    @Override
    public Path getPath(URI uri)
    {
        FileSystem fileSystem = getFileSystem(uri);

        /**
         * TODO: set as a list. one s3FileSystem by region
         */
        return fileSystem.getPath(uri.getPath());
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir,
                                                    DirectoryStream.Filter<? super Path> filter)
    {
        final S3Path s3Path = toS3Path(dir);

        return new DirectoryStream<Path>()
        {
            @Override
            public void close()
            {
                // nothing to do here
            }

            @Override
            public Iterator<Path> iterator()
            {
                return new S3FilteredIterator(s3Path, filter);
            }
        };
    }

    @Override
    public InputStream newInputStream(Path path,
                                      OpenOption... options)
            throws IOException
    {
        final S3Path s3Path = toS3Path(path);
        final String key = s3Path.getKey();
        final String bucketName = s3Path.getBucketName();

        Preconditions.checkArgument(options.length == 0,
                                    "OpenOptions not yet supported: %s",
                                    ImmutableList.copyOf(options)); // TODO
        Preconditions.checkArgument(!key.equals(""), "cannot create InputStream for root directory: %s", path);

        try
        {
            final S3Client client = s3Path.getFileSystem().getClient();
            final GetObjectRequest request = GetObjectRequest.builder()
                                                             .bucket(bucketName)
                                                             .key(key)
                                                             .build();
            final ResponseInputStream<GetObjectResponse> res = client.getObject(request);

            if (res == null)
            {
                final String message = format("The specified path is a directory: %s", path);
                throw new IOException(message);
            }

            return res;
        }
        catch (final S3Exception e)
        {
            if (e.statusCode() == NOT_FOUND)
            {
                throw new NoSuchFileException(path.toString());
            }

            // otherwise throws a generic IO exception
            final String message = format("Cannot access file: %s", path);
            throw new IOException(message, e);
        }
    }

    @Override
    public OutputStream newOutputStream(final Path path,
                                        final OpenOption... options)
            throws IOException
    {
        final S3Path s3Path = toS3Path(path);

        // validate options
        if (options.length > 0)
        {
            final Set<OpenOption> opts = new LinkedHashSet<>(Arrays.asList(options));

            // cannot handle APPEND here -> use newByteChannel() implementation
            if (opts.contains(StandardOpenOption.APPEND))
            {
                return super.newOutputStream(path, options);
            }

            if (opts.contains(StandardOpenOption.READ))
            {
                throw new IllegalArgumentException("READ not allowed");
            }

            final boolean create = opts.remove(StandardOpenOption.CREATE);
            final boolean createNew = opts.remove(StandardOpenOption.CREATE_NEW);
            final boolean truncateExisting = opts.remove(StandardOpenOption.TRUNCATE_EXISTING);

            // remove irrelevant/ignored options
            opts.remove(StandardOpenOption.WRITE);
            opts.remove(StandardOpenOption.SPARSE);

            if (!opts.isEmpty())
            {
                throw new UnsupportedOperationException(opts.iterator().next() + " not supported");
            }

            validateCreateAndTruncateOptions(path, s3Path, create, createNew, truncateExisting);
        }


        final Map<String, String> metadata = buildMetadataFromPath(path);

        S3FileSystem fileSystem = s3Path.getFileSystem();

        return new S3OutputStream(fileSystem.getClient(), s3Path.toS3ObjectId(), null, metadata, fileSystem.getRequestHeaderCacheControlProperty());
    }

    private void validateCreateAndTruncateOptions(final Path path,
                                                  final S3Path s3Path,
                                                  final boolean create,
                                                  final boolean createNew,
                                                  final boolean truncateExisting)
            throws FileAlreadyExistsException, NoSuchFileException
    {
        if (!(create && truncateExisting))
        {
            if (s3Path.getFileSystem().provider().exists(s3Path))
            {
                if (createNew || !truncateExisting)
                {
                    throw new FileAlreadyExistsException(path.toString());
                }
            }
            else if (!createNew && !create)
            {
                throw new NoSuchFileException(path.toString());
            }
        }
    }

    private Map<String, String> buildMetadataFromPath(final Path path)
    {
        final Map<String, String> metadata = new HashMap<>();
        final String contentType = Mimetype.getInstance().getMimetype(path);
        if (!StringUtils.isEmpty(contentType))
        {
            metadata.put(CONTENT_TYPE, contentType);
        }
        return metadata;
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options,
                                              FileAttribute<?>... attrs)
            throws IOException
    {
        S3Path s3Path = toS3Path(path);

        return new S3SeekableByteChannel(s3Path, options, true);
    }

    @Override
    public AsynchronousFileChannel newAsynchronousFileChannel(Path path,
                                                              Set<? extends OpenOption> options,
                                                              ExecutorService executor,
                                                              FileAttribute<?>... attrs)
            throws IOException
    {
        S3Path s3Path = toS3Path(path);

        return new S3FileChannel(s3Path, options, executor, true);
    }

    /**
     * Deviations from spec: Does not perform atomic check-and-create. Since a
     * directory is just an S3 object, all directories in the hierarchy are
     * created or it already existed.
     */
    @Override
    public void createDirectory(Path dir,
                                FileAttribute<?>... attrs)
            throws IOException
    {
        final S3Path s3Path = toS3Path(dir);
        final S3Client client = s3Path.getFileSystem().getClient();

        Preconditions.checkArgument(attrs.length == 0,
                                    "attrs not yet supported: %s",
                                    ImmutableList.copyOf(attrs)); // TODO

        if (exists(s3Path))
        {
            throw new FileAlreadyExistsException(format("target already exists: %s", s3Path));
        }

        // create bucket if necessary
        final Bucket bucket = s3Path.getFileStore().getBucket();
        final String bucketName = s3Path.getBucketName();

        if (bucket == null)
        {
            final CreateBucketRequest request = CreateBucketRequest.builder()
                                                                   .bucket(bucketName)
                                                                   .build();
            client.createBucket(request);
        }

        // create the object as directory
        final String directoryKey = s3Path.getKey().endsWith("/") ? s3Path.getKey() : s3Path.getKey() + "/";
        final PutObjectRequest request = PutObjectRequest.builder()
                                                         .bucket(bucketName)
                                                         .key(directoryKey)
                                                         .cacheControl(s3Path.getFileSystem().getRequestHeaderCacheControlProperty())
                                                         .contentLength(0L)
                                                         .build();

        client.putObject(request, RequestBody.fromBytes(new byte[0]));
    }

    @Override
    public void delete(Path path)
            throws IOException
    {
        final S3Path rootPath = toS3Path(path);
        final S3Client client = rootPath.getFileSystem().getClient();
        final String bucketName = rootPath.getBucketName();

        LinkedList<Deque<S3Path>> s3Paths = getPathsByBatch(rootPath);

        for (Deque<S3Path> batch : s3Paths)
        {
            deleteBatch(client, batch, bucketName);
        }
    }

    private void deleteBatch(S3Client client,
                             Deque<S3Path> batch,
                             String bucketName)
            throws IOException
    {

        List<ObjectIdentifier> keys = batch.stream()
                                           .map(s3Path -> ObjectIdentifier.builder()
                                                                          .key(s3Path.getKey())
                                                                          .build())
                                           .collect(Collectors.toList());

        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                                                                            .bucket(bucketName)
                                                                            .delete(Delete.builder()
                                                                                          .objects(keys)
                                                                                          .build())
                                                                            .build();

        try
        {
            client.deleteObjects(multiObjectDeleteRequest);
        }
        catch (SdkException e)
        {
            throw new IOException(e);
        }

        // we delete the two objects (sometimes exists the key '/' and sometimes not)
        keys = batch.stream()
                    .map(s3Path -> ObjectIdentifier.builder().key(s3Path.getKey() + '/').build())
                    .collect(Collectors.toList());
        multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                                                       .bucket(bucketName)
                                                       .delete(Delete.builder()
                                                                     .objects(keys)
                                                                     .build())
                                                       .build();

        try
        {
            client.deleteObjects(multiObjectDeleteRequest);
        }
        catch (SdkException e)
        {
            throw new IOException(e);
        }
    }

    private LinkedList<Deque<S3Path>> getPathsByBatch(S3Path path)
            throws IOException
    {
        LinkedList<S3Path> allPaths = new LinkedList<>();
        visitAllFiles(path, allPaths);

        return splitByBatchWithOrder(allPaths, MAX_OBJECT_PER_REQUEST);
    }

    private <T> LinkedList<Deque<T>> splitByBatchWithOrder(List<T> list,
                                                           int maxSize)
    {
        LinkedList<Deque<T>> pathsByBatch = new LinkedList<>();

        if (!list.isEmpty())
        {
            Deque<T> deque = new ArrayDeque<>();
            for (T t : list)
            {
                if (deque.size() < maxSize)
                {
                    deque.push(t);
                }
                else
                {
                    pathsByBatch.add(deque);
                    deque = new ArrayDeque<>();
                }
            }
            pathsByBatch.add(deque); // add the last batch

        }

        return pathsByBatch;
    }

    private void visitAllFiles(Path path,
                               LinkedList<S3Path> paths)
            throws IOException
    {
        S3Path s3Path = toS3Path(path);
        if (Files.notExists(s3Path))
        {
            LOGGER.warn("Deleting {} was skipped because the path was not found.", s3Path);
        }
        else
        {
            paths.add(s3Path);

            if (Files.isDirectory(s3Path))
            {
                try (final DirectoryStream<Path> stream = Files.newDirectoryStream(s3Path))
                {
                    for (Path child : stream)
                    {
                        visitAllFiles(child, paths);
                    }
                }
                catch (SecurityException e)
                {
                    LOGGER.warn("Deleting {} was skipped because the path could not be read-accessed.", s3Path);
                }
            }
        }
    }

    @Override
    public void copy(Path source,
                     Path target,
                     CopyOption... options)
            throws IOException
    {
        if (isSameFile(source, target))
        {
            return;
        }

        S3Path s3Source = toS3Path(source);
        S3Path s3Target = toS3Path(target);
        // TODO: implements support for copying directories

        Preconditions.checkArgument(!Files.isDirectory(source), "copying directories is not yet supported: %s", source);
        Preconditions.checkArgument(!Files.isDirectory(target), "copying directories is not yet supported: %s", target);

        ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
        verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING), actualOptions);

        if (exists(s3Target) && !actualOptions.contains(StandardCopyOption.REPLACE_EXISTING))
        {
            throw new FileAlreadyExistsException(format("target already exists: %s", target));
        }

        String bucketNameOrigin = s3Source.getBucketName();
        String keySource = s3Source.getKey();
        String bucketNameTarget = s3Target.getBucketName();
        String keyTarget = s3Target.getKey();
        final S3Client client = s3Source.getFileSystem().getClient();

        final String encodedUrl = encodeUrl(bucketNameOrigin, keySource);

        final CopyObjectRequest request = CopyObjectRequest.builder()
                                                           .copySource(encodedUrl)
                                                           .cacheControl(s3Target.getFileSystem().getRequestHeaderCacheControlProperty())
                                                           .destinationBucket(bucketNameTarget)
                                                           .destinationKey(keyTarget)
                                                           .build();

        client.copyObject(request);
    }

    private String encodeUrl(final String bucketNameOrigin,
                             final String keySource)
            throws UnsupportedEncodingException
    {
        String encodedUrl;
        try
        {
            encodedUrl = URLEncoder.encode(bucketNameOrigin + "/" + keySource, StandardCharsets.UTF_8.toString());
        }
        catch (final UnsupportedEncodingException e)
        {
            throw new UnsupportedEncodingException("URL could not be encoded: " + e.getMessage());
        }
        return encodedUrl;
    }

    @Override
    public void move(Path source,
                     Path target,
                     CopyOption... options)
            throws IOException
    {
        if (options != null && Arrays.asList(options).contains(StandardCopyOption.ATOMIC_MOVE))
        {
            throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "Atomic not supported");
        }

        copy(source, target, options);
        delete(source);
    }

    @Override
    public boolean isSameFile(Path path1,
                              Path path2)
    {
        return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
    }

    @Override
    public boolean isHidden(Path path)
    {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path)
    {
        return toS3Path(path).getFileStore();
    }

    @Override
    public void checkAccess(Path path,
                            AccessMode... modes)
            throws IOException
    {
        S3Path s3Path = toS3Path(path);

        Preconditions.checkArgument(s3Path.isAbsolute(), "path must be absolute: %s", s3Path);

        if (!exists(s3Path))
        {
            throw new NoSuchFileException(toString());
        }

        if (modes.length > 0)
        {
            final S3Object s3Object = s3Utils.getS3Object(s3Path);
            final String key = s3Object.key();
            final String bucket = s3Path.getBucketName();
            final S3AccessControlList accessControlList = new S3AccessControlList(bucket, key);

            accessControlList.checkAccess(modes);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public <V extends FileAttributeView> V getFileAttributeView(Path path,
                                                                Class<V> type,
                                                                LinkOption... options)
    {
        S3Path s3Path = toS3Path(path);
        if (type == BasicFileAttributeView.class)
        {
            return (V) new S3BasicFileAttributeView(s3Path);
        }
        else if (type == PosixFileAttributeView.class)
        {
            return (V) new S3PosixFileAttributeView(s3Path);
        }
        else if (type == null)
        {
            throw new NullPointerException("Type is mandatory");
        }
        else
        {
            return null;
        }
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path,
                                                            Class<A> type,
                                                            LinkOption... options)
            throws IOException
    {
        S3Path s3Path = toS3Path(path);
        if (type == BasicFileAttributes.class)
        {
            if (cache.isInTime(s3Path.getFileSystem().getCache(), s3Path.getFileAttributes()))
            {
                A result = type.cast(s3Path.getFileAttributes());
                s3Path.setFileAttributes(null);

                return result;
            }
            else
            {
                S3BasicFileAttributes attrs = s3Utils.getS3FileAttributes(s3Path);
                s3Path.setFileAttributes(attrs);

                return type.cast(attrs);
            }
        }
        else if (type == PosixFileAttributes.class)
        {
            if (s3Path.getFileAttributes() instanceof PosixFileAttributes &&
                cache.isInTime(s3Path.getFileSystem().getCache(), s3Path.getFileAttributes()))
            {
                A result = type.cast(s3Path.getFileAttributes());
                s3Path.setFileAttributes(null);

                return result;
            }

            S3PosixFileAttributes attrs = s3Utils.getS3PosixFileAttributes(s3Path);
            s3Path.setFileAttributes(attrs);

            return type.cast(attrs);
        }

        throw new UnsupportedOperationException(format("only %s or %s supported",
                                                       BasicFileAttributes.class,
                                                       PosixFileAttributes.class));
    }

    @Override
    public Map<String, Object> readAttributes(Path path,
                                              String attributes,
                                              LinkOption... options)
            throws IOException
    {
        if (attributes == null)
        {
            throw new IllegalArgumentException("Attributes null");
        }

        if (attributes.contains(":") && !attributes.contains("basic:") && !attributes.contains("posix:"))
        {
            throw new UnsupportedOperationException(format("attributes %s are not supported, only basic /" +
                                                           " posix are supported",
                                                           attributes));
        }

        if (attributes.equals("*") || attributes.equals("basic:*"))
        {
            BasicFileAttributes attr = readAttributes(path, BasicFileAttributes.class, options);

            return AttributesUtils.fileAttributeToMap(attr);
        }
        else if (attributes.equals("posix:*"))
        {
            PosixFileAttributes attr = readAttributes(path, PosixFileAttributes.class, options);

            return AttributesUtils.fileAttributeToMap(attr);
        }
        else
        {
            String[] filters = new String[]{ attributes };

            if (attributes.contains(","))
            {
                filters = attributes.split(",");
            }

            Class<? extends BasicFileAttributes> filter = BasicFileAttributes.class;

            if (attributes.startsWith("posix:"))
            {
                filter = PosixFileAttributes.class;
            }

            return AttributesUtils.fileAttributeToMap(readAttributes(path, filter, options), filters);
        }
    }

    @Override
    public void setAttribute(Path path,
                             String attribute,
                             Object value,
                             LinkOption... options)
    {
        throw new UnsupportedOperationException();
    }

    // ~~

    /**
     * Create the fileSystem
     *
     * @param uri   URI
     * @param props Properties
     * @return S3FileSystem never null
     */
    public S3FileSystem createFileSystem(URI uri,
                                         Properties props)
    {
        final String key = getFileSystemKey(uri, props);
        final S3Client client = getS3Client(uri, props);
        final String host = uri.getHost();
        final Properties properties = new Properties(props);
        return new S3FileSystem(this, key, client, host, properties);
    }

    protected S3Client getS3Client(URI uri,
                                   Properties props)
    {
        final S3Factory factory = getS3Factory(props);
        return factory.getS3Client(uri, props);
    }

    protected S3Factory getS3Factory(final Properties props)
    {
        if (props.containsKey(S3_FACTORY_CLASS))
        {
            String s3FactoryClass = props.getProperty(S3_FACTORY_CLASS);

            try
            {
                return (S3Factory) Class.forName(s3FactoryClass).getDeclaredConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException | NoSuchMethodException | InvocationTargetException e)
            {
                throw new S3FileSystemConfigurationException("Configuration problem, couldn't instantiate " +
                                                             "S3Factory (" + s3FactoryClass + "): ",
                                                             e);
            }
        }

        return new S3ClientFactory();
    }

    /**
     * find /amazon.properties in the classpath
     *
     * @return Properties amazon.properties
     */
    public Properties loadAmazonProperties()
    {
        Properties props = new Properties();

        // http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html
        // http://www.javaworld.com/javaqa/2003-08/01-qa-0808-property.html
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon.properties"))
        {
            if (in != null)
            {
                props.load(in);
            }
        }
        catch (IOException e)
        {
            // If amazon.properties can't be loaded that's ok.
        }

        return props;
    }

    // ~~~

    private <T> void verifySupportedOptions(Set<? extends T> allowedOptions,
                                            Set<? extends T> actualOptions)
    {
        Sets.SetView<? extends T> unsupported = difference(actualOptions, allowedOptions);

        Preconditions.checkArgument(unsupported.isEmpty(), "the following options are not supported: %s", unsupported);
    }

    private static boolean isBucketRoot(S3Path s3Path)
    {
        String key = s3Path.getKey();
        return key.equals("") || key.equals("/");
    }

    /**
     * check that the paths exists or not
     *
     * @param path S3Path
     * @return true if exists
     */
    boolean exists(S3Path path)
    {
        S3Path s3Path = toS3Path(path);

        if (isBucketRoot(s3Path))
        {
            // check to see if bucket exists
            try
            {
                s3Utils.listS3Objects(s3Path);
                return true;
            }
            catch (SdkException e)
            {
                return false;
            }
        }

        try
        {
            s3Utils.getS3Object(s3Path);

            return true;
        }
        catch (NoSuchFileException e)
        {
            return false;
        }
    }

    public void close(S3FileSystem fileSystem)
    {
        if (fileSystem.getKey() != null && fileSystems.containsKey(fileSystem.getKey()))
        {
            fileSystems.remove(fileSystem.getKey());
        }
    }

    public boolean isOpen(S3FileSystem s3FileSystem)
    {
        return fileSystems.containsKey(s3FileSystem.getKey());
    }

    /**
     * only 4 testing
     */

    protected static ConcurrentMap<String, S3FileSystem> getFilesystems()
    {
        return fileSystems;
    }

    public Cache getCache()
    {
        return cache;
    }

    public void setCache(Cache cache)
    {
        this.cache = cache;
    }

}
