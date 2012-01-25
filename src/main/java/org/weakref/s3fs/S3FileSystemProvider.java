package org.weakref.s3fs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;

/**
 * Spec:
 *
 * URI: s3://[endpoint]/{bucket}/{key}
 *   If endpoint is missing, it's assumed to be the default S3 endpoint (s3.amazonaws.com)
 *
 * FileSystem roots: /{bucket}/
 *
 * Treatment of S3 objects:
 *  - If a key ends in "/" it's considered a directory *and* a regular file. Otherwise, it's just a regular file.
 *  - It is legal for a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a directory.
 *  - If a file "a/b/c" exists but there's no "a" or "a/b/", these are considered "implicit" directories. They can be listed and traversed,
 *    but they can't be deleted.
 *
 * Deviations from FileSystem provider API:
 *  - Deleting a file or directory always succeeds, regardless of whether the file/directory existed before the operation was issued
 *     i.e. Files.delete() and Files.deleteIfExists() are equivalent.
 *
 *
 * Future versions of this provider might allow for a strict mode that mimics the semantics of the FileSystem provider API on a best effort
 * basis, at an increased processing cost.
 *
 * TODO: how to deal with multiple '/' in a row? (e.g., a///b/c)
 * TODO: support for multiple independent filesystems (different endpoints, different credentials)
 *
 */
public class S3FileSystemProvider
        extends FileSystemProvider
{
    public static final String ACCESS_KEY = "access-key";
    public static final String SECRET_KEY = "secret-key";

    private final AtomicReference<S3FileSystem> fileSystem = new AtomicReference<>();

    @Override
    public String getScheme()
    {
        return "s3";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env)
            throws IOException
    {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals("s3"), "uri scheme must be 's3': '%s'", uri);

        Object accessKey = env.get(ACCESS_KEY);
        Object secretKey = env.get(SECRET_KEY);

        Preconditions.checkArgument((accessKey == null && secretKey == null) || (accessKey != null && secretKey != null), "%s and %s should both be provided or should both be omitted", ACCESS_KEY, SECRET_KEY);

        AmazonS3Client client;

        if (accessKey == null && secretKey == null) {
            client = new AmazonS3Client();
        }
        else {
            client = new AmazonS3Client(new BasicAWSCredentials(accessKey.toString(), secretKey.toString()));
        }

        if (uri.getHost() != null) {
            client.setEndpoint(uri.getHost());
        }

        S3FileSystem result = new S3FileSystem(this, client);

        if (!fileSystem.compareAndSet(null, result)) {
            throw new FileSystemAlreadyExistsException("S3 filesystem already exists. Use getFileSystem() instead");
        }

        return result;
    }

    @Override
    public FileSystem getFileSystem(URI uri)
    {
        FileSystem fileSystem = this.fileSystem.get();

        if (fileSystem == null) {
            throw new FileSystemNotFoundException(String.format("S3 filesystem not yet created. Use newFileSystem() instead"));
        }

        return fileSystem;
    }

    /**
     * Deviation from spec: throws FileSystemNotFoundException if FileSystem hasn't yet been initialized. Call newFileSystem() first.
     */
    @Override
    public Path getPath(URI uri)
    {
        Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "URI scheme must be %s", getScheme());
        
        if (uri.getHost() != null && !uri.getHost().isEmpty()) {
            throw new IllegalArgumentException(String.format("non-empty URI host not supported at this time: %s", uri.getHost())); // TODO
        }

        return getFileSystem(uri).getPath(uri.getPath());
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options)
            throws IOException
    {
        Preconditions.checkArgument(options.length == 0, "OpenOptions not yet supported: %s", ImmutableList.copyOf(options)); // TODO

        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
        S3Path s3Path = (S3Path) path;

        Preconditions.checkArgument(!s3Path.getKey().equals(""), "cannot create InputStream for root directory: %s", s3Path);

        return s3Path.getFileSystem()
                .getClient()
                .getObject(s3Path.getBucket(), s3Path.getKey())
                .getObjectContent();
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Deviations from spec:
     *  Does not perform atomic check-and-create.
     *  Since a directory is just an S3 object, all directories in the hierarchy are created implicitly
     *  The call succeeds whether the the directory was created or it already existed.
     */
    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs)
            throws IOException
    {
        S3Path s3Path = (S3Path) dir;
        Preconditions.checkArgument(s3Path.isDirectory(), "dir does not represent a directory path: %s", dir);

        Preconditions.checkArgument(attrs.length == 0, "attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        s3Path.getFileSystem()
                .getClient()
                .putObject(s3Path.getBucket(), s3Path.getKey(), new ByteArrayInputStream(new byte[0]), metadata);
    }

    /**
     * Deviations from spec:
     *  - does not check whether path exists before deleting it. I.e., the operation is considered
     *    successful whether the entry was deleted or it didn't exist in the first place
     *  - doesn't throw DirectoryNotEmptyException if the path is a directory and it contains entries
     */
    @Override
    public void delete(Path path)
            throws IOException
    {
        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());

        S3Path s3Path = (S3Path) path;
        s3Path.getFileSystem()
                .getClient()
                .deleteObject(s3Path.getBucket(), s3Path.getKey());
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options)
            throws IOException
    {
        Preconditions.checkArgument(source instanceof S3Path, "source must be an instance of %s", S3Path.class.getName());
        Preconditions.checkArgument(target instanceof S3Path, "target must be an instance of %s", S3Path.class.getName());

        if (isSameFile(source, target)) {
            return;
        }

        S3Path s3Source = (S3Path) source;
        S3Path s3Target = (S3Path) target;

        Preconditions.checkArgument(!s3Source.isDirectory(), "copying directories is not yet supported: %s", source); // TODO
        Preconditions.checkArgument(!s3Target.isDirectory(), "copying directories is not yet supported: %s", target); // TODO

        ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
        verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING), actualOptions);

        if (!actualOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
            if (exists(s3Target)) {
                throw new FileAlreadyExistsException(format("target already exists: %s", target));
            }
        }

        s3Source.getFileSystem()
                .getClient()
                .copyObject(s3Source.getBucket(), s3Source.getKey(), s3Target.getBucket(), s3Target.getKey());
    }

    @Override
    public void move(Path source, Path target, CopyOption... options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSameFile(Path path1, Path path2)
            throws IOException
    {
        return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
    }

    @Override
    public boolean isHidden(Path path)
            throws IOException
    {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException
    {
        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
        S3Path s3Path = (S3Path) path;

        if (type == BasicFileAttributes.class) {
            AmazonS3 client = s3Path.getFileSystem().getClient();

            ObjectMetadata metadata = null;
            try {
                metadata = client.getObjectMetadata(s3Path.getBucket(), s3Path.getKey());
            }
            catch (AmazonS3Exception e) {
                if (!s3Path.isDirectory()) { // don't bail out if this path represents a directory -- see if it has children
                    if (e.getStatusCode() == 404) {
                        throw new NoSuchFileException(path.toString());
                    }
                    Throwables.propagate(e);
                }
            }

            if (metadata != null) {
                return type.cast(new S3FileAttributes(s3Path.getKey(),
                        FileTime.from(metadata.getLastModified().getTime(), TimeUnit.MILLISECONDS),
                        metadata.getContentLength(),
                        s3Path.isDirectory(),
                        true));
            }

            // key doesn't exist. See if it's an "implicit" directory (i.e., there are entries with it as a common prefix)
            if (s3Path.isDirectory()) {
                ListObjectsRequest request = new ListObjectsRequest(s3Path.getBucket(), s3Path.getKey(), null, "/", 1);
                ObjectListing listing = client.listObjects(request);

                if (!listing.getObjectSummaries().isEmpty() || !listing.getCommonPrefixes().isEmpty()) {
                    // has children
                    return type.cast(new S3FileAttributes(s3Path.getKey(),
                            FileTime.from(0, TimeUnit.MILLISECONDS),
                            0,
                            true,
                            false));
                }
            }
        }

        return null;
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private <T> void verifySupportedOptions(Set<? extends T> allowedOptions, Set<? extends T> actualOptions)
    {
        Sets.SetView<? extends T> unsupported = difference(actualOptions, allowedOptions);
        Preconditions.checkArgument(unsupported.isEmpty(), "the following options are not supported: %s", unsupported);
    }

    private boolean exists(S3Path path)
    {
        AmazonS3 client = path.getFileSystem().getClient();

        try {
            client.getObjectMetadata(path.getBucket(), path.getKey());
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            Throwables.propagate(e);
        }

        return true;
    }
}
