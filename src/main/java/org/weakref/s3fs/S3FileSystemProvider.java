package org.weakref.s3fs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
        throw new UnsupportedOperationException();
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

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options)
            throws IOException
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHidden(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
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
}
