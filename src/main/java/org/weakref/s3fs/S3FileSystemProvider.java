package org.weakref.s3fs;

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
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

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
    @Override
    public String getScheme()
    {
        return "s3";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileSystem getFileSystem(URI uri)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getPath(URI uri)
    {
        throw new UnsupportedOperationException();
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
