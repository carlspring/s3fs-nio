package org.weakref.s3fs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

public class S3FileSystem
        extends FileSystem
{
    private final S3FileSystemProvider provider;

    public S3FileSystem(S3FileSystemProvider provider)
    {
        this.provider = provider;
    }

    @Override
    public FileSystemProvider provider()
    {
        return provider;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    @Override
    public String getSeparator()
    {
        return S3Path.PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<FileStore> getFileStores()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> supportedFileAttributeViews()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getPath(String first, String... more)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService()
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
