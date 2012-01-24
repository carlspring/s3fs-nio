package org.weakref.s3fs;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
    private final AmazonS3Client client;

    public S3FileSystem(S3FileSystemProvider provider, AmazonS3Client client)
    {
        this.provider = provider;
        this.client = client;
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
        ImmutableList.Builder<Path> builder = ImmutableList.builder();

        for (Bucket bucket : client.listBuckets()) {
            builder.add(new S3Path(bucket.getName()));
        }

        return builder.build();
    }

    @Override
    public Iterable<FileStore> getFileStores()
    {
        return ImmutableList.of();
    }

    @Override
    public Set<String> supportedFileAttributeViews()
    {
        return ImmutableSet.of("basic");
    }

    @Override
    public Path getPath(String first, String... more)
    {
        if (more.length == 0) {
            return S3Path.forPath(first);
        }

        return new S3Path(first, more);
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
