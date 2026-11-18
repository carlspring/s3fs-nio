package org.carlspring.cloud.storage.s3fs;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.spy;

/**
 * A Mock friendly version of the {@link S3FileSystemProvider} which allows a bit more flexibility for testing purposes.
 */
public class S3FileSystemProviderMock extends S3FileSystemProvider
{

    protected final ConcurrentHashMap<Path, S3SeekableByteChannel> byteChannelSpies = new ConcurrentHashMap<>();

    public ConcurrentHashMap<Path, S3SeekableByteChannel> getByteChannelsSpies()
    {
        return byteChannelSpies;
    }

    @Override
    public String getScheme()
    {
        return super.getScheme() + "mock";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env)
    {
        return super.newFileSystem(uri, env);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options,
                                              FileAttribute<?>... attrs) throws IOException
    {
        S3SeekableByteChannel delegate = (S3SeekableByteChannel) super.newByteChannel(path, options, attrs);
        S3SeekableByteChannel mock = spy(delegate);
        byteChannelSpies.put(path, mock);
        return mock;
    }

    @SuppressWarnings("unchecked")
    protected S3FileSystemRegistryMock getFileSystemRegistry()
    {
        return S3FileSystemRegistryMock.getInstance();
    }
}
