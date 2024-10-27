package org.carlspring.cloud.storage.s3fs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.carlspring.cloud.storage.s3fs.cache.S3FileAttributesCache;
import org.carlspring.cloud.storage.s3fs.util.S3Utils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Properties;
import java.util.Set;

import static org.carlspring.cloud.storage.s3fs.S3Path.PATH_SEPARATOR;

/**
 * S3FileSystem with a concrete client configured and ready to use.
 *
 * @see S3Client configured by {@link S3Factory}
 */
public class S3FileSystem
        extends FileSystem
        implements Comparable<S3FileSystem>
{

    private final S3FileSystemProvider provider;

    private final String key;

    private final S3Client client;

    private final String endpoint;

    private S3FileAttributesCache fileAttributesCache;

    private final Properties properties;

    public S3FileSystem(final S3FileSystemProvider provider,
                        final String key,
                        final S3Client client,
                        final String endpoint,
                        Properties properties)
    {
        this.provider = provider;
        this.key = key;
        this.client = client;
        this.endpoint = endpoint;
        this.properties = properties;

        int cacheTTL = Integer.parseInt(String.valueOf(properties.getOrDefault(S3Factory.CACHE_ATTRIBUTES_TTL, S3Factory.CACHE_ATTRIBUTES_TTL_DEFAULT)));
        int cacheSize = Integer.parseInt(String.valueOf(properties.getOrDefault(S3Factory.CACHE_ATTRIBUTES_SIZE, S3Factory.CACHE_ATTRIBUTES_SIZE_DEFAULT)));

        this.fileAttributesCache = new S3FileAttributesCache(cacheTTL, cacheSize);
    }

    public S3FileSystem(final S3FileSystemProvider provider,
                        final String key,
                        final S3Client client,
                        final String endpoint)
    {
        this(provider, key, client, endpoint, new Properties());
    }

    @Override
    public S3FileSystemProvider provider()
    {
        return provider;
    }

    public String getKey()
    {
        return key;
    }

    @Override
    public void close()
            throws IOException
    {
        this.fileAttributesCache.invalidateAll();
        this.provider.close(this);
    }

    @Override
    public boolean isOpen()
    {
        return this.provider.isOpen(this);
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    @Override
    public String getSeparator()
    {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories()
    {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();
        for (FileStore fileStore : getFileStores())
        {
            builder.add(((S3FileStore) fileStore).getRootDirectory());
        }
        return builder.build();
    }

    @Override
    public Iterable<FileStore> getFileStores()
    {
        ImmutableList.Builder<FileStore> builder = ImmutableList.builder();
        for (Bucket bucket : client.listBuckets().buckets())
        {
            builder.add(new S3FileStore(this, bucket.name()));
        }

        return builder.build();
    }

    @Override
    public Set<String> supportedFileAttributeViews()
    {
        return ImmutableSet.of("basic", "posix");
    }

    @Override
    public S3Path getPath(final String first,
                          final String... more)
    {
        if (more.length == 0)
        {
            return new S3Path(this, first);
        }

        return new S3Path(this, first, more);
    }

    @Override
    public PathMatcher getPathMatcher(final String syntaxAndPattern)
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
    {
        throw new UnsupportedOperationException();
    }

    public S3Client getClient()
    {
        return client;
    }

    /**
     * get the endpoint associated with this fileSystem.
     *
     * @return string
     * @see <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html">http://docs.aws.amazon.com/general/latest/gr/rande.html</a>
     */
    public String getEndpoint()
    {
        return endpoint;
    }

    /**
     * @deprecated Use {@link org.carlspring.cloud.storage.s3fs.util.S3Utils#key2Parts(String)} instead. To be removed in one of next majors versions.
     * @param keyParts
     * @return String[]
     */
    public String[] key2Parts(String keyParts)
    {
        return S3Utils.key2Parts(keyParts);
    }

    /**
     * @return The {@link S3FileAttributesCache} instance holding the path attributes cache for this file provider.
     */
    public S3FileAttributesCache getFileAttributesCache()
    {
        return fileAttributesCache;
    }

    /**
     * Used for internal testing.
     * @return the properties with which this file system was initialized.
     */
    protected Properties getProperties()
    {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    /**
     * @return The value of the {@link S3Factory#REQUEST_HEADER_CACHE_CONTROL} property. Default is empty.
     */
    public String getRequestHeaderCacheControlProperty()
    {
        return properties.getProperty(S3Factory.REQUEST_HEADER_CACHE_CONTROL, ""); // default is nothing.
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;

        int result = 1;
        result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (obj == null)
        {
            return false;
        }

        if (!(obj instanceof S3FileSystem))
        {
            return false;
        }

        S3FileSystem other = (S3FileSystem) obj;
        if (endpoint == null)
        {
            if (other.endpoint != null)
            {
                return false;
            }
        }
        else if (!endpoint.equals(other.endpoint))
        {
            return false;
        }
        if (key == null)
        {
            return other.key == null;
        }
        else
        {
            return key.equals(other.key);
        }
    }

    @Override
    public int compareTo(final S3FileSystem o)
    {
        return key.compareTo(o.getKey());
    }

}
