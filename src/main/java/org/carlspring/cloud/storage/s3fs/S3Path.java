package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import software.amazon.awssdk.core.Protocol;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;

public class S3Path
        implements Path
{

    public static final String PATH_SEPARATOR = "/";
    public static final String PATH_OTHER_INSTANCE_MESSAGE = "other must be an instance of %s";

    /**
     * S3FileStore which represents the Bucket this path resides in.
     */
    private final S3FileStore fileStore;

    /**
     * URI not encoded
     * Is the key for S3Client
     */
    private final String uri;

    /**
     * actual filesystem
     */
    private final S3FileSystem fileSystem;

    /**
     * S3BasicFileAttributes cache
     */
    private S3BasicFileAttributes fileAttributes;


    /**
     * Build an S3Path from path segments. '/' are stripped from each segment.
     *
     * @param fileSystem S3FileSystem
     * @param first      should be start with a '/' and is the bucket name
     * @param more       directories and files
     */
    public S3Path(S3FileSystem fileSystem, String first, String... more)
    {
        Preconditions.checkArgument(first != null, "The first path must not be null. Perhaps you forgot to set the bucket name?");
        Preconditions.checkArgument(!first.startsWith("//"), "The first path doesn't start with '//'. Missing bucket.");
        // see tests org.carlspring.cloud.storage.s3fs.Path.EndsWithTest#endsWithRelativeBlankAbsolute()

        boolean hasBucket = first.startsWith("/");

        List<String> pathsURI = Lists.newArrayList(Splitter.on(PATH_SEPARATOR).omitEmptyStrings().split(first));

        if (hasBucket)
        { // absolute path

            Preconditions.checkArgument(!pathsURI.isEmpty(), "path must start with bucket name");
            Preconditions.checkArgument(!pathsURI.get(0).isEmpty(), "bucket name must be not empty");

            // the filestore (bucket) is not part of the uri
            String bucket = pathsURI.remove(0);

            this.fileStore = new S3FileStore(fileSystem, bucket);
        }
        else
        {
            // relative uri
            this.fileStore = null;
        }

        StringBuilder uriBuilder = new StringBuilder();
        if (hasBucket)
        {
            uriBuilder.append(PATH_SEPARATOR);
        }

        for (String path : pathsURI)
        {
            uriBuilder.append(path + PATH_SEPARATOR);
        }

        if (more != null)
        {
            for (String path : more)
            {
                uriBuilder.append(path + PATH_SEPARATOR);
            }
        }
        String localUri = normalizeURI(uriBuilder.toString());
        // remove last PATH_SEPARATOR
        if (!first.isEmpty() &&
                // only first param and not ended with PATH_SEPARATOR
                ((!first.endsWith(PATH_SEPARATOR) && (more == null || more.length == 0))
                // we have more param and not ended with PATH_SEPARATOR
                || more != null &&  more.length > 0 && !more[more.length-1].endsWith(PATH_SEPARATOR))) {
            localUri = localUri.substring(0, localUri.length() - 1);
        }
        this.uri = localUri;
        this.fileSystem = fileSystem;
    }

    /**
     * Remove duplicated slash
     */
    private String normalizeURI(String uri)
    {
        return uri.replace("//", "/");
    }

    public S3FileStore getFileStore()
    {
        return fileStore;
    }

    /**
     * bucket name for amazon.
     *
     * @return the bucketName for S3Client
     */
    public String getBucketName()
    {
        return fileStore.name();
    }

    /**
     * key for amazon without final slash.
     * <b>note:</b> the final slash need to be added to save a directory (Amazon s3 spec)
     *
     * @return the key for S3Client
     */
    public String getKey()
    {
        String key = this.uri;

        if (key.startsWith("/"))
        {
            key = key.substring(1);
        }

        return key;
    }

    public S3ObjectId toS3ObjectId()
    {
        return S3ObjectId.builder()
                         .bucket(getBucketName())
                         .key(getKey())
                         .build();
    }

    @Override
    public S3FileSystem getFileSystem()
    {
        return this.fileSystem;
    }

    @Override
    public boolean isAbsolute()
    {
        return fileStore != null;
    }

    @Override
    public Path getRoot()
    {
        if (isAbsolute())
        {
            return new S3Path(fileSystem, PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR);
        }

        return null;
    }

    @Override
    public Path getFileName()
    {
        List<String> paths = uriToList();
        if (paths.isEmpty())
        {
            // get FileName of root directory is null
            return null;
        }

        String filename = paths.get(paths.size() - 1);

        return new S3Path(fileSystem, filename);
    }

    @Override
    public Path getParent()
    {
        // bucket is not present in the parts
        if (uri.isEmpty())
        {
            return null;
        }

        String newUri = this.uri;

        if (this.uri.endsWith("/"))
        {
            newUri = this.uri.substring(0, this.uri.length() - 1);
        }

        int lastPathSeparatorPosition = newUri.lastIndexOf(PATH_SEPARATOR);

        if (lastPathSeparatorPosition == -1)
        {
            return null;
        }

        newUri = uri.substring(0, lastPathSeparatorPosition + 1);

        if (newUri.isEmpty())
        {
            return null;
        }

        String filestore = isAbsolute() ? PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR : "";

        return new S3Path(fileSystem, filestore + newUri);
    }

    @Override
    public int getNameCount()
    {
        return uriToList().size();
    }

    @Override
    public Path getName(int index)
    {

        List<String> paths = uriToList();

        if (index < 0 || index >= paths.size())
        {
            throw new IllegalArgumentException("index out of range");
        }

        String path = paths.get(index);
        StringBuilder pathsBuilder = new StringBuilder();

        if (isAbsolute() && index == 0)
        {
            pathsBuilder.append(PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR);
        }

        pathsBuilder.append(path);

        if (index < paths.size() - 1)
        {
            pathsBuilder.append(PATH_SEPARATOR);
        }

        // if is the last path, check if end with path separator
        if (index == paths.size() - 1 && this.uri.endsWith(PATH_SEPARATOR))
        {
            pathsBuilder.append(PATH_SEPARATOR);
        }

        return new S3Path(fileSystem, pathsBuilder.toString());
    }

    private List<String> uriToList()
    {
        return Splitter.on(PATH_SEPARATOR).omitEmptyStrings().splitToList(this.uri);
    }

    /**
     * The bucket name not count
     */
    @Override
    public Path subpath(int beginIndex, int endIndex)
    {
        List<String> paths = uriToList();

        if (beginIndex < 0 || endIndex > paths.size())
        {
            throw new IllegalArgumentException("index out of range");
        }

        List<String> pathSubList = paths.subList(beginIndex, endIndex);
        StringBuilder pathsStringBuilder = new StringBuilder();

        // build path string
        if (this.isAbsolute() && beginIndex == 0)
        {
            pathsStringBuilder.append(PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR);
        }

        for (String path : pathSubList)
        {
            pathsStringBuilder.append(path).append(PATH_SEPARATOR);
        }

        String pathsResult = pathsStringBuilder.toString();

        // if the uri doesnt have last PATH_SEPARATOR we must remove it.
        if (endIndex == paths.size() && !this.uri.endsWith(PATH_SEPARATOR))
        {
            pathsResult = pathsResult.substring(0, pathsResult.length() - 1);
        }

        return new S3Path(fileSystem, pathsResult);
    }

    @Override
    public boolean startsWith(Path other)
    {
        if (other.getNameCount() > this.getNameCount())
        {
            return false;
        }

        if (!(other instanceof S3Path))
        {
            return false;
        }

        if (this.isAbsolute() && !other.isAbsolute())
        {
            return false;
        }

        S3Path path = (S3Path) other;

        if (this.isAbsolute() && other.isAbsolute() && !getBucketName().equals(path.getBucketName()))
        {
            return false;
        }

        if (path.uri.isEmpty() && !this.uri.isEmpty())
        {
            return false;
        }

        List<String> pathsOther = path.uriToList();
        List<String> paths = this.uriToList();

        for (int i = 0; i < pathsOther.size(); i++)
        {
            if (!pathsOther.get(i).equals(paths.get(i)))
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean startsWith(String path)
    {
        S3Path other = new S3Path(this.fileSystem, path);

        return this.startsWith(other);
    }

    @Override
    public boolean endsWith(Path other)
    {
        if (other.getNameCount() > this.getNameCount())
        {
            return false;
        }
        // empty
        if (other.getNameCount() == 0 && this.getNameCount() != 0)
        {
            return false;
        }

        if (!(other instanceof S3Path))
        {
            return false;
        }

        S3Path path = (S3Path) other;

        if ((path.getFileStore() != null && !path.getFileStore().equals(this.getFileStore())) ||
            (path.getFileStore() != null && this.getFileStore() == null))
        {
            return false;
        }

        // check subkeys

        List<String> pathsOther = path.uriToList();
        List<String> paths = this.uriToList();

        int i = pathsOther.size() - 1;
        int j = paths.size() - 1;
        while (i >= 0 && j >= 0)
        {
            if (!pathsOther.get(i).equals(paths.get(j)))
            {
                return false;
            }

            i--;
            j--;
        }

        return true;
    }

    @Override
    public boolean endsWith(String other)
    {
        return this.endsWith(new S3Path(this.fileSystem, other));
    }

    // Inspired by "JimfsPath.isNormal()"
    @Override
    public Path normalize()
    {
        List<String> pathParts = uriToList();
        if (isNormal(pathParts))
            return this;

        Deque<String> newPathParts = new ArrayDeque<>();
        for (String pathPart : pathParts)
        {
            if (pathPart.equals(".."))
            {
                String lastPathPart = newPathParts.peekLast();
                if (lastPathPart != null && !lastPathPart.equals(".."))
                {
                    newPathParts.removeLast();
                }
                else if (!isAbsolute())
                {
                    // if there's a root and we have an extra ".." that would go
                    // up above the root, ignore it
                    newPathParts.add(pathPart);
                }
            }
            else if (!pathPart.equals("."))
            {
                newPathParts.add(pathPart);
            }
        }
        StringBuilder pathBuilder = new StringBuilder();
        if (this.isAbsolute())
        {
            pathBuilder.append(PATH_SEPARATOR).append(this.fileStore.name()).append(PATH_SEPARATOR);
        }
        pathBuilder.append(Joiner.on(PATH_SEPARATOR).join(newPathParts));
        if (newPathParts.size() > 0 && uri.endsWith(PATH_SEPARATOR))
        {
            pathBuilder.append(PATH_SEPARATOR);
        }
        return new S3Path(this.fileSystem, pathBuilder.toString());
    }

    // Inspired by "JimfsPath.isNormal()"
    private boolean isNormal(List<String> pathParts)
    {
        if (getNameCount() == 0 || getNameCount() == 1 && !isAbsolute())
        {
            return true;
        }
        boolean foundNonParentName = isAbsolute(); // if there's a root, the
                                                   // path doesn't start with ..
        boolean normal = true;
        for (String pathPart : pathParts)
        {
            if (pathPart.equals(".."))
            {
                if (foundNonParentName)
                {
                    normal = false;
                    break;
                }
            }
            else
            {
                if (pathPart.equals("."))
                {
                    normal = false;
                    break;
                }
                foundNonParentName = true;
            }
        }
        return normal;
    }

    @Override
    public Path resolve(Path other)
    {
        if (other.isAbsolute())
        {
            Preconditions.checkArgument(other instanceof S3Path,
                                        PATH_OTHER_INSTANCE_MESSAGE,
                                        S3Path.class.getName());

            return other;
        }

        S3Path otherS3Path = (S3Path) other;
        StringBuilder pathBuilder = new StringBuilder();

        if (this.isAbsolute())
        {
            pathBuilder.append(PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR);
        }

        pathBuilder.append(this.uri);

        if (!otherS3Path.uri.isEmpty())
        {
            pathBuilder.append(PATH_SEPARATOR + otherS3Path.uri);
        }

        return new S3Path(this.fileSystem, pathBuilder.toString());
    }

    @Override
    public Path resolve(String other)
    {
        return resolve(new S3Path(this.getFileSystem(), other));
    }

    @Override
    public Path resolveSibling(Path other)
    {
        Preconditions.checkArgument(other instanceof S3Path, PATH_OTHER_INSTANCE_MESSAGE, S3Path.class.getName());

        S3Path s3Path = (S3Path) other;

        Path parent = getParent();

        if (parent == null || s3Path.isAbsolute())
        {
            return s3Path;
        }

        List<String> othersPaths = s3Path.uriToList();

        if (othersPaths.isEmpty())
        { // other is relative and empty
            return parent;
        }

        List<String> paths = this.uriToList();

        StringBuilder pathBuilder = new StringBuilder();
        String lastPath = othersPaths.get(othersPaths.size() - 1);

        if (isAbsolute())
        {
            pathBuilder.append(PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR);
        }
        for (String path : concat(paths.subList(0, paths.size() - 1), othersPaths))
        {
            pathBuilder.append(path);
            if (!lastPath.equals(path) || s3Path.uri.endsWith(PATH_SEPARATOR))
            {
                pathBuilder.append(PATH_SEPARATOR);
            }
        }

        return new S3Path(fileSystem, pathBuilder.toString());
    }

    @Override
    public Path resolveSibling(String other)
    {
        return resolveSibling(new S3Path(this.getFileSystem(), other));
    }

    @Override
    public Path relativize(Path other)
    {
        Preconditions.checkArgument(other instanceof S3Path, PATH_OTHER_INSTANCE_MESSAGE, S3Path.class.getName());
        S3Path s3Path = (S3Path) other;

        if (this.equals(other))
        {
            return new S3Path(this.getFileSystem(), "");
        }

        Preconditions.checkArgument(isAbsolute(), "Path is already relative: %s", this);
        Preconditions.checkArgument(s3Path.isAbsolute(), "Cannot relativize against a relative path: %s", s3Path);
        Preconditions.checkArgument(fileStore.equals(s3Path.getFileStore()),
                                    "Cannot relativize paths with different buckets: '%s', '%s'",
                                    this,
                                    other);

        String uriPath = decode(URI.create(encode(this.uri)).relativize(URI.create(encode(s3Path.uri))));

        return new S3Path(fileSystem, uriPath);
    }

    /**
     * Examples:
     * <p>
     * Relative:
     * --------
     * NO use fileSystem and not used fileStore.
     * - path/file
     * <p>
     * Absolute:
     * --------
     * Use the fileSystem to get the host and the filestore to get the first path (in the future the filestore can be attached to the host)
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
     * - s3://AMAZONACCESSKEY@s3.amazonaws.com/bucket/path/file
     * - s3://AMAZONACCESSKEY@bucket.s3.amazonaws.com/path/file
     * - s3://s3-aws-region.amazonaws.com/bucket/path/file
     *
     * @return URI never null
     */
    @Override
    public URI toUri()
    {
        String encodedUri = encode(this.uri);

        // absolute
        if (this.isAbsolute())
        {
            String builder = fileSystem.getKey() + PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR + encodedUri;

            return URI.create("s3://" + normalizeURI(builder));
        }
        else
        {
            return URI.create(encodedUri);
        }
    }

    /**
     * Get the url for the s3Path.
     * <p>
     * The url represents a Uniform Resource
     * Locator, a pointer to a "resource" on the World
     * Wide Web.
     * <p>
     * All S3Path has a URL if is absolute
     *
     * @return URL or null if is not absoulte
     * @see S3Path#toUri() for unique resource identifier
     */
    public URL toURL()
    {
        if (!this.isAbsolute())
        {
            return null;
        }

        final S3Client s3client = this.getFileSystem().getClient();
        //TODO: How to get s3fs.protocol property value?
        final String protocol = Protocol.HTTPS.name();
        final String endpointStr = String.format("%s://%s", protocol, this.getFileSystem().getEndpoint());
        final URI endpoint = URI.create(endpointStr);
        final S3Utilities utilities = s3client.utilities();
        final String bucketName = this.getBucketName();
        final String key = this.getKey();
        final GetUrlRequest request = GetUrlRequest.builder().bucket(bucketName).key(key).endpoint(endpoint).build();
        return utilities.getUrl(request);
    }

    @Override
    public Path toAbsolutePath()
    {
        if (isAbsolute())
        {
            return this;
        }

        throw new IllegalStateException(format("Relative path cannot be made absolute: %s", this));
    }

    @Override
    public Path toRealPath(LinkOption... options)
    {
        return toAbsolutePath();
    }

    @Override
    public File toFile()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator()
    {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();

        if (isAbsolute())
        {
            builder.add(new S3Path(fileSystem, PATH_SEPARATOR + getBucketName() + PATH_SEPARATOR));
        }

        List<String> paths = uriToList();

        if (uriToList().isEmpty())
        {
            return builder.build().iterator();
        }

        String lastPath = paths.get(paths.size() - 1);

        for (String path : uriToList())
        {
            String pathFinal = path + PATH_SEPARATOR;

            if (path.equals(lastPath) && !lastPath.endsWith(PATH_SEPARATOR))
            {
                pathFinal = pathFinal.substring(0, pathFinal.length() - 1);
            }

            builder.add(new S3Path(fileSystem, pathFinal));
        }

        return builder.build().iterator();
    }

    @Override
    public int compareTo(Path other)
    {
        return toString().compareTo(other.toString());
    }

    @Override
    public String toString()
    {
        return toUri().toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        S3Path path = (S3Path) o;
        if (fileStore != null ? !fileStore.equals(path.fileStore) : path.fileStore != null)
        {
            return false;
        }

        return uri.equals(path.uri);
    }

    @Override
    public int hashCode()
    {
        int result = fileStore != null ? fileStore.name().hashCode() : 0;
        result = 31 * result + uri.hashCode();

        return result;
    }

    /**
     * Encode special URI characters for path.
     *
     * @param uri String the uri path
     * @return String
     */
    private String encode(String uri)
    {
        // remove special case URI starting with //
        uri = uri.replace("//", "/");
        uri = uri.replaceAll(" ", "%20");

        return uri;
    }

    /**
     * Decode uri special characters
     *
     * @param uri URI mandatory
     * @return String decoded
     */
    private String decode(URI uri)
    {
        try
        {
            return URLDecoder.decode(uri.toString(), "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new IllegalStateException("Error decoding key: " + this.uri, e);
        }
    }

    public S3BasicFileAttributes getFileAttributes()
    {
        return fileAttributes;
    }

    public void setFileAttributes(S3BasicFileAttributes fileAttributes)
    {
        this.fileAttributes = fileAttributes;
    }

}
