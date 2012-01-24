package org.weakref.s3fs;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class S3Path
        implements Path
{
    public static final String PATH_SEPARATOR = "/";

    private final String bucket;

    private final boolean directory; // files that end in "/" are considered directories in s3
    private final List<String> parts;

    /**
     * Root component for the given bucket
     */
    public S3Path(String bucket)
    {
        this.bucket = bucket;
        this.parts = ImmutableList.of();
        this.directory = true;
    }

    /**
     * Build an S3Path from path segments. '/' are stripped from each segment.
     *
     * If the last part ends in '/', the path is considered directory
     */
    public S3Path(String bucket, String... parts)
    {
        this(bucket,
                ImmutableList.copyOf(parts),
                parts.length == 0 || (parts.length > 0 && parts[parts.length - 1].endsWith("/"))
                );
    }


    private S3Path(String bucket, Iterable<String> parts, boolean isDirectory)
    {
        if (bucket != null) {
            bucket = bucket.replace("/", "");
        }

        this.bucket = bucket;
        this.directory = isDirectory;
        this.parts = ImmutableList.copyOf(transform(parts, strip("/")));
    }
    
    /**
     * path must be a string of the form "/{bucket}", "/{bucket}/{key}" or just "{key}"
     *
     * redundant '/' are stripped
     */
    public static S3Path forPath(String path)
    {
        List<String> parts = ImmutableList.copyOf(Splitter.on(PATH_SEPARATOR).omitEmptyStrings().split(path));

        String bucket = null;
        List<String> pathParts = parts;

        if (path.startsWith(PATH_SEPARATOR)) { // absolute path
            Preconditions.checkArgument(parts.size() >= 1, "path must start with bucket name");

            bucket = parts.get(0);

            if (!parts.isEmpty()) {
                pathParts = parts.subList(1, parts.size());
            }
        }

        return new S3Path(bucket, pathParts, path.endsWith("/") || pathParts.isEmpty());
    }

    public String getBucket()
    {
        return bucket;
    }
    
    public String getKey()
    {
        if (parts.isEmpty()) {
            return "";
        }

        ImmutableList.Builder<String> builder = ImmutableList.<String>builder().addAll(parts);
        if (isDirectory()) {
            builder.add(""); // to get a trailing '/'
        }

        return Joiner.on(PATH_SEPARATOR).join(builder.build());
    }
    
    @Override
    public FileSystem getFileSystem()
    {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public boolean isAbsolute()
    {
        return bucket != null;
    }

    public boolean isDirectory()
    {
        return directory;
    }

    @Override
    public Path getRoot()
    {
        if (isAbsolute()) {
            return new S3Path(bucket, ImmutableList.<String>of(), true);
        }

        return null;
    }

    @Override
    public Path getFileName()
    {
        if (!parts.isEmpty()) {
            return new S3Path(null, parts.subList(parts.size() - 1, parts.size()), directory);
        }

        return null;
    }

    @Override
    public Path getParent()
    {
        if (parts.isEmpty()) {
            return null;
        }
        
        return new S3Path(bucket, parts.subList(0, parts.size() - 1), true);
    }

    @Override
    public int getNameCount()
    {
        return parts.size();
    }

    @Override
    public Path getName(int index)
    {
        boolean isDirectory = (index < parts.size() - 1) || directory;

        return new S3Path(null, parts.subList(index, index + 1), isDirectory);
    }

    @Override
    public Path subpath(int beginIndex, int endIndex)
    {
        boolean isDirectory = (endIndex <= parts.size() - 1) || directory;

        return new S3Path(null, parts.subList(beginIndex, endIndex), isDirectory);
    }

    @Override
    public boolean startsWith(Path other)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean startsWith(String other)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean endsWith(Path other)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean endsWith(String other)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path normalize()
    {
        return this;
    }

    @Override
    public Path resolve(Path other)
    {
        Preconditions.checkArgument(other instanceof S3Path, "other must be an instance of %s", S3Path.class.getName());

        S3Path s3Path = (S3Path) other;

        if (s3Path.isAbsolute()) {
            return s3Path;
        }

        if (s3Path.parts.isEmpty()) { // other is relative and empty
            return this;
        }

        return new S3Path(bucket, concat(parts, s3Path.parts), s3Path.directory);
    }

    @Override
    public Path resolve(String other)
    {
        return resolve(forPath(other));
    }

    @Override
    public Path resolveSibling(Path other)
    {
        Preconditions.checkArgument(other instanceof S3Path, "other must be an instance of %s", S3Path.class.getName());

        S3Path s3Path = (S3Path) other;

        Path parent = getParent();

        if (parent == null || s3Path.isAbsolute()) {
            return s3Path;
        }

        if (s3Path.parts.isEmpty()) { // other is relative and empty
            return parent;
        }

        return new S3Path(bucket, concat(parts.subList(0, parts.size() - 1), s3Path.parts), s3Path.directory);
    }

    @Override
    public Path resolveSibling(String other)
    {
        return resolveSibling(forPath(other));
    }

    @Override
    public Path relativize(Path other)
    {
        Preconditions.checkArgument(other instanceof S3Path, "other must be an instance of %s", S3Path.class.getName());
        S3Path s3Path = (S3Path) other;

        if (this.equals(other)) {
            return S3Path.forPath("");
        }


        Preconditions.checkArgument(isAbsolute(), "Path is already relative: %s", this);
        Preconditions.checkArgument(s3Path.isAbsolute(), "Cannot relativize against a relative path: %s", s3Path);
        Preconditions.checkArgument(bucket.equals(s3Path.getBucket()), "Cannot relativize paths with different buckets: '%s', '%s'", this, other);

        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public URI toUri()
    {
        return URI.create("s3://" + bucket + Joiner.on(PATH_SEPARATOR).join(parts));
    }

    @Override
    public Path toAbsolutePath()
    {
        if (isAbsolute()) {
            return this;
        }

        throw new IllegalStateException(format("Relative path cannot be made absolute: %s", this));
    }

    @Override
    public Path toRealPath(LinkOption... options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public File toFile()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator()
    {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();

        for (Iterator<String> iterator = parts.iterator(); iterator.hasNext(); ) {
            String part =  iterator.next();

            boolean isDirectory = iterator.hasNext() || directory;

            builder.add(new S3Path(null, ImmutableList.of(part), isDirectory));
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
        StringBuilder builder = new StringBuilder();

        if (isAbsolute()) {
            builder.append(PATH_SEPARATOR);
            builder.append(bucket);
            builder.append(PATH_SEPARATOR);
        }

        builder.append(Joiner.on(PATH_SEPARATOR).join(parts));

        if (directory && !parts.isEmpty()) {
            builder.append(PATH_SEPARATOR);
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        S3Path paths = (S3Path) o;

        if (directory != paths.directory) {
            return false;
        }
        if (bucket != null ? !bucket.equals(paths.bucket) : paths.bucket != null) {
            return false;
        }
        if (!parts.equals(paths.parts)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = bucket != null ? bucket.hashCode() : 0;
        result = 31 * result + (directory ? 1 : 0);
        result = 31 * result + parts.hashCode();
        return result;
    }


    private static Function<String, String> strip(final String str)
    {
        return new Function<String, String>()
        {
            public String apply(String input)
            {
                return input.replace(str, "");
            }
        };
    }
}
