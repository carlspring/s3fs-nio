package org.carlspring.cloud.storage.s3fs;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.Tika;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static java.lang.String.format;

public class S3SeekableByteChannel
        implements SeekableByteChannel
{

    private final S3Path path;

    private final Set<? extends OpenOption> options;

    private final SeekableByteChannel seekable;

    private final Path tempFile;

    private final String requestCacheControlHeader;


    /**
     * Open or creates a file, returning a seekable byte channel
     *
     * @param path             the path open or create
     * @param options          options specifying how the file is opened
     * @param tempFileRequired true if a temp file wanted, false in case of a in-memory solution option.
     * @throws IOException if an I/O error occurs
     */
    public S3SeekableByteChannel(final S3Path path,
                                 final Set<? extends OpenOption> options,
                                 final boolean tempFileRequired)
            throws IOException
    {
        this.path = path;
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
        this.requestCacheControlHeader = path.getFileSystem().getRequestHeaderCacheControlProperty();

        final String key = path.getKey();
        final boolean exists = path.getFileSystem().provider().exists(path);

        if (exists && this.options.contains(StandardOpenOption.CREATE_NEW))
        {
            throw new FileAlreadyExistsException(format("target already exists: %s", path));
        }
        else if (!exists && !this.options.contains(StandardOpenOption.CREATE_NEW) && !this.options.contains(
                StandardOpenOption.CREATE))
        {
            throw new NoSuchFileException(format("target not exists: %s", path));
        }

        final Set<? extends OpenOption> seekOptions = new HashSet<>(this.options);
        seekOptions.remove(StandardOpenOption.CREATE_NEW);

        if(tempFileRequired)
        {
            tempFile = Files.createTempFile("s3fs-", ".tmp");

            boolean removeTempFile = true;

            try
            {
                if (exists)
                {
                    final S3Client client = path.getFileSystem().getClient();
                    final String bucketName = path.getFileStore().getBucket().name();
                    final GetObjectRequest request = GetObjectRequest.builder()
                                                                     .bucket(bucketName)
                                                                     .key(key)
                                                                     .build();

                    try (InputStream byteStream = client.getObject(request))
                    {
                        Files.copy(byteStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                    }

                }

                seekable = Files.newByteChannel(tempFile, seekOptions);

                removeTempFile = false;
            }
            finally
            {
                if (removeTempFile)
                {
                    Files.deleteIfExists(tempFile);
                }
            }
        }else
        {
            this.tempFile = null;
            this.seekable = Files.newByteChannel(path, seekOptions);
        }
    }

    @Override
    public boolean isOpen()
    {
        return seekable.isOpen();
    }

    @Override
    public void close()
            throws IOException
    {
        try
        {
            if (!seekable.isOpen())
            {
                return;
            }

            seekable.close();

            if (options.contains(StandardOpenOption.DELETE_ON_CLOSE))
            {
                path.getFileSystem().provider().delete(path);

                return;
            }

            if (options.contains(StandardOpenOption.READ) && options.size() == 1)
            {
                return;
            }

            if(this.tempFile != null)
            {
                sync();
            }
        }
        finally
        {
            if(tempFile != null)
            {
                Files.deleteIfExists(tempFile);
            }
        }
    }

    /**
     * try to sync the temp file with the remote s3 path.
     *
     * @throws IOException if the tempFile fails to open a newInputStream
     */
    protected void sync()
            throws IOException
    {
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(tempFile)))
        {
            PutObjectRequest.Builder builder = PutObjectRequest.builder();
            long length = Files.size(tempFile);
            builder.contentLength(length);

            if (path.getFileName() != null)
            {
                builder.contentType(new Tika().detect(stream, path.getFileName().toString()));
            }

            builder.bucket(path.getBucketName());
            builder.key(path.getKey());
            builder.cacheControl(requestCacheControlHeader);

            S3Client client = path.getFileSystem().getClient();

            client.putObject(builder.build(), RequestBody.fromInputStream(stream, length));
        }
    }

    @Override
    public int write(ByteBuffer src)
            throws IOException
    {
        return seekable.write(src);
    }

    @Override
    public SeekableByteChannel truncate(long size)
            throws IOException
    {
        return seekable.truncate(size);
    }

    @Override
    public long size()
            throws IOException
    {
        return seekable.size();
    }

    @Override
    public int read(ByteBuffer dst)
            throws IOException
    {
        return seekable.read(dst);
    }

    @Override
    public SeekableByteChannel position(long newPosition)
            throws IOException
    {
        return seekable.position(newPosition);
    }

    @Override
    public long position()
            throws IOException
    {
        return seekable.position();
    }

}
