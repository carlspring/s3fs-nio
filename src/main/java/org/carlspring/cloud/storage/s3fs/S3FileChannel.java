package org.carlspring.cloud.storage.s3fs;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static java.lang.String.format;

public class S3FileChannel
        extends FileChannel
{

    private final S3Path path;

    private final Set<? extends OpenOption> options;

    private final FileChannel filechannel;

    private final Path tempFile;

    public S3FileChannel(final S3Path path,
                         final Set<? extends OpenOption> options)
            throws IOException
    {
        this.path = path;
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
        String key = path.getKey();
        boolean exists = path.getFileSystem().provider().exists(path);

        if (exists && this.options.contains(StandardOpenOption.CREATE_NEW))
        {
            throw new FileAlreadyExistsException(format("target already exists: %s", path));
        }
        else if (!exists && !this.options.contains(StandardOpenOption.CREATE_NEW) &&
                 !this.options.contains(StandardOpenOption.CREATE))
        {
            throw new NoSuchFileException(format("target not exists: %s", path));
        }

        tempFile = Files.createTempFile("temp-s3-", key.replaceAll("/", "_"));
        boolean removeTempFile = true;
        try
        {
            if (exists)
            {
                try (ResponseInputStream<GetObjectResponse> byteStream = path.getFileSystem()
                                                                             .getClient()
                                                                             .getObject(GetObjectRequest
                                                                                                .builder()
                                                                                                .bucket(path.getFileStore().name())
                                                                                                .key(key).build()))
                {
                    Files.copy(byteStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                }
            }

            Set<? extends OpenOption> fileChannelOptions = new HashSet<>(this.options);
            fileChannelOptions.remove(StandardOpenOption.CREATE_NEW);
            filechannel = FileChannel.open(tempFile, fileChannelOptions);
            removeTempFile = false;
        }
        finally
        {
            if (removeTempFile)
            {
                Files.deleteIfExists(tempFile);
            }
        }
    }

    @Override
    public int read(final ByteBuffer dst)
            throws IOException
    {
        return filechannel.read(dst);
    }

    @Override
    public long read(final ByteBuffer[] dsts,
                     final int offset,
                     final int length)
            throws IOException
    {
        return filechannel.read(dsts, offset, length);
    }

    @Override
    public int write(final ByteBuffer src)
            throws IOException
    {
        return filechannel.write(src);
    }

    @Override
    public long write(final ByteBuffer[] srcs,
                      final int offset,
                      final int length)
            throws IOException
    {
        return filechannel.write(srcs, offset, length);
    }

    @Override
    public long position()
            throws IOException
    {
        return filechannel.position();
    }

    @Override
    public FileChannel position(final long newPosition)
            throws IOException
    {
        return filechannel.position(newPosition);
    }

    @Override
    public long size()
            throws IOException
    {
        return filechannel.size();
    }

    @Override
    public FileChannel truncate(final long size)
            throws IOException
    {
        return filechannel.truncate(size);
    }

    @Override
    public void force(final boolean metaData)
            throws IOException
    {
        filechannel.force(metaData);
    }

    @Override
    public long transferTo(final long position,
                           final long count,
                           WritableByteChannel target)
            throws IOException
    {
        return filechannel.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(final ReadableByteChannel src,
                             final long position,
                             final long count)
            throws IOException
    {
        return filechannel.transferFrom(src, position, count);
    }

    @Override
    public int read(final ByteBuffer dst,
                    final long position)
            throws IOException
    {
        return filechannel.read(dst, position);
    }

    @Override
    public int write(final ByteBuffer src,
                     final long position)
            throws IOException
    {
        return filechannel.write(src, position);
    }

    @Override
    public MappedByteBuffer map(final MapMode mode,
                                final long position,
                                final long size)
            throws IOException
    {
        return filechannel.map(mode, position, size);
    }

    @Override
    public FileLock lock(final long position,
                         final long size,
                         final boolean shared)
            throws IOException
    {
        return filechannel.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(final long position,
                            final long size,
                            final boolean shared)
            throws IOException
    {
        return filechannel.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel()
            throws IOException
    {
        super.close();
        filechannel.close();
        if (!this.options.contains(StandardOpenOption.READ))
        {
            sync();
        }
        Files.deleteIfExists(tempFile);
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
            //TODO: If the temp file is larger than 5 GB then, instead of a putObject, a multi-part upload is needed.
            final PutObjectRequest.Builder builder = PutObjectRequest.builder();
            final long length = Files.size(tempFile);
            builder.bucket(path.getFileStore().name())
                   .key(path.getKey())
                   .contentLength(length)
                   .contentType(new Tika().detect(stream, path.getFileName().toString()));

            final S3Client client = path.getFileSystem().getClient();
            client.putObject(builder.build(), RequestBody.fromInputStream(stream, length));
        }
    }
}
