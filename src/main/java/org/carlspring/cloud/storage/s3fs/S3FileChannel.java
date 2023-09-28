package org.carlspring.cloud.storage.s3fs;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static java.lang.String.format;

public class S3FileChannel
        extends AsynchronousFileChannel
{

    private final Logger logger = LoggerFactory.getLogger(S3FileChannel.class);

    private final S3Path path;

    private final Set<? extends OpenOption> options;

    private final AsynchronousFileChannel fileChannel;

    private Path tempFile = null;

    /**
     * Read write lock.
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Lock for opening and closing. Has to be the write lock, because the state of channel is
     * changing.
     */
    private final Lock openCloseLock = readWriteLock.writeLock();

    /**
     * Lock for writing. This lock only has to be closed, when the {@link #openCloseLock} is locked.
     * Thus, we can use read lock.
     */
    private final Lock writeReadChannelLock = readWriteLock.readLock();

    public S3FileChannel(final S3Path path,
                         final Set<? extends OpenOption> options,
                         final ExecutorService executor,
                         final boolean tempFileRequired)
            throws IOException
    {
        this(path, options, executor, tempFileRequired, new HashMap<>());
    }

    /**
     * Open or creates a file, returning a file channel.
     *
     * @param path             the path open or create.
     * @param options          options specifying how the file is opened.
     * @param executor         the thread pool or null to associate the channel with the default thread pool.
     * @param tempFileRequired true if a temp file wanted, false in case of a in-memory solution option.
     * @throws IOException if an I/O error occurs
     */
    public S3FileChannel(final S3Path path,
                         final Set<? extends OpenOption> options,
                         final ExecutorService executor,
                         final boolean tempFileRequired,
                         final Map<String, String> properties)
            throws IOException
    {
        openCloseLock.lock();

        this.path = path;
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
        String headerCacheControlProperty = path.getFileSystem().getRequestHeaderCacheControlProperty();
        boolean exists = path.getFileSystem().provider().exists(path);
        boolean removeTempFile = false;

        try
        {
            if (!isOpen())
            {
                if (exists && this.options.contains(StandardOpenOption.CREATE_NEW))
                {
                    throw new FileAlreadyExistsException(format("The target already exists: %s", path));
                }
                else if (!exists && !this.options.contains(StandardOpenOption.CREATE_NEW) &&
                         !this.options.contains(StandardOpenOption.CREATE))
                {
                    throw new NoSuchFileException(format("The target does not exist: %s", path));
                }

                final Set<? extends OpenOption> fileChannelOptions = new HashSet<>(this.options);
                fileChannelOptions.remove(StandardOpenOption.CREATE_NEW);

                if (tempFileRequired)
                {
                    final String key = path.getKey();
                    this.tempFile = Files.createTempFile("temp-s3-", key.replaceAll("/", "_"));
                    removeTempFile = true;

                    if (exists)
                    {
                        final S3Client client = path.getFileSystem().getClient();
                        final GetObjectRequest request = GetObjectRequest.builder()
                                                                         .bucket(path.getBucketName())
                                                                         .key(key)
                                                                         .build();
                        try (ResponseInputStream<GetObjectResponse> byteStream = client.getObject(request))
                        {
                            Files.copy(byteStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                        }

                        removeTempFile = false;
                    }

                    this.fileChannel = AsynchronousFileChannel.open(tempFile.toAbsolutePath(),
                                                                    fileChannelOptions,
                                                                    executor);
                }
                else
                {
                    this.tempFile = null;
                    this.fileChannel = AsynchronousFileChannel.open(path, fileChannelOptions, executor);
                }
            }
            else
            {
                throw new FileAlreadyExistsException(format("Tried to open already opened channel for path %s", path));
            }
        }
        finally
        {
            openCloseLock.unlock();

            if (removeTempFile)
            {
                Files.deleteIfExists(tempFile);
            }
        }
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
     *
     * @param dst      The buffer into which bytes are to be transferred.
     * @param position The file position at which the transfer is to begin; must be non-negative.
     * @return a Future representing the pending result of the operation. The Future's get method returns the number
     * of bytes read or -1 if the given position is greater than or equal to the file's size at the time that
     * the read is attempted.
     */
    @Override
    public Future<Integer> read(final ByteBuffer dst,
                                final long position)
    {
        writeReadChannelLock.lock();
        try
        {
            if (isOpen())
            {
                return fileChannel.read(dst, position);
            }
            else
            {
                return CompletableFuture.completedFuture(0);
            }
        }
        finally
        {
            writeReadChannelLock.unlock();
        }
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
     *
     * @param dst        The buffer into which bytes are to be transferred.
     * @param position   The file position at which the transfer is to begin; must be non-negative.
     * @param attachment The object to attach to the I/O operation; can be null
     * @param handler    The handler for consuming the result
     */
    @Override
    public <A> void read(final ByteBuffer dst,
                         final long position,
                         final A attachment,
                         final CompletionHandler<Integer, ? super A> handler)
    {
        writeReadChannelLock.lock();
        try
        {
            if (isOpen())
            {
                fileChannel.read(dst, position, attachment, handler);
            }
        }
        finally
        {
            writeReadChannelLock.unlock();
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer, starting at the given file position.
     * Writing is performed if, and only if, the channel is open.
     *
     * @param src      The buffer from which bytes are to be transferred.
     * @param position The file position at which the transfer is to begin; must be non-negative.
     * @return a Future representing the pending result of the write operation. The Future's get method returns
     * the number of bytes written.
     */
    @Override
    public Future<Integer> write(final ByteBuffer src,
                                 final long position)
    {
        writeReadChannelLock.lock();
        try
        {
            if (isOpen())
            {
                return fileChannel.write(src, position);
            }
            else
            {
                return CompletableFuture.completedFuture(0);
            }
        }
        finally
        {
            writeReadChannelLock.unlock();
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer, starting at the given file position.
     * Writing is performed if, and only if, the channel is open.
     *
     * @param src        The buffer from which bytes are to be transferred.
     * @param position   The file position at which the transfer is to begin; must be non-negative.
     * @param attachment The object to attach to the I/O operation; can be null.
     * @param handler    The handler for consuming the result.
     */
    @Override
    public <A> void write(final ByteBuffer src,
                          final long position,
                          final A attachment,
                          final CompletionHandler<Integer, ? super A> handler)
    {
        writeReadChannelLock.lock();
        try
        {
            if (isOpen())
            {
                fileChannel.write(src, position, attachment, handler);
            }
        }
        finally
        {
            writeReadChannelLock.unlock();
        }
    }

    /**
     * Returns the current size of this channel's file.
     *
     * @return The current size of this channel's file, measured in bytes.
     * @throws IOException If some other I/O error occurs.
     */
    @Override
    public long size()
            throws IOException
    {
        return fileChannel.size();
    }

    /**
     * Truncates this channel's file to the given size.
     * If the given size is less than the file's current size then the file is truncated, discarding any bytes beyond
     * the new end of the file. If the given size is greater than or equal to the file's current size then the file
     * is not modified.
     *
     * @param size The new size, a non-negative byte count.
     * @return This file channel.
     * @throws IOException If some other I/O error occurs.
     */
    @Override
    public AsynchronousFileChannel truncate(final long size)
            throws IOException
    {
        return fileChannel.truncate(size);
    }

    /**
     * Forces any updates to this channel's file to be written to the storage device that contains it.
     * If this channel's file resides on a local storage device then when this method returns it is guaranteed that
     * all changes made to the file since this channel was created, or since this method was last invoked, will have
     * been written to that device. This is useful for ensuring that critical information is not lost in the event of
     * a system crash.
     * <p>
     * Invoking this method may cause an I/O operation to occur even if the channel was only opened for reading.
     * <p>
     * This method is only guaranteed to force changes that were made to this channel's file via the methods defined
     * in this class.
     *
     * @param metaData If true then this method is required to force changes to both the file's content and metadata
     *                 to be written to storage; otherwise, it need only force content changes to be written.
     * @throws IOException If some other I/O error occurs.
     */
    @Override
    public void force(final boolean metaData)
            throws IOException
    {
        fileChannel.force(metaData);
    }

    /**
     * Acquires a lock on the given region of this channel's file.
     * This method initiates an operation to acquire a lock on the given region of this channel's file.
     * The handler parameter is a completion handler that is invoked when the lock is acquired (or the operation fails).
     * The result passed to the completion handler is the resulting FileLock.
     *
     * @param position The position at which the locked region is to start; must be non-negative.
     * @param size     The size of the locked region; must be non-negative, and the sum position + size must be
     *                 non-negative.
     * @param shared   true to request a shared lock, in which case this channel must be open for reading (and possibly
     *                 writing); false to request an exclusive lock, in which case this channel must be open for
     *                 writing (and possibly reading).
     * @return a Future representing the pending result. The Future's get method returns the FileLock on successful
     * completion.
     */
    @Override
    public Future<FileLock> lock(final long position,
                                 final long size,
                                 final boolean shared)
    {
        return this.fileChannel.lock(position, size, shared);
    }

    /**
     * Acquires a lock on the given region of this channel's file.
     * This method initiates an operation to acquire a lock on the given region of this channel's file.
     * The handler parameter is a completion handler that is invoked when the lock is acquired (or the operation fails).
     * The result passed to the completion handler is the resulting FileLock.
     *
     * @param position   The position at which the locked region is to start; must be non-negative.
     * @param size       The size of the locked region; must be non-negative, and the sum position + size must be
     *                   non-negative.
     * @param shared     true to request a shared lock, in which case this channel must be open for reading (and
     *                   possibly writing); false to request an exclusive lock, in which case this channel must be open
     *                   for writing (and possibly reading).
     * @param attachment The object to attach to the I/O operation; can be null.
     * @param handler    The handler for consuming the result.
     */
    @Override
    public <A> void lock(final long position,
                         final long size,
                         final boolean shared,
                         final A attachment,
                         final CompletionHandler<FileLock, ? super A> handler)
    {
        this.fileChannel.lock(position, size, shared, attachment, handler);
    }

    /**
     * Attempts to acquire a lock on the given region of this channel's file.
     * This method does not block. An invocation always returns immediately, either having acquired a lock on the
     * requested region or having failed to do so. If it fails to acquire a lock because an overlapping lock is held
     * by another program then it returns null. If it fails to acquire a lock for any other reason then an appropriate
     * exception is thrown.
     *
     * @param position The position at which the locked region is to start; must be non-negative.
     * @param size     The size of the locked region; must be non-negative, and the sum position + size must be
     *                 non-negative.
     * @param shared   true to request a shared lock, false to request an exclusive lock.
     * @return A lock object representing the newly-acquired lock, or null if the lock could not be acquired because
     * another program holds an overlapping lock.
     * @throws IOException If some other I/O error occurs.
     */
    @Override
    public FileLock tryLock(final long position,
                            final long size,
                            final boolean shared)
            throws IOException
    {
        return this.fileChannel.tryLock(position, size, shared);
    }

    /**
     * Tells whether or not this channel is open.
     *
     * @return true if, and only if, this channel is open.
     */
    @Override
    public boolean isOpen()
    {
        return this.fileChannel != null && this.fileChannel.isOpen();
    }

    /**
     * Closes this channel.
     * After a channel is closed, any further attempt to invoke I/O operations upon it will cause a {@link ClosedChannelException} to be thrown.
     * <p>
     * If this channel is already closed then invoking this method has no effect.
     * <p>
     * This method may be invoked at any time. If some other thread has already invoked it, however, then another invocation will block until the first invocation is complete, after which it will return without effect.
     *
     * @throws IOException If {@link IOException} happens during closing.
     */
    @Override
    public void close()
            throws IOException
    {
        openCloseLock.lock();

        try
        {
            if (isOpen())
            {
                fileChannel.force(true);
                fileChannel.close();
                if (this.tempFile != null && Files.exists(tempFile))
                {
                    if (!this.options.contains(StandardOpenOption.READ))
                    {
                        sync();
                    }

                    Files.delete(tempFile);
                }
            }
            else
            {
                if (logger.isDebugEnabled())
                {
                    logger.info("Tried to close already closed channel for path {}.", path);
                }
            }

        }
        finally
        {
            openCloseLock.unlock();
        }
    }

    /**
     * Tries to sync the temp file with the remote S3 path.
     *
     * @throws IOException if the tempFile fails to open a newInputStream.
     */
    protected void sync()
            throws IOException
    {
        try (InputStream stream = new BufferedInputStream(Files.newInputStream(tempFile)))
        {
            final PutObjectRequest.Builder builder = PutObjectRequest.builder();
            final long length = Files.size(tempFile);
            builder.bucket(path.getBucketName())
                   .key(path.getKey())
                   .contentLength(length)
                   .contentType(new Tika().detect(stream, path.getFileName().toString()));

            final S3Client client = path.getFileSystem().getClient();
            client.putObject(builder.build(), RequestBody.fromInputStream(stream, length));
        }
    }
}
