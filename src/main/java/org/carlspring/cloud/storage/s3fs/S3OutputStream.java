package org.carlspring.cloud.storage.s3fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.http.Header.CONTENT_LENGTH;
import static software.amazon.awssdk.http.Header.CONTENT_TYPE;

/**
 * Writes data directly into an S3 Client object.
 */
public final class S3OutputStream
        extends OutputStream
{

    private static final Logger LOGGER = LoggerFactory.getLogger(S3OutputStream.class);

    /**
     * Minimum part size of a part in a multipart upload: 5 MiB.
     *
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html">Amazon S3 multipart upload limits</a>
     */
    protected static final int MIN_UPLOAD_PART_SIZE = 5 << 20;

    /**
     * Maximum number of parts that may comprise a multipart upload: 10,000.
     *
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html">Amazon S3 multipart upload limits</a>
     */
    protected static final int MAX_ALLOWED_UPLOAD_PARTS = 10_000;

    /**
     * S3 Client API implementation to use.
     */
    private final S3Client s3Client;

    /**
     * ID of the S3 object to store data into.
     */
    private final S3ObjectId objectId;

    /**
     * Amazon S3 storage class to apply to the newly created S3 object, if any.
     */
    private final StorageClass storageClass;

    /**
     * Metadata that will be attached to the stored S3 object.
     */
    private final Map<String, String> metadata;

    /**
     * Indicates if the stream has been closed.
     */
    private volatile boolean closed;

    /**
     * Internal buffer. May be {@code null} if no bytes are buffered.
     */
    private byte[] buffer;

    /**
     * Number of bytes that are currently stored in the internal buffer. If {@code 0}, then {@code buffer} may also be
     * {@code null}.
     */
    private int bufferSize;

    /**
     * If a multipart upload is in progress, holds the ID for it, {@code null} otherwise.
     */
    private String uploadId;

    /**
     * If a multipart upload is in progress, holds the ETags of the uploaded parts, {@code null} otherwise.
     */
    private List<String> partETags;

    private final String requestCacheControlHeader;

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * No special object metadata or storage class will be attached to the object.
     *
     * @param s3Client S3 ClientAPI implementation to use
     * @param objectId ID of the S3 object to store data into
     * @throws NullPointerException if at least one parameter is {@code null}
     */
    public S3OutputStream(final S3Client s3Client,
                          final S3ObjectId objectId)
    {
        this.s3Client = requireNonNull(s3Client);
        this.objectId = requireNonNull(objectId);
        this.metadata = new HashMap<>();
        this.storageClass = null;
        this.requestCacheControlHeader = "";
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * No special object metadata will be attached to the object.
     *
     * @param s3Client     S3 ClientAPI implementation to use
     * @param objectId     ID of the S3 object to store data into
     * @param storageClass S3 Clientstorage class to apply to the newly created S3 object, if any
     * @throws NullPointerException if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final S3Client s3Client,
                          final S3ObjectId objectId,
                          final StorageClass storageClass)
    {
        this.s3Client = requireNonNull(s3Client);
        this.objectId = requireNonNull(objectId);
        this.metadata = new HashMap<>();
        this.storageClass = storageClass;
        this.requestCacheControlHeader = "";
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * The given {@code metadata} will be attached to the written object. No special storage class will be set for the
     * object.
     *
     * @param s3Client S3 ClientAPI to use
     * @param objectId ID of the S3 object to store data into
     * @param metadata metadata to attach to the written object
     * @throws NullPointerException if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final S3Client s3Client,
                          final S3ObjectId objectId,
                          final Map<String, String> metadata)
    {
        this.s3Client = requireNonNull(s3Client);
        this.objectId = requireNonNull(objectId);
        this.storageClass = null;
        this.metadata = new HashMap<>(metadata);
        this.requestCacheControlHeader = "";
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * The given {@code metadata} will be attached to the written object.
     *
     * @param s3Client     S3 ClientAPI to use
     * @param objectId     ID of the S3 object to store data into
     * @param storageClass S3 Client storage class to apply to the newly created S3 object, if any
     * @param metadata     metadata to attach to the written object
     * @throws NullPointerException if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final S3Client s3Client,
                          final S3ObjectId objectId,
                          final StorageClass storageClass,
                          final Map<String, String> metadata)
    {
        this.s3Client = requireNonNull(s3Client);
        this.objectId = requireNonNull(objectId);
        this.storageClass = storageClass;
        this.metadata = new HashMap<>(metadata);
        this.requestCacheControlHeader = "";
    }

    /**
     * Creates a new {@code S3OutputStream} that writes data directly into the S3 object with the given {@code objectId}.
     * The given {@code metadata} will be attached to the written object.
     *
     * @param s3Client     S3 ClientAPI to use
     * @param objectId     ID of the S3 object to store data into
     * @param storageClass S3 Client storage class to apply to the newly created S3 object, if any
     * @param metadata     metadata to attach to the written object
     * @param requestCacheControlHeader Controls
     * @throws NullPointerException if at least one parameter except {@code storageClass} is {@code null}
     */
    public S3OutputStream(final S3Client s3Client,
                          final S3ObjectId objectId,
                          final StorageClass storageClass,
                          final Map<String, String> metadata,
                          final String requestCacheControlHeader)
    {
        this.s3Client = requireNonNull(s3Client);
        this.objectId = requireNonNull(objectId);
        this.storageClass = storageClass;
        this.metadata = new HashMap<>(metadata);
        this.requestCacheControlHeader = requestCacheControlHeader;
    }

    //protected for testing purposes
    protected void setPartETags(final List<String> partETags)
    {
        this.partETags = partETags;
    }

    @Override
    public void write(final int bytes)
            throws IOException
    {
        write(new byte[]{ (byte) bytes });
    }

    @Override
    public void write(final byte[] bytes,
                      final int offset,
                      final int length)
            throws IOException
    {
        if ((offset < 0) || (offset > bytes.length) || (length < 0) || ((offset + length) > bytes.length) ||
            ((offset + length) < 0))
        {
            throw new IndexOutOfBoundsException();
        }

        if (length == 0)
        {
            return;
        }

        if (closed)
        {
            throw new IOException("Already closed");
        }

        synchronized (this)
        {
            if (uploadId != null && partETags.size() >= MAX_ALLOWED_UPLOAD_PARTS)
            {
                throw new IOException("Maximum number of upload parts reached");
            }

            if (length >= MIN_UPLOAD_PART_SIZE || bufferSize + length >= MIN_UPLOAD_PART_SIZE)
            {
                uploadPart((long) bufferSize + (long) length, bufferCombinedWith(bytes, offset, length));
                bufferSize = 0;
            }
            else
            {
                if (buffer == null)
                {
                    buffer = new byte[MIN_UPLOAD_PART_SIZE];
                }

                System.arraycopy(bytes, offset, buffer, bufferSize, length);
                bufferSize += length;
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed)
        {
            return;
        }

        synchronized (this)
        {
            if (uploadId == null)
            {
                putObject(bufferSize, bufferAsStream(), getValueFromMetadata(CONTENT_TYPE));
                buffer = null;
                bufferSize = 0;
            }
            else
            {
                uploadPart(bufferSize, bufferAsStream());
                buffer = null;
                bufferSize = 0;
                completeMultipartUpload();
            }

            closed = true;
        }
    }

    /**
     * Creates a multipart upload and gets the upload id.
     *
     * @return The upload identifier.
     * @throws IOException if S3 client couldn't be contacted for a response, or the client couldn't parse
     *                     the response from S3.
     */
    private CreateMultipartUploadResponse createMultipartUpload()
            throws IOException
    {
        final CreateMultipartUploadRequest.Builder requestBuilder =
                CreateMultipartUploadRequest.builder()
                                            .bucket(objectId.getBucket())
                                            .key(objectId.getKey())
                                            .metadata(metadata);

        if (storageClass != null)
        {
            requestBuilder.storageClass(storageClass.toString());
        }

        try
        {
            return s3Client.createMultipartUpload(requestBuilder.build());
        }
        catch (final SdkException e)
        {
            // S3 client couldn't be contacted for a response, or the client couldn't parse the response from S3.
            throw new IOException("Failed to create S3 client multipart upload", e);
        }
    }

    private void uploadPart(final long contentLength,
                            final InputStream content)
            throws IOException
    {

        if (uploadId == null)
        {
            uploadId = createMultipartUpload().uploadId();
            if (uploadId == null)
            {
                throw new IOException("Failed to get a valid multipart upload ID from S3 Client");
            }

            partETags = new ArrayList<>();
        }

        final int partNumber = partETags.size() + 1;

        final UploadPartRequest request = UploadPartRequest.builder()
                                                           .bucket(objectId.getBucket())
                                                           .key(objectId.getKey())
                                                           .uploadId(uploadId)
                                                           .partNumber(partNumber)
                                                           .contentLength(contentLength)
                                                           .build();

        LOGGER.debug("Uploading part {} with length {} for {} ", partNumber, contentLength, objectId);

        boolean success = false;
        try
        {
            final RequestBody requestBody = RequestBody.fromInputStream(content, contentLength);
            final String partETag = s3Client.uploadPart(request, requestBody).eTag();
            LOGGER.debug("Uploaded part {} with length {} for {}", partETag, contentLength, objectId);
            partETags.add(partETag);

            success = true;
        }
        catch (final SdkException e)
        {
            throw new IOException("Failed to upload multipart data to S3 Client", e);
        }
        finally
        {
            if (!success)
            {
                closed = true;
                abortMultipartUpload();
            }
        }

        if (partNumber >= MAX_ALLOWED_UPLOAD_PARTS)
        {
            close();
        }
    }

    private void abortMultipartUpload()
    {
        LOGGER.debug("Aborting multipart upload {} for {}", uploadId, objectId);
        try
        {
            final AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                                                                                   .bucket(objectId.getBucket())
                                                                                   .key(objectId.getKey())
                                                                                   .uploadId(uploadId)
                                                                                   .build();

            s3Client.abortMultipartUpload(request);
            uploadId = null;
            partETags = null;
        }
        catch (final SdkException e)
        {
            LOGGER.warn("Failed to abort multipart upload {}: {}", uploadId, e.getMessage());
        }
    }

    /**
     * Calls completeMultipartUpload operation to tell S3 to merge all uploaded part and finish the multipart operation.
     *
     * @throws IOException if failed to complete S3 Client multipart upload.
     */
    private void completeMultipartUpload()
            throws IOException
    {
        final int partCount = partETags.size();
        LOGGER.debug("Completing upload to {} consisting of {} parts", objectId, partCount);

        try
        {

            final Collection<CompletedPart> parts = buildParts(partETags);
            final CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                                                                                              .parts(parts)
                                                                                              .build();
            final CompleteMultipartUploadRequest request =
                    CompleteMultipartUploadRequest.builder()
                                                  .bucket(objectId.getBucket())
                                                  .key(objectId.getKey())
                                                  .uploadId(uploadId)
                                                  .multipartUpload(completedMultipartUpload)
                                                  .build();

            s3Client.completeMultipartUpload(request);
        }
        catch (final SdkException e)
        {
            throw new IOException("Failed to complete S3 Client multipart upload", e);
        }

        LOGGER.debug("Completed upload to {} consisting of {} parts", objectId, partCount);

        uploadId = null;
        partETags = null;
    }

    private Collection<CompletedPart> buildParts(final List<String> partETags)
    {
        final AtomicInteger counter = new AtomicInteger(1);
        return partETags.stream()
                        .map(eTag -> CompletedPart.builder().partNumber(counter.getAndIncrement()).eTag(eTag).build())
                        .collect(Collectors.toList());
    }

    private void putObject(final long contentLength,
                           final InputStream content,
                           final String contentType)
            throws IOException
    {

        final Map<String, String> metadataMap = new HashMap<>(this.metadata);
        metadataMap.put(CONTENT_LENGTH, String.valueOf(contentLength));

        final PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                                                                        .bucket(objectId.getBucket())
                                                                        .key(objectId.getKey())
                                                                        .cacheControl(requestCacheControlHeader)
                                                                        .contentLength(contentLength)
                                                                        .contentType(contentType)
                                                                        .metadata(metadataMap);

        if (storageClass != null)
        {
            requestBuilder.storageClass(storageClass);
        }

        try
        {
            final RequestBody requestBody = RequestBody.fromInputStream(content, contentLength);
            s3Client.putObject(requestBuilder.build(), requestBody);
        }
        catch (final SdkException e)
        {
            throw new IOException("Failed to put data into S3 Client object", e);
        }
    }

    private InputStream bufferAsStream()
    {
        if (bufferSize > 0)
        {
            return new ByteArrayInputStream(buffer, 0, bufferSize);
        }

        return new InputStream()
        {
            @Override
            public int read()
            {
                return -1;
            }
        };
    }

    private InputStream bufferCombinedWith(final byte[] bytes,
                                           final int offset,
                                           final int length)
    {
        final ByteArrayInputStream stream = new ByteArrayInputStream(bytes, offset, length);
        if (bufferSize < 1)
        {
            return stream;
        }

        return new SequenceInputStream(new ByteArrayInputStream(buffer, 0, bufferSize), stream);
    }

    private String getValueFromMetadata(final String key)
    {
        if (metadata.containsKey(key))
        {
            return metadata.get(key);
        }

        return null;
    }
}
