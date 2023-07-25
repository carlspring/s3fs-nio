package org.carlspring.cloud.storage.s3fs;

import org.assertj.core.api.Assertions;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.Header;
import software.amazon.awssdk.services.s3.model.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.carlspring.cloud.storage.s3fs.S3OutputStream.MAX_ALLOWED_UPLOAD_PARTS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3OutputStreamTest extends BaseTest
{

    private static final String BUCKET_NAME = "s3OutputStreamTest";

    @Captor
    private ArgumentCaptor<PutObjectRequest> putObjectCaptor;

    @Captor
    private ArgumentCaptor<RequestBody> requestBodyCaptor;

    private static Stream<Arguments> offsetAndLengthForWriteProvider()
    {
        return Stream.of(
                Arguments.of(-1, 0),
                Arguments.of(5, 0),
                Arguments.of(0, -1),
                Arguments.of(0, 1),
                Arguments.of(-1, -1)
        );
    }

    @Test
    void openAndCloseProducesEmptyObject()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);
        final byte[] data = new byte[0];

        //when
        underTest.close();

        //then
        assertThatBytesHaveBeenPut(client, data);
    }

    @Test
    void writeToClosedStreamShouldProduceExceptionThreadSafe()
            throws ExecutionException, InterruptedException, IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);
        final S3ObjectId objectId = S3ObjectId.builder().bucket(BUCKET_NAME).key(key).build();

        final S3OutputStream outputStream = new S3OutputStream(client, objectId);

        // Simulate closing the outputStream from another thread.
        Runnable closeStreamRunnable = () -> {
            try
            {
                outputStream.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
        closeStreamRunnable.run();

        CountDownLatch count = new CountDownLatch(1);
        AtomicBoolean alreadyClosedException = new AtomicBoolean(false);
        Runnable runnable = () -> {
            count.countDown();
            try
            {
                outputStream.write(new byte[0]);
            }
            catch (S3OutputStream.StreamAlreadyClosedException e)
            {
                alreadyClosedException.set(true);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        };

        CompletableFuture[] futures = new CompletableFuture[] {
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
        };
        count.countDown();

        CompletableFuture.allOf(futures).get();
        assertTrue(alreadyClosedException.get());
    }

    @Test
    void closingStreamShouldBeThreadSafe()
            throws ExecutionException, InterruptedException, IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);
        final S3ObjectId objectId = S3ObjectId.builder().bucket(BUCKET_NAME).key(key).build();

        final S3OutputStream outputStream = new S3OutputStream(client, objectId);

        // Simulate closing the outputStream from another thread.
        Runnable closeStreamRunnable = () -> {
            try
            {
                outputStream.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
        closeStreamRunnable.run();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        Runnable runnable = () -> {
            latch.countDown();
            if(outputStream.isClosed()) {
                counter.incrementAndGet();
            }
        };

        CompletableFuture[] futures = new CompletableFuture[] {
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
                CompletableFuture.runAsync(runnable),
        };
        latch.countDown();

        CompletableFuture.allOf(futures).get();
        assertThat(counter).hasValue(5);
    }

    @Test
    void zeroBytesWrittenProduceEmptyObject()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);
        final byte[] data = new byte[0];

        //when
        underTest.write(data);
        underTest.close();

        //then
        assertThatBytesHaveBeenPut(client, data);
    }

    @ParameterizedTest(name = "{index} ==> offset={0}, length={1}")
    @MethodSource("offsetAndLengthForWriteProvider")
    void invalidValuesForOffsetAndLengthProducesIndexOutOfBoundsException(final int offset,
                                                                          final int length)
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);
        final byte[] data = new byte[0];

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IndexOutOfBoundsException.class,
                                                 () -> underTest.write(data, offset, length));

        //then
        assertNotNull(exception);
    }

    @Test
    void closeAndWriteProducesIOException()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int sixMiB = 6 * 1024 * 1024;
        final int threeMiB = 3 * 1024 * 1024;

        final byte[] data = newRandomData(sixMiB);

        //when
        underTest.write(data, 0, sixMiB);
        underTest.close();

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IOException.class,
                                                 () -> underTest.write(data, threeMiB, threeMiB));

        //then
        assertNotNull(exception);
    }

    @Test
    void maxNumberOfUploadPartsReachedProducesIOException()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int sixMiB = 6 * 1024 * 1024;
        final int twelveMiB = 12 * 1024 * 1024;

        final byte[] data = newRandomData(twelveMiB);

        //when
        underTest.write(data, 0, sixMiB);
        underTest.setPartETags(getPartEtagsMaxList());

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IOException.class,
                                                 () -> underTest.write(data, sixMiB, sixMiB));

        //then
        assertNotNull(exception);
    }

    @Test
    void smallDataUsesPutObject()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);
        final byte[] data = newRandomData(64);

        //when
        underTest.write(data);
        underTest.close();

        //then
        assertThatBytesHaveBeenPut(client, data);
    }

    @Test
    void bigDataUsesMultipartUpload()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int sixMiB = 6 * 1024 * 1024;
        final int threeMiB = 3 * 1024 * 1024;

        final byte[] data = newRandomData(sixMiB);

        //when
        underTest.write(data, 0, threeMiB);
        underTest.write(data, threeMiB, threeMiB);
        underTest.close();

        //then
        assertThatBytesHaveBeenUploaded(client, data);
    }

    @Test
    void whenCreateMultipartUploadFailsThenAnExceptionIsThrown()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int eightMiB = 8 * 1024 * 1024;
        final int fourMiB = 4 * 1024 * 1024;

        final byte[] data = newRandomData(eightMiB);

        final SdkException sdkException = SdkException.builder().message("error").build();
        final CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                                                                                 .bucket(BUCKET_NAME)
                                                                                 .key(key)
                                                                                 .metadata(new HashMap<>())
                                                                                 .build();
        doThrow(sdkException).when(client).createMultipartUpload(request);

        //when
        underTest.write(data, 0, fourMiB);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IOException.class,
                                                 () -> underTest.write(data, fourMiB, fourMiB));

        //then
        assertNotNull(exception);
    }

    @Test
    void whenCreateMultipartUploadReturnsNullUploadIdThenAnExceptionIsThrown()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int sixMiB = 6 * 1024 * 1024;
        final int threeMiB = 3 * 1024 * 1024;

        final byte[] data = newRandomData(sixMiB);

        final CreateMultipartUploadResponse response = mock(CreateMultipartUploadResponse.class);
        when(response.uploadId()).thenReturn(null);

        final CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                                                                                 .bucket(BUCKET_NAME)
                                                                                 .key(key)
                                                                                 .metadata(new HashMap<>())
                                                                                 .build();
        doReturn(response).when(client).createMultipartUpload(request);

        //when
        underTest.write(data, 0, threeMiB);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IOException.class,
                                                 () -> underTest.write(data, threeMiB, threeMiB));

        //then
        assertNotNull(exception);
    }

    @Test
    void whenUploadPartFailsThenAnExceptionIsThrown()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int sixMiB = 6 * 1024 * 1024;
        final int threeMiB = 3 * 1024 * 1024;

        final byte[] data = newRandomData(sixMiB);

        final SdkException sdkException = SdkException.builder().message("error").build();
        doThrow(sdkException).when(client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));

        //when
        underTest.write(data, 0, threeMiB);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IOException.class,
                                                 () -> underTest.write(data, threeMiB, threeMiB));

        //then
        assertNotNull(exception);
    }

    @Test
    void whenUploadPartAndAbortMultipartFailsThenAnExceptionIsThrown()
            throws IOException
    {
        //given
        final String key = getTestBasePath() + "/" + randomUUID();
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket(BUCKET_NAME).file(key);

        final S3ObjectId objectId = S3ObjectId.builder()
                                              .bucket(BUCKET_NAME)
                                              .key(key)
                                              .build();

        final S3OutputStream underTest = new S3OutputStream(client, objectId);

        final int sixMiB = 6 * 1024 * 1024;
        final int threeMiB = 3 * 1024 * 1024;

        final byte[] data = newRandomData(sixMiB);

        final SdkException sdkException = SdkException.builder().message("error").build();
        doThrow(sdkException).when(client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        doThrow(sdkException).when(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        //when
        underTest.write(data, 0, threeMiB);

        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(IOException.class,
                                                 () -> underTest.write(data, threeMiB, threeMiB));

        //then
        assertNotNull(exception);
    }

    private void assertThatBytesHaveBeenPut(final S3ClientMock client,
                                            final byte[] data)
            throws IOException
    {
        verify(client, atLeastOnce()).putObject(putObjectCaptor.capture(), requestBodyCaptor.capture());

        final PutObjectRequest putObjectRequest = putObjectCaptor.getValue();
        assertEquals(putObjectRequest.metadata().get(Header.CONTENT_LENGTH), String.valueOf(data.length));

        final byte[] putData;
        try (final InputStream inputStream = requestBodyCaptor.getValue().contentStreamProvider().newStream();
             final DataInputStream dataInputStream = new DataInputStream(inputStream))
        {
            putData = new byte[data.length];
            dataInputStream.readFully(putData);
            assertEquals(inputStream.read(), -1);
        }

        assertArrayEquals(data, putData, "Mismatch between expected content and actual content");
    }

    private void assertThatBytesHaveBeenUploaded(final S3ClientMock client,
                                                 final byte[] data)
    {
        final InOrder inOrder = inOrder(client);

        inOrder.verify(client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        inOrder.verify(client, atLeastOnce()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        inOrder.verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        inOrder.verifyNoMoreInteractions();

        assertArrayEquals(data, client.getUploadedParts(), "Mismatch between expected content and actual content");
    }

    private static byte[] newRandomData(final int size)
    {
        final byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }

    private List<String> getPartEtagsMaxList()
    {
        return Stream.generate(String::new).limit(MAX_ALLOWED_UPLOAD_PARTS).collect(Collectors.toList());
    }

}
