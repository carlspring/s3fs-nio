package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class S3SeekableByteChannelTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        fileSystem = FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true,
                              false })
    void constructor(final boolean tempFileRequired)
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = new S3SeekableByteChannel(file1,
                                                                  EnumSet.of(StandardOpenOption.WRITE,
                                                                             StandardOpenOption.READ), tempFileRequired);

        assertNotNull(channel);

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();
    }

    @Test
    void readDontNeedToSyncTempFile()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.READ), true));

        assertNotNull(channel);

        channel.close();

        verify(channel, never()).sync();
    }

    @Test
    void tempFileRequiredFlagToFalseDontNeedToSyncTempFile()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");
        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.READ), false));

        assertNotNull(channel);

        channel.close();

        verify(channel, never()).sync();
    }

    @Test
    void writeNeedToSyncTempFile()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("buck").file("file1");

        S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST).getPath("/buck/file1");

        S3SeekableByteChannel channel = spy(new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE), true));

        channel.write(ByteBuffer.wrap("hoi".getBytes()));
        channel.close();

        verify(channel, times(1)).sync();
    }

    @Test
    void alreadyExists()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileAlreadyExistsException.class, () -> {
            S3ClientMock client = S3MockFactory.getS3ClientMock();
            client.bucket("buck").file("file1");

            S3Path file1 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST)
                                               .getPath("/buck/file1");
            new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW), true);
        });

        assertNotNull(exception);
    }

    @Test
    void brokenNetwork()
    {
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final GetObjectRequest request = GetObjectRequest.builder().bucket("buck").key("file2").build();
        doThrow(new RuntimeException("network broken")).when(client).getObject(request);

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final S3Path file2 = (S3Path) fileSystem.getPath("/buck/file2");

        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ);

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(RuntimeException.class, () -> new S3SeekableByteChannel(file2, options, true));

        assertNotNull(exception);
    }

    @Test
    void tempFileDisappeared()
            throws SecurityException,
                   IllegalArgumentException
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NoSuchFileException.class, () -> {
            S3Path file2 = (S3Path) FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST)
                                               .getPath("/buck/file2");
            S3SeekableByteChannel channel = new S3SeekableByteChannel(file2,
                                                                      EnumSet.of(StandardOpenOption.WRITE,
                                                                                 StandardOpenOption.READ), true);

            Field f = channel.getClass().getDeclaredField("tempFile");
            f.setAccessible(true);

            Path tempFile = (Path) f.get(channel);

            Files.delete(tempFile);

            channel.close();
        });

        assertNotNull(exception);
    }

    @Test
    void writeFileWithReallyLongName() throws IOException {

        //given
        final S3ClientMock client = S3MockFactory.getS3ClientMock();
        final String longDirectoryName = "FuscetellusodiodapibusidfermentumquissuscipitideratEtiamquisquamVestibulumeratnullaullamcorpernecrutrumnonnon";
        final String longFileName = "ummyaceratSedutperspiciatisundeomnisisfasdfasdfasfsafdtenatuserrorsitvoluptatemaccusantiumdoloremquelaudantiumtotamremaperiameaqueipsaq";
        final String bucketName = "buck";
        final String fileName = longDirectoryName + "/" + longFileName;
        client.bucket(bucketName).file(fileName);

        final FileSystem fileSystem = FileSystems.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        final String pathStr = String.format("/%s/%s", bucketName, fileName);
        final S3Path file1 = (S3Path) fileSystem.getPath(pathStr);
        final S3SeekableByteChannel channel = spy(
                new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE), true));
        final String content = "hoi";
        final ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());

        //when
        channel.write(byteBuffer);
        channel.close();

        //then
        verify(channel, times(1)).sync();
    }

}
