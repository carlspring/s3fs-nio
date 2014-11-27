package com.upplication.s3fs;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import org.junit.Test;

import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;

public class S3SeekableByteChannelTest extends S3UnitTest {

	@Test
	public void constructor() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		Path mocket = client.addBucket("buck");
		client.addFile(mocket, "file1");
		
		S3Path file1 = (S3Path) FileSystems.getFileSystem(S3_GLOBAL_URI).getPath("/buck/file1");
		S3SeekableByteChannel channel = new S3SeekableByteChannel(file1, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ), file1.getFileStore());
		assertNotNull(channel);
		channel.write(ByteBuffer.wrap("hoi".getBytes()));
		channel.close();
	}
	
	@SuppressWarnings("resource")
	@Test(expected=RuntimeException.class)
	public void brokenNetwork() throws IOException {
		AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
		doThrow(new RuntimeException("network broken")).when(client).getObject("buck", "file2");
		
		S3Path file2 = (S3Path) FileSystems.getFileSystem(S3_GLOBAL_URI).getPath("/buck/file2");
		new S3SeekableByteChannel(file2, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ), file2.getFileStore());
	}
}