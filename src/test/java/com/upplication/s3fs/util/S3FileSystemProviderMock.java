package com.upplication.s3fs.util;

import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.AmazonS3Client;
import com.upplication.s3fs.S3FileSystemProvider;

public class S3FileSystemProviderMock extends S3FileSystemProvider {
	private FileSystem fsMem;
	private AmazonS3ClientMock amazonS3Client;

	
	@Override
	protected AmazonS3Client getAmazonClient(URI uri, Properties props) {
		return getAmazonClientMock();
	}
	
	@Override
	protected Properties loadAmazonProperties() {
		return new Properties();
	}
	
	@Override
	public Path createTempDir() throws IOException {
		return Files.createDirectory(getFsMem().getPath("/"+UUID.randomUUID().toString()));
	}

	protected FileSystem getFsMem() {
		if(fsMem == null)
			try {
				fsMem = MemoryFileSystemBuilder.newLinux().build("basescheme");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		return fsMem;
	}
	
	public AmazonS3ClientMock getAmazonClientMock() {
		if(amazonS3Client == null )
			try {
				amazonS3Client = spy(new AmazonS3ClientMock(getFsMem().getPath("/")));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		return amazonS3Client;
	}
}