package com.upplication.s3fs.util;

import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3;
import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import com.upplication.s3fs.AmazonS3Factory;

public class AmazonS3MockFactory implements AmazonS3Factory {
	private static FileSystem fsMem;
	private static AmazonS3ClientMock amazonS3Client;

	
	@Override
	public AmazonS3 getAmazonS3(URI uri, Properties props) {
		return getAmazonClientMock();
	}
	
	public static AmazonS3ClientMock getAmazonClientMock() {
		if(amazonS3Client == null )
			try {
				amazonS3Client = spy(new AmazonS3ClientMock(getFsMem().getPath("/")));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		return amazonS3Client;
	}
	
	private static FileSystem getFsMem() {
		if(fsMem == null)
			try {
				fsMem = MemoryFileSystemBuilder.newLinux().build("basescheme");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		return fsMem;
	}
}
