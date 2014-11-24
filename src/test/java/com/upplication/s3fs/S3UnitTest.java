package com.upplication.s3fs;

import static com.upplication.s3fs.S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS;

import java.net.URI;

import org.junit.BeforeClass;

public class S3UnitTest {
	@BeforeClass
	public static void setProperties() {
		System.setProperty(AMAZON_S3_FACTORY_CLASS, "com.upplication.s3fs.util.AmazonS3MockFactory");
	}

	public static final URI S3_GLOBAL_URI = URI.create("s3:///");
}