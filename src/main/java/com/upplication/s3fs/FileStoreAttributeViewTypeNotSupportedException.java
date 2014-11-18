package com.upplication.s3fs;

import java.nio.file.attribute.FileStoreAttributeView;

public class FileStoreAttributeViewTypeNotSupportedException extends S3FileSystemException {
	private static final long serialVersionUID = 1L;

	public FileStoreAttributeViewTypeNotSupportedException(Class<? extends FileStoreAttributeView> type) {
		super(type.getName() + " is not supported.");
	}
	
}