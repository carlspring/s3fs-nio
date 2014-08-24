package com.upplication.s3fs.spike;

import com.upplication.s3fs.S3FileSystemProvider;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SpecTest {
	@Test
	public void parentOfRelativeSinglePathIsNull(){
		Path path = FileSystems.getDefault().getPath("relativo");
		assertNull(path.getParent());
	}
	
	@Test
	public void installedFileSystemsLoadFromMetaInf() throws IOException{
		List<FileSystemProvider> providers = FileSystemProvider.installedProviders();
		boolean installed = false;
		for (FileSystemProvider prov : providers){
			if (prov instanceof S3FileSystemProvider){
				installed = true;
				return;
			}
		}
		assertTrue(installed);
	}
}
