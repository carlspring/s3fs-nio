package com.upplication.s3fs.spike;

import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.util.EnvironmentBuilder;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class EnvironmentIT {

	@Test
	public void couldCreateFileSystem(){
		Map<String, Object> res = EnvironmentBuilder.getRealEnv();
		
		assertNotNull(res);
		assertNotNull(res.get(S3FileSystemProvider.ACCESS_KEY));
		assertNotNull(res.get(S3FileSystemProvider.SECRET_KEY));
	}
}
