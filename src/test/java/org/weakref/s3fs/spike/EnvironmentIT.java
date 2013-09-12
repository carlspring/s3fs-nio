package org.weakref.s3fs.spike;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;
import org.weakref.s3fs.S3FileSystemProvider;
import org.weakref.s3fs.util.EnvironmentBuilder;

public class EnvironmentIT {

	@Test
	public void couldCreateFileSystem(){
		Map<String, Object> res = EnvironmentBuilder.getRealEnv();
		
		assertNotNull(res);
		assertNotNull(res.get(S3FileSystemProvider.ACCESS_KEY));
		assertNotNull(res.get(S3FileSystemProvider.SECRET_KEY));
	}
}
