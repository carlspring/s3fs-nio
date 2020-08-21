package org.carlspring.cloud.storage.s3fs.spike;

import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;

import java.util.Map;

import org.junit.Test;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.assertNotNull;

public class EnvironmentIT {

    @Test
    public void couldCreateFileSystem() {
        Map<String, Object> res = EnvironmentBuilder.getRealEnv();

        assertNotNull(res);
        assertNotNull(res.get(ACCESS_KEY));
        assertNotNull(res.get(SECRET_KEY));
    }
}
