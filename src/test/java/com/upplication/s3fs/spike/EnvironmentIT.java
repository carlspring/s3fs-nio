package com.upplication.s3fs.spike;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import com.upplication.s3fs.util.EnvironmentBuilder;
import org.junit.Test;

public class EnvironmentIT {

    @Test
    public void couldCreateFileSystem() {
        Map<String, Object> res = EnvironmentBuilder.getRealEnv();

        assertNotNull(res);
        assertNotNull(res.get(ACCESS_KEY));
        assertNotNull(res.get(SECRET_KEY));
    }
}
