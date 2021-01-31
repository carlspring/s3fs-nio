package org.carlspring.cloud.storage.s3fs.spike;

import org.carlspring.cloud.storage.s3fs.junit.annotations.MinioIntegrationTest;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;

import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@S3IntegrationTest
@MinioIntegrationTest
class EnvironmentIT
        extends BaseIntegrationTest
{

    @Test
    void couldCreateFileSystem()
    {
        Map<String, String> res = ENVIRONMENT_CONFIGURATION.asMap();

        assertNotNull(res);
        assertNotNull(res.get(ACCESS_KEY));
        assertNotNull(res.get(SECRET_KEY));
    }

}
