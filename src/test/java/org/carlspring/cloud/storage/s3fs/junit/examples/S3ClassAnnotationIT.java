package org.carlspring.cloud.storage.s3fs.junit.examples;

import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertTrue;

@S3IntegrationTest
public class S3ClassAnnotationIT extends BaseIntegrationTest
{

    private static final Logger logger = LoggerFactory.getLogger(S3ClassAnnotationIT.class);

    @Test
    public void testShouldExecuteBecauseOfClassAnnotation()
    {
        logger.debug(new Exception().getStackTrace()[0].getMethodName());
        assertTrue(true);
    }

}
