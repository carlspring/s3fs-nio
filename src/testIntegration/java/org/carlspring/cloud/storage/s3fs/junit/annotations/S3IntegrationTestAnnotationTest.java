package org.carlspring.cloud.storage.s3fs.junit.annotations;


import org.carlspring.cloud.storage.s3fs.junit.examples.*;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class S3IntegrationTestAnnotationTest
        extends BaseAnnotationTest
{

    @Test
    void testClassLevelAnnotation()
    {
        assertTrue(hasS3Annotation(S3ClassAnnotationIT.class));
        assertTrue(hasS3Annotation(CombinedS3MinioIT.class));

        assertFalse(hasS3Annotation(MinioClassAnnotationIT.class));
        assertFalse(hasS3Annotation(CombinedMinioS3IT.class));

        assertTrue(hasS3Annotation(CombinedIT.class));
    }

    @Test
    void testMethodLevelAnnotation()
            throws NoSuchMethodException
    {
        assertTrue(hasS3Annotation(S3MethodAnnotationIT.class, "testShouldExecuteBecauseOfMethodAnnotation"));
        assertTrue(hasS3Annotation(CombinedIT.class, "testMinioMethodShouldExecuteBecauseOfClassLevelAnnotation"));
        assertTrue(hasS3Annotation(CombinedIT.class, "testS3MethodShouldExecuteBecauseOfClassLevelAnnotation"));
        assertTrue(hasS3Annotation(CombinedS3MinioIT.class, "testS3MethodShouldExecuteBecauseOfClassLevelAnnotation"));
        assertTrue(hasS3Annotation(CombinedMinioS3IT.class, "testS3MethodShouldExecuteBecauseOfMethodLevelAnnotation"));

        assertFalse(hasS3Annotation(MinioMethodAnnotationIT.class, "testShouldExecuteBecauseOfMethodAnnotation"));
    }

}
