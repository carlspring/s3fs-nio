package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.junit.annotations.MinioIntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.carlspring.cloud.storage.s3fs.util.MinioContainer;

import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.carlspring.cloud.storage.s3fs.S3Factory.*;

/**
 * This abstract class holds common integration test logic.
 */
public abstract class BaseIntegrationTest extends BaseTest
{

    private static MinioContainer minioContainer;

    static
    {
        if (isMinioEnv())
        {
            try
            {
                final String accessKey = (String) EnvironmentBuilder.getRealEnv().get(ACCESS_KEY);
                final String secretKey = (String) EnvironmentBuilder.getRealEnv().get(SECRET_KEY);
                final String bucketName = (String) EnvironmentBuilder.getRealEnv().get(EnvironmentBuilder.BUCKET_NAME_KEY);
                minioContainer = new MinioContainer(accessKey, secretKey, bucketName);
                minioContainer.start();

                // Minio is using HTTP.
                System.setProperty(PROTOCOL, "http");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public static boolean isMinioEnv()
    {
        String integrationTestType = System.getProperty("running.it");
        boolean propertyWins = integrationTestType != null && integrationTestType.equalsIgnoreCase("minio");

        // For when you are running via IDEs.
        if(!propertyWins) {
            // Filter out noise and leave only `org.carlspring.cloud.storage.s3fs` calls in the stack trace.
            List<StackTraceElement> elements = Arrays.stream(Thread.currentThread().getStackTrace())
                                                     .filter(e -> {
                                                         String basePackage = BaseTest.class.getPackage().getName();
                                                         String packageName = null;
                                                         try
                                                         {
                                                             packageName = Class.forName(e.getClassName()).getPackage().getName();
                                                         }
                                                         catch (ClassNotFoundException classNotFoundException)
                                                         {
                                                             // This is commented out, because sometimes when running
                                                             // in debug mode from an IDE the stacktrace will contain
                                                             // gradle classes which cannot be found.
                                                             //classNotFoundException.printStackTrace();
                                                         }
                                                         return packageName != null && packageName.startsWith(basePackage);
                                                     })
                                                     .collect(Collectors.toList());

            if (elements.size() > 0)
            {
                StackTraceElement last = elements.get(elements.size() - 1);
                String className = last.getClassName();
                try
                {
                    Class<?> clazz = Class.forName(className);
                    int modifiers = clazz.getModifiers();
                    boolean isAbstract = Modifier.isAbstract(modifiers);

                    if (!isAbstract)
                    {
                        return Arrays.stream(clazz.getDeclaredAnnotations())
                                     .anyMatch(a -> a.annotationType() == MinioIntegrationTest.class);
                    }
                }
                catch (ClassNotFoundException e)
                {
                    e.printStackTrace();
                }

            }

            return false;

        }


        return propertyWins;
    }

    public static MinioContainer getMinioContainer()
    {
        return minioContainer;
    }

    public static URI getS3URIForMinioContainer() {
        return EnvironmentBuilder.getS3URI(URI.create("s3://localhost:" + getMinioContainer().getMappedPort(9000) + "/"));
    }

}
