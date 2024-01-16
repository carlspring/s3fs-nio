package org.carlspring.cloud.storage.s3fs;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import ch.qos.logback.classic.pattern.TargetLengthBasedClassNameAbbreviator;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import static java.util.UUID.randomUUID;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;

/**
 * This test is meant to hold common test case logic which can be used in general for unit and integration tests.
 */
public abstract class BaseTest
{

    public static final String PR_NUMBER_ENV_VAR = "PR_NUMBER";

    protected static final Object mutex = new Object();

    /**
     * This is a helper method which can be used to generate "base" paths so that the created resources in S3 / MinIO
     * can be easily tracked to the test case class and method that created them. It will try to process the stack trace
     * and find a class which has been annotated with {@link Test} which will be used to form the path.
     *
     * @return abbreviated fully-qualified class name and method (i.e. o.c.c.s.s.u.BaseIntegrationTest/methodName) or
     * <b>null</b> if it can't find .
     */
    protected String getTestBasePath()
    {
        // Filter out noise and leave only `org.carlspring.cloud.storage.s3fs` calls in the stack trace.
        List<StackTraceElement> elements = Arrays.stream(Thread.currentThread().getStackTrace())
                                                 .filter(e -> {
                                                     String basePackage = BaseTest.class.getPackage().getName();
                                                     String packageName = null;

                                                     try
                                                     {
                                                         packageName = Class.forName(e.getClassName())
                                                                            .getPackage()
                                                                            .getName();
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
            String methodName = last.getMethodName();
            try
            {
                Class<?> clazz = Class.forName(className);
                int modifiers = clazz.getModifiers();
                boolean isAbstract = Modifier.isAbstract(modifiers);

                if (!isAbstract)
                {
                    Method method = Arrays.stream(clazz.getDeclaredMethods())
                                          .filter(m -> m.getName().equals(methodName))
                                          .findFirst()
                                          .orElse(null);
                    if(method == null) {
                        throw new NoSuchMethodException(className+"#"+methodName);
                    }
                    boolean hasTestAnnotation = Arrays.stream(method.getDeclaredAnnotations())
                                                      .anyMatch(a -> a.annotationType() == Test.class ||
                                                                     a.annotationType() == ParameterizedTest.class);
                    if (hasTestAnnotation)
                    {
                        // Additional prefix after the class name for better differentiation.
                        String prNumber = System.getenv(PR_NUMBER_ENV_VAR);
                        prNumber = prNumber != null ? "-pr-" + prNumber : "";
                        return abbreviateClassName(className) + prNumber + "/" + methodName;
                    }
                }
            }
            catch (ClassNotFoundException|NoSuchMethodException e)
            {
                e.printStackTrace();
            }

        }

        return null;
    }

    /**
     * @param subPath The path to append
     *
     * @return o.c.c.s.s.u.BaseIntegrationTest/methodName/subPath
     */
    protected String getTestBasePath(String subPath)
    {
        String basePath = getTestBasePath();

        if (basePath != null)
        {
            return basePath + "/" + subPath;
        }

        return null;
    }

    /**
     * @param clazz
     *
     * @return
     */
    protected String getTestBasePath(Class<?> clazz,
                                     String methodName)
    {
        return abbreviateClassName(clazz.getName()) + "/" + methodName;
    }

    /**
     * @return o.c.c.s.s.u.BaseIntegrationTest/methodName/UUID
     */
    protected String getTestBasePathWithUUID()
    {
        return getTestBasePath(randomUUID().toString());
    }

    /**
     * This method can be used for corner cases where {@link BaseTest#getTestBasePathWithUUID} is unable to calculate
     * the value or more flexibility is needed.
     *
     * @param clazz
     * @param methodName
     *
     * @return
     */
    protected String getTestBasePathWithUUID(Class<?> clazz,
                                             String methodName)
    {
        return getTestBasePath(clazz, methodName) + "/" + UUID.randomUUID();
    }

    /**
     * @param fqClassName Fully-qualified class name
     *
     * @return abbreviated version of the fully-qualified class name (i.e. o.c.c.s.s.u.BaseIntegrationTest)
     */
    private String abbreviateClassName(String fqClassName)
    {
        return new TargetLengthBasedClassNameAbbreviator(5).abbreviate(fqClassName);
    }

    protected static FileSystem createNewFileSystem(URI uri)
            throws IOException
    {
        HashMap<String, Object> env = new HashMap<>(EnvironmentBuilder.getRealEnv());
        if(!env.containsKey(S3Factory.REQUEST_HEADER_CACHE_CONTROL)) {
            env.put(S3Factory.REQUEST_HEADER_CACHE_CONTROL, "no-cache");
        }
        return createNewFileSystem(uri, env);
    }

    protected static FileSystem createNewFileSystem(URI uri, HashMap<String, Object> env)
            throws IOException
    {
        if(!env.containsKey(S3Factory.REQUEST_HEADER_CACHE_CONTROL)) {
            env.put(S3Factory.REQUEST_HEADER_CACHE_CONTROL, "no-cache");
        }

        return FileSystems.newFileSystem(uri, env, Thread.currentThread().getContextClassLoader());
    }


    protected static FileSystem provisionFilesystem(URI uri) throws IOException
    {
        return provisionFilesystem(uri, new HashMap<>(EnvironmentBuilder.getRealEnv()));
    }

    protected static FileSystem provisionFilesystem(URI uri, HashMap<String, Object> env) throws IOException
    {
        synchronized (mutex) {
            // TODO: Do we really  need this?
            System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);
            System.clearProperty(ACCESS_KEY);
            System.clearProperty(SECRET_KEY);
            try(FileSystem fileSystem = FileSystems.getFileSystem(uri))
            {
                return fileSystem;
            }
            catch (FileSystemNotFoundException e)
            {
                 return createNewFileSystem(uri, env);
            }
        }
    }


}
