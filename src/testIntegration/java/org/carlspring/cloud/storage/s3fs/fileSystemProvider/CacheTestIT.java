package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.BaseIntegrationTest;
import org.carlspring.cloud.storage.s3fs.S3Factory;
import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.util.EnvironmentBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;

@DisabledIfEnvironmentVariable(named = "CI", matches = ".*", disabledReason = "CI runs are slow and unreliable; Run locally.")
public class CacheTestIT
        extends BaseIntegrationTest

{

    private static final Logger logger = LoggerFactory.getLogger(CacheTestIT.class);

    private static final String bucket = EnvironmentBuilder.getBucket();

    private static final URI uriGlobal = EnvironmentBuilder.getS3URI(S3_GLOBAL_URI_IT);

    private static S3FileSystem fs;

    private static S3FileSystemProvider provider;

    private static final List<S3Path> files = Collections.synchronizedList(new ArrayList<>());
    private static final int multiplier = 1;
    private static final int totalFilesToCreate = 1000 * multiplier;
    private static final int createResourceWaitTime = Math.max(0, totalFilesToCreate / 100);
    private static final int maxCacheSize = (int) Math.round(totalFilesToCreate * 1.1); // caching 110% of the total files to prevent "cache trashing"
    private static final AtomicInteger totalFilesAdded = new AtomicInteger(0);
    // Assert the loop executed within ~100 seconds (the normal execution time is around 90 for 1000 files with good connection)
    private static final int expectedTime = 105 * multiplier;

    private static String testBasePathString = getTestBasePath(CacheTestIT.class, "common_cache_test_artifacts");

    @BeforeAll
    public static void prepareS3Resources() throws IOException, InterruptedException
    {
        fs = (S3FileSystem) build();
        provider = fs.provider();

        S3Path rootPath = fs.getPath(bucket, testBasePathString);
        boolean createResources = Files.notExists(rootPath);

        // Create a thread pool with a number of threads equal to the number of available processors
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        // Settings
        int batchSize = 20;

        logger.info("Preparing assets.");

        // Create files
        for (int i = 0; i < totalFilesToCreate; i += batchSize) {
            final int start = i; // Capture the starting index for this batch
            final int end = Math.min(start + batchSize, totalFilesToCreate); // Calculate end index

            executorService.submit(() -> {
                try {
                    for (int j = start; j < end; j++) {
                        int index = totalFilesAdded.incrementAndGet();
                        S3Path file = (S3Path) rootPath.resolve("file-" + index);
                        if(createResources) {
                            Files.write(file, String.valueOf(index).getBytes(), StandardOpenOption.CREATE);
                        }
                        files.add(file);
                    }
                } catch (Exception e) {
                    totalFilesAdded.decrementAndGet();
                    logger.error("Filed to write file.", e);
                }
            });
        }

        // Shutdown the executor service and await termination
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        // Assert the remote file list count is correct.
        // NB: The more S3 objects you put, the longer sleep is necessary, because of eventually consistent replication.
        //     Using no thread.sleep causes the `Files.list(rootPath)` to have slightly inaccurate total count.
        //
        if(createResources) {
            logger.info("Waiting for {} seconds for S3 to replication to finish (eventual consistency)", createResourceWaitTime);
            Thread.sleep(Duration.ofSeconds(createResourceWaitTime).toMillis());
            assertThat(Files.list(rootPath).count()).isEqualTo(totalFilesToCreate);
        }

        fs.getFileAttributesCache().invalidateAll();
        fs.close();
    }

    @BeforeEach
    public void setup()
            throws IOException
    {
        fs = (S3FileSystem) build();
        provider = fs.provider();
    }

    private static FileSystem build()
            throws IOException
    {
        try
        {
            FileSystems.getFileSystem(uriGlobal).close();

            return createNewFileSystem();
        }
        catch (FileSystemNotFoundException e)
        {
            return createNewFileSystem();
        }
    }

    private static FileSystem createNewFileSystem()
            throws IOException
    {
        Map<String, Object> env = new HashMap<>();
        env.putAll(EnvironmentBuilder.getRealEnv());
        env.put(S3Factory.REQUEST_HEADER_CACHE_CONTROL, "max-age=60");
        env.put(S3Factory.CACHE_ATTRIBUTES_TTL, "120000");
        env.put(S3Factory.CACHE_ATTRIBUTES_SIZE, String.valueOf(maxCacheSize));
        env.put(S3Factory.MAX_CONNECTIONS, "200");
        return FileSystems.newFileSystem(uriGlobal, env);
    }

    @Test
    void testScenario001() throws IOException
    {
        /**
         * Create many files in a folder and attempt operations that should use the cache such as `Files.exists`,
         * `Files.isDirectory()`, etc.
         */

        fs.getFileAttributesCache().invalidateAll();

        // Measure start time
        long startTime = System.currentTimeMillis();
        int processedFileCount = 0;
        for (int i = 0; i < totalFilesToCreate; i++)
        {
            // Calling multiple times to ensure we are hitting the cache.
            S3Path path = files.get(i);
            assertThat(Files.exists(path)).withFailMessage(path + " does not exist.").isTrue();
            assertThat(Files.exists(path)).withFailMessage(path + " does not exist.").isTrue();
            assertThat(Files.exists(path)).withFailMessage(path + " does not exist.").isTrue();

            assertThat(Files.isDirectory(path)).withFailMessage(path + " should not be a directory.").isFalse();
            assertThat(Files.isDirectory(path)).withFailMessage(path + " should not be a directory.").isFalse();
            assertThat(Files.isDirectory(path)).withFailMessage(path + " should not be a directory.").isFalse();

            assertThat(Files.isRegularFile(path)).withFailMessage(path + " should be a regular file.").isTrue();
            assertThat(Files.isRegularFile(path)).withFailMessage(path + " should be a regular file.").isTrue();
            assertThat(Files.isRegularFile(path)).withFailMessage(path + " should be a regular file.").isTrue();

            assertThat(Files.notExists(path)).withFailMessage(path + " should exist.").isFalse();
            assertThat(Files.notExists(path)).withFailMessage(path + " should exist.").isFalse();
            assertThat(Files.notExists(path)).withFailMessage(path + " should exist.").isFalse();

            assertThat(Files.getLastModifiedTime(path)).withFailMessage(path + " should not have null lastModifiedTime")
                                                       .isNotNull();
            assertThat(Files.getLastModifiedTime(path)).withFailMessage(path + " should not have null lastModifiedTime")
                                                       .isNotNull();
            assertThat(Files.getLastModifiedTime(path)).withFailMessage(path + " should not have null lastModifiedTime")
                                                       .isNotNull();

            assertThat(Files.size(path)).isGreaterThan(0);
            assertThat(Files.size(path)).isGreaterThan(0);
            assertThat(Files.size(path)).isGreaterThan(0);

            ++processedFileCount;
        }

        // Measure end time
        long endTime = System.currentTimeMillis();
        long elapsedTime = (endTime - startTime) / 1000;

        // Assert the loop executed within ~100 seconds (the normal execution time is around 90 for 1000 files with good connection)
        logger.info("Cache test ended");
        logger.info("Inserted files {} / Processed files: {} / Test case {}", totalFilesAdded.get(), processedFileCount, testBasePathString);
        logger.info("Start time {} / End time {} / Elapsed time {}s", startTime, endTime, elapsedTime);
        assertThat(elapsedTime).isLessThanOrEqualTo(expectedTime); // seconds

    }

    @Test
    void testScenario002() throws IOException
    {
        S3Path rootPath = fs.getPath(bucket, testBasePathString);
        fs.getFileAttributesCache().invalidateAll();

        // Measure start time
        long startTime = System.currentTimeMillis();
        AtomicInteger processedFileCount = new AtomicInteger(0);

        // Reminder: The `.limit()` will not limit the amount of requests sent to AWS.
        //           If the rootPath contains 100k files and the `.limit(1000)` -- it will still fetch the 100k files first.
        try (Stream<Path> stream = Files.list(rootPath).limit(totalFilesAdded.get()))
        {
            stream.filter(path -> path != null &&
                                  Files.exists(path) &&
                                  !Files.isDirectory(path) &&
                                  Files.isRegularFile(path))
                  .forEach(path -> {
                      processedFileCount.incrementAndGet();
                      logger.info("Found file {}", path);
                  });
        }

        // Measure end time
        long endTime = System.currentTimeMillis();
        long elapsedTime = (endTime - startTime) / 1000;

        logger.info("Cache test ended");
        logger.info("Inserted files {} / Processed files: {} / Test case {}", totalFilesAdded.get(), processedFileCount.get(), testBasePathString);
        logger.info("Start time {} / End time {} / Elapsed time {}s", startTime, endTime, elapsedTime);
        assertThat(elapsedTime).isLessThanOrEqualTo(expectedTime);
        assertThat(processedFileCount.get()).isEqualTo(totalFilesToCreate);
    }

}
