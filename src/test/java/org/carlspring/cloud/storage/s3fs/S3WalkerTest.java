package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.MockBucket;
import org.carlspring.cloud.storage.s3fs.util.S3ClientMock;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;
import org.carlspring.cloud.storage.s3fs.util.S3MockFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.reset;

class S3WalkerTest
        extends S3UnitTestBase
{

    static class RegisteringVisitor
            implements FileVisitor<Path>
    {

        List<String> visitOrder = new ArrayList<>();


        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        {
            visitOrder.add("preVisitDirectory(" + dir.toAbsolutePath().toString() + ")");

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
        {
            visitOrder.add("visitFile(" + file.toAbsolutePath().toString() + ")");

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc)
        {
            visitOrder.add("visitFileFailed(" + file.toAbsolutePath().toString() + ", " +
                           exc.getClass().getSimpleName() + "(" + exc.getMessage() + "))");

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc)
        {
            visitOrder.add("postVisitDirectory(" + dir.toAbsolutePath().toString() + ")");

            return FileVisitResult.CONTINUE;
        }

        public List<String> getVisitOrder()
        {
            return visitOrder;
        }
    }

    static class CheckVisitor
            implements FileVisitor<Path>
    {

        private final Iterator<String> iterator;


        public CheckVisitor(Iterator<String> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        {
            assertTrue(iterator.hasNext());
            assertEquals(iterator.next(), "preVisitDirectory(" + dir.toAbsolutePath().toString() + ")");

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
        {
            assertTrue(iterator.hasNext());
            assertEquals(iterator.next(), "visitFile(" + file.toAbsolutePath().toString() + ")");

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc)
        {
            assertTrue(iterator.hasNext());
            assertEquals(iterator.next(),
                         "visitFileFailed(" + file.toAbsolutePath().toString() + ", " + exc.getClass().getSimpleName() +
                         "(" + exc.getMessage() + "))");

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc)
        {
            assertTrue(iterator.hasNext());
            assertEquals(iterator.next(), "postVisitDirectory(" + dir.toAbsolutePath().toString() + ")");

            return FileVisitResult.CONTINUE;
        }

    }

    @BeforeEach
    void setup()
            throws IOException
    {
        FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    void walkFileTree()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();

        MockBucket mockBucket = client.bucket("/tree");
        mockBucket.dir("folder", "folder/subfolder1");
        mockBucket.file("folder/subfolder1/file1.1",
                        "folder/subfolder1/file1.2",
                        "folder/subfolder1/file1.3",
                        "folder/subfolder1/file1.4");
        mockBucket.file("folder/subfolder2/file2.1",
                        "folder/subfolder2/file2.2",
                        "folder/subfolder2/file2.3",
                        "folder/subfolder2/file2.4");

        S3Path folder = (S3Path) Paths.get(URI.create(S3EndpointConstant.S3_GLOBAL_URI_TEST + "tree/folder"));

        reset(client);

        RegisteringVisitor registrar = new RegisteringVisitor();

        Files.walkFileTree(folder, registrar);

        // 14: 2 for folders: one previst and one postvisit and 1 for files
        assertEquals(14, registrar.getVisitOrder().size());

        final Iterator<String> iterator = registrar.getVisitOrder().iterator();

        reset(client);

        Files.walkFileTree(folder, new CheckVisitor(iterator));

        assertFalse(iterator.hasNext(), "Iterator should have been  exhausted.");

        reset(client);

        Iterator<String> iter = registrar.getVisitOrder().iterator();

        Files.walkFileTree(folder, Collections.emptySet(), 20, new CheckVisitor(iter));

        assertFalse(iter.hasNext(), "Iterator should have been  exhausted.");
    }

    @Test
    void walkEmptyFileTree()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("tree").dir("folder");

        S3Path folder = (S3Path) Paths.get(URI.create(S3EndpointConstant.S3_GLOBAL_URI_TEST + "tree/folder"));
        RegisteringVisitor registrar = new RegisteringVisitor();

        reset(client);

        Files.walkFileTree(folder, registrar);

        assertEquals(2, registrar.getVisitOrder().size());

        final Iterator<String> iterator = registrar.getVisitOrder().iterator();

        reset(client);

        Files.walkFileTree(folder, new CheckVisitor(iterator));

        assertFalse(iterator.hasNext(), "Iterator should have been  exhausted.");
    }

    @Test
    void walkLargeFileTree()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();

        MockBucket mockBucket = client.bucket("/tree");

        for (int i = 0; i < 21; i++)
        {
            for (int j = 0; j < 50; j++)
            {
                StringBuilder filename = new StringBuilder("folder/subfolder");

                if (i < 10)
                {
                    filename.append("0");
                }

                filename.append(i);
                filename.append("/file");

                if (j < 10)
                {
                    filename.append("0");
                }

                filename.append(j);

                mockBucket.file(filename.toString());
            }
        }

        S3Path folder = (S3Path) Paths.get(URI.create(S3EndpointConstant.S3_GLOBAL_URI_TEST + "tree/folder"));

        reset(client);

        RegisteringVisitor registrar = new RegisteringVisitor();

        Files.walkFileTree(folder, registrar);

        assertEquals(1094, registrar.getVisitOrder().size());

        final Iterator<String> iterator = registrar.getVisitOrder().iterator();

        reset(client);

        Files.walkFileTree(folder, new CheckVisitor(iterator));

        assertFalse(iterator.hasNext(), "Iterator should have been  exhausted.");
    }

    @Test
    void noSuchElementTreeWalk()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();
        client.bucket("/tree");

        S3Path folder = (S3Path) Paths.get(URI.create(S3EndpointConstant.S3_GLOBAL_URI_TEST + "tree/folder"));

        reset(client);

        RegisteringVisitor registrar = new RegisteringVisitor();

        Files.walkFileTree(folder, registrar);

        assertEquals(1, registrar.getVisitOrder().size());

        final Iterator<String> iterator = registrar.getVisitOrder().iterator();

        reset(client);

        Files.walkFileTree(folder, new CheckVisitor(iterator));

        assertFalse(iterator.hasNext(), "Iterator should have been  exhausted.");
    }

    @Test
    void skippingWalk()
            throws IOException
    {
        S3ClientMock client = S3MockFactory.getS3ClientMock();

        MockBucket mockBucket = client.bucket("/tree");
        mockBucket.dir("folder", "folder/subfolder1");
        mockBucket.file("folder/subfolder1/file1.1", "folder/subfolder1/file1.2");
        mockBucket.file("folder/subfolder2/file2.1", "folder/subfolder2/file2.2");
        mockBucket.file("folder/subfolder3/file3.1", "folder/subfolder3/file3.2");
        mockBucket.file("folder/subfolder4/file4.1", "folder/subfolder4/file4.2");

        S3Path folder = (S3Path) Paths.get(URI.create(S3EndpointConstant.S3_GLOBAL_URI_TEST + "tree/folder"));

        reset(client);

        final List<String> visitation = new ArrayList<>();

        FileVisitor<Path> visitor = new FileVisitor<Path>()
        {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
            {
                String fileName = dir.getFileName().toString();
                if (fileName.equals("subfolder2"))
                {
                    return FileVisitResult.SKIP_SUBTREE;
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            {
                String name = file.getFileName().toString();

                visitation.add(name);

                if (name.equals("file3.1"))
                {
                    return FileVisitResult.SKIP_SIBLINGS;
                }

                if (name.equals("file4.1"))
                {
                    return FileVisitResult.TERMINATE;
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
            {
                return FileVisitResult.CONTINUE;
            }

        };

        Files.walkFileTree(folder, visitor);

        assertEquals(Arrays.asList("file1.1", "file1.2", "file3.1", "file4.1"), visitation);

        visitation.clear();

        Files.walkFileTree(folder, visitor);

        assertEquals(Arrays.asList("file1.1", "file1.2", "file3.1", "file4.1"), visitation);
    }

}
