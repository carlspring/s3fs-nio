package org.carlspring.cloud.storage.s3fs.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.StringUtils;
import static java.util.Arrays.asList;
import static software.amazon.awssdk.http.HttpStatusCode.NOT_FOUND;

public class S3ClientMock
        implements S3Client
{

    private final static Logger LOGGER = LoggerFactory.getLogger(S3ClientMock.class);

    /**
     * max elements amazon aws
     */
    private static final int LIMIT_AWS_MAX_ELEMENTS = 1000;

    private final Path base;

    private final Owner defaultOwner;

    private final Map<String, Owner> bucketOwners;

    public S3ClientMock(final Path base)
    {
        this.base = base;
        this.defaultOwner = Owner.builder().id("1").displayName("Mock").build();
        this.bucketOwners = new HashMap<>();
    }

    public String serviceName()
    {
        return null;
    }

    @Override
    public void close()
    {
        try
        {
            Files.walkFileTree(base, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult postVisitDirectory(Path dir,
                                                          IOException exc)
                        throws IOException
                {
                    if (dir != base)
                    {
                        Files.delete(dir);
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file,
                                                 BasicFileAttributes attrs)
                        throws IOException
                {
                    Files.delete(file);

                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (final IOException e)
        {
            LOGGER.error("Exception while closing the client base path", e);
        }
    }

    void addFile(final Path bucket,
                 String fileName,
                 final byte[] content)
            throws IOException
    {
        addFile(bucket, fileName, content, new FileAttribute<?>[0]);
    }

    void addFile(final Path bucket,
                 String fileName,
                 final byte[] content,
                 final FileAttribute<?>... attrs)
            throws IOException
    {
        if (fileName.endsWith("/"))
        {
            fileName = fileName.substring(0, fileName.length() - 1);
        }

        Path file = Files.createFile(bucket.resolve(fileName.replaceAll("/", "%2F")), attrs);

        try (OutputStream outputStream = Files.newOutputStream(file))
        {
            outputStream.write(content);
        }
    }

    void addDirectory(final Path bucket,
                      String directoryName)
            throws IOException
    {
        if (!directoryName.endsWith("/"))
        {
            directoryName += "/";
        }

        final String encodedDirectory = directoryName.replaceAll("/", "%2F");
        Files.createFile(bucket.resolve(encodedDirectory));
    }

    public MockBucket bucket(final String bucketName)
            throws IOException
    {
        final Path bucketPath = base.resolve(bucketName);
        return new MockBucket(this, Files.createDirectories(bucketPath));
    }

    public Path bucket(final String bucketName,
                       final Owner owner)
            throws IOException
    {
        bucketOwners.put(bucketName, owner);

        return Files.createDirectories(base.resolve(bucketName));
    }

    @Override
    public CopyObjectResponse copyObject(final CopyObjectRequest copyObjectRequest)
            throws AwsServiceException, SdkClientException
    {
        final String source = copyObjectRequest.copySource();
        final String[] sources = source.split("%2F", 2);
        try
        {
            final String sourceBucketName = decode(sources[0]);
            final String sourceKey = decode(sources[1]);

            final String destinationBucketName = copyObjectRequest.destinationBucket();
            final String destinationKey = copyObjectRequest.destinationKey();

            final Path src = find(sourceBucketName, sourceKey);
            if (src != null && Files.exists(src))
            {
                final Path bucket = find(destinationBucketName);
                final Path dest = bucket.resolve(destinationKey.replaceAll("/", "%2F"));

                try
                {
                    Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
                }
                catch (IOException e)
                {
                    throw AwsServiceException.create("Problem copying mock objects: ", e);
                }

                return CopyObjectResponse.builder().build();
            }

        }
        catch (final UnsupportedEncodingException e)
        {
            LOGGER.error(e.getMessage());
        }

        throw AwsServiceException.builder().message("object source not found").build();
    }

    @Override
    public CreateBucketResponse createBucket(final CreateBucketRequest createBucketRequest)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = createBucketRequest.bucket();
        Path element = null;
        try
        {
            final String name = bucketName.replaceAll("//", "");

            element = base.resolve(name);
            Files.createDirectories(element);

            return CreateBucketResponse.builder().build();
        }
        catch (final IOException e)
        {
            throw SdkException.create("Error while creating the directory: " + element, e);
        }
    }

    @Override
    public DeleteObjectResponse deleteObject(final DeleteObjectRequest deleteObjectRequest)
            throws AwsServiceException, SdkClientException
    {
        boolean deleteMarker;
        final String bucketName = deleteObjectRequest.bucket();
        final String key = deleteObjectRequest.key();

        final Path bucket = find(bucketName);
        Path resolve = bucket.resolve(key);

        try
        {
            deleteMarker = Files.deleteIfExists(resolve);
        }
        catch (final IOException e)
        {
            throw AwsServiceException.create("Problem deleting mock object: ", e);
        }

        if(!deleteMarker)
        {
            resolve = bucket.resolve(key.replaceAll("/", "%2F"));
            try
            {
                deleteMarker = Files.deleteIfExists(resolve);
            }
            catch (final IOException e)
            {
                throw AwsServiceException.create("Problem deleting mock object: ", e);
            }
        }

        return DeleteObjectResponse.builder().deleteMarker(deleteMarker).build();
    }

    @Override
    public GetBucketLocationResponse getBucketLocation(final GetBucketLocationRequest getBucketLocationRequest) throws AwsServiceException, SdkClientException, S3Exception {
        throw new UnsupportedOperationException();
    }

    private String decode(final String value)
            throws UnsupportedEncodingException
    {
        try
        {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.toString());
        }
        catch (final UnsupportedEncodingException e)
        {
            throw new UnsupportedEncodingException("URL could not be decoded: " + e.getMessage());
        }
    }

    @Override
    public GetBucketAclResponse getBucketAcl(final GetBucketAclRequest getBucketAclRequest)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = getBucketAclRequest.bucket();
        final Path element = find(bucketName);

        return createGetBucketAclResponse(element, bucketName);
    }

    private GetBucketAclResponse createGetBucketAclResponse(final Path element,
                                                            final String bucketName)
    {
        final Owner owner = getOwner(bucketName);
        final Collection<Grant> grants = getGrants(element, owner);

        return GetBucketAclResponse.builder().owner(owner).grants(grants).build();
    }


    @Override
    public HeadBucketResponse headBucket(final HeadBucketRequest headBucketRequest)
    {
        final String bucketName = headBucketRequest.bucket();
        final Path bucket = find(bucketName);

        if (Files.notExists(bucket))
        {
            throw NoSuchBucketException.builder().message("Bucket not found: " + bucketName).build();
        }
        else
        {
            return HeadBucketResponse.builder().build();
        }
    }

    @Override
    public HeadObjectResponse headObject(final HeadObjectRequest headObjectRequest)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = headObjectRequest.bucket();
        final String key = headObjectRequest.key();
        final GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(key).build();

        try
        {
            final GetObjectResponse response;
            final boolean isDirectory = key.endsWith("/") || key.isEmpty();
            if(isDirectory){
                Path path = find(bucketName, key);
                response = getObject(request, path);
            }else{
                final ResponseInputStream<GetObjectResponse> object = getObject(request);
                response = object.response();
            }

            return parse(response);
        }
        catch (final SdkException e)
        {
            throw S3Exception.builder()
                             .message("Resource not available: " + bucketName + "/" + key)
                             .statusCode(NOT_FOUND)
                             .build();
        }

    }

    private S3Object getObject(final String bucketName,
                               final String key)
            throws AwsServiceException
    {
        Path element = find(bucketName, key);

        if (element == null || Files.notExists(element))
        {
            element = find(bucketName, key + "/");
        }

        if (element == null || Files.notExists(element))
        {
            throw S3Exception.builder()
                             .message("Not found with key: " + key)
                             .statusCode(NOT_FOUND)
                             .build();
        }

        try
        {
            return parse(element, find(bucketName)).getS3Object();
        }
        catch (final IOException e)
        {
            throw AwsServiceException.create("Problem getting mock for object: ", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <ReturnT> ReturnT getObject(final GetObjectRequest getObjectRequest,
                                       final ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = getObjectRequest.bucket();
        final String key = getObjectRequest.key();
        Path element = find(bucketName, key);

        if (element == null || Files.notExists(element))
        {
            element = find(bucketName, key + "/");
        }

        S3Object object = null;
        if (element != null && Files.exists(element))
        {
            object = getObject(bucketName, key);
            if (StringUtils.equals(object.key(), key))
            {
                final GetObjectResponse response = buildGetObjectResponse(object);

                final boolean isDirectory = key.endsWith("/") || key.isEmpty();
                if(!isDirectory)
                {
                    final InputStream inputStream = getInputStream(false, element);
                    final AbortableInputStream abortableInputStream = AbortableInputStream.create(inputStream);
                    try
                    {
                        return responseTransformer.transform(response, abortableInputStream);
                    }
                    catch (final Exception e)
                    {
                        throw SdkException.create("Error while transforming the response: " + response, e);
                    }
                }

                return (ReturnT) response;
            }
        }

        throw NoSuchKeyException.builder()
                                .message("Key not found: " + (object != null ? object.key() : key))
                                .statusCode(NOT_FOUND)
                                .build();
    }

    private GetObjectResponse buildGetObjectResponse(final S3Object object)
    {
        return GetObjectResponse.builder()
                                .lastModified(object.lastModified())
                                .eTag(object.eTag())
                                .contentLength(object.size())
                                .storageClass(object.storageClassAsString())
                                .build();
    }

    @Override
    public GetObjectAclResponse getObjectAcl(final GetObjectAclRequest getObjectAclRequest)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = getObjectAclRequest.bucket();
        final String key = getObjectAclRequest.key();
        Path element = find(bucketName, key);

        if (element == null || Files.notExists(element))
        {
            element = find(bucketName, key + "/");
        }

        if (element == null || Files.notExists(element))
        {
            throw NoSuchKeyException.builder()
                                    .message("Key not found: " + key)
                                    .statusCode(NOT_FOUND)
                                    .build();
        }

        return createGetObjectAclResponse(element, bucketName);
    }

    @Override
    public ListBucketsResponse listBuckets(final ListBucketsRequest listBucketsRequest)
            throws AwsServiceException, SdkClientException
    {

        final List<Bucket> buckets = new ArrayList<>();
        Owner owner = null;
        try
        {
            for (final Path path : Files.newDirectoryStream(base))
            {
                final String bucketName = path.getFileName().toString();
                owner = getOwner(bucketName);

                final Instant creationTime = Files.readAttributes(path,
                                                                  BasicFileAttributes.class).creationTime().toInstant();
                final Bucket bucket = Bucket.builder()
                                            .name(bucketName)
                                            .creationDate(creationTime)
                                            .build();

                buckets.add(bucket);
            }
        }
        catch (final IOException e)
        {
            throw SdkException.create("Error while opening the directory: " + base, e);
        }

        return ListBucketsResponse.builder().buckets(buckets).owner(owner).build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(final ListObjectsV2Request request)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = request.bucket();
        final String prefix = request.prefix();
        final String marker = request.continuationToken();
        final String delimiter = request.delimiter();

        final Path bucket = find(bucketName);
        final TreeMap<String, S3Element> elements = new TreeMap<>();

        try
        {
            for (final Path path : Files.newDirectoryStream(bucket))
            {
                final S3Element element = parse(path, bucket);

                if (!elements.containsKey(element.getS3Object().key()))
                {
                    elements.put(element.getS3Object().key(), element);
                }
            }
        }
        catch (final IOException e)
        {
            throw SdkException.create("Error while opening the directory: " + bucket, e);
        }

        final Iterator<S3Element> iterator = elements.values().iterator();

        int i = 0;
        final List<S3Object> objects = new ArrayList<>();
        final List<CommonPrefix> commonPrefixes = new ArrayList<>();
        boolean waitForMarker = StringUtils.isNotBlank(marker);

        while (iterator.hasNext())
        {
            final S3Element element = iterator.next();
            final String key = element.getS3Object().key();
            if (key.equals("/"))
            {
                continue;
            }

            if (waitForMarker)
            {
                waitForMarker = !key.startsWith(marker);
                if (waitForMarker)
                {
                    continue;
                }
            }

            if (prefix != null && key.startsWith(prefix))
            {
                final int beginIndex = key.indexOf(prefix) + prefix.length();
                final String rest = key.substring(beginIndex);

                if (StringUtils.isNotBlank(delimiter) && rest.contains(delimiter))
                {
                    final String substring = key.substring(0, beginIndex + rest.indexOf(delimiter));
                    final CommonPrefix commonPrefix = CommonPrefix.builder().prefix(substring).build();

                    if (!commonPrefixes.contains(commonPrefix))
                    {
                        commonPrefixes.add(commonPrefix);
                    }

                    continue;
                }

                final S3Object object = element.getS3Object();
                objects.add(object);

                if (i + 1 == LIMIT_AWS_MAX_ELEMENTS && iterator.hasNext())
                {
                    return ListObjectsV2Response.builder()
                                                .isTruncated(true)
                                                .contents(objects)
                                                .name(bucketName)
                                                .prefix(prefix)
                                                .delimiter(delimiter)
                                                .commonPrefixes(commonPrefixes)
                                                .nextContinuationToken(iterator.next().getS3Object().key())
                                                .build();
                }

                i++;
            }
        }

        objects.sort(Comparator.comparing(S3Object::key));

        return ListObjectsV2Response.builder()
                                    .isTruncated(false)
                                    .contents(objects)
                                    .name(bucketName)
                                    .prefix(prefix)
                                    .delimiter(delimiter)
                                    .commonPrefixes(commonPrefixes)
                                    .build();
    }

    private GetObjectAclResponse createGetObjectAclResponse(final Path element,
                                                            final String bucketName)
    {
        final Owner owner = getOwner(bucketName);
        final Collection<Grant> grants = getGrants(element, owner);

        return GetObjectAclResponse.builder().owner(owner).grants(grants).build();
    }

    private Path find(final String bucketName,
                      final String key)
    {
        final Path bucket = find(bucketName);

        if (Files.notExists(bucket))
        {
            return null;
        }

        try
        {
            final String fileKey = key.replaceAll("/", "%2F");
            final List<Path> matches = new ArrayList<>();

            Files.walkFileTree(bucket, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult preVisitDirectory(Path dir,
                                                         BasicFileAttributes attrs)
                {
                    String relativize = bucket.relativize(dir).toString();

                    if (relativize.equals(fileKey))
                    {
                        matches.add(dir);
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file,
                                                 BasicFileAttributes attrs)
                {
                    String relativize = bucket.relativize(file).toString();

                    if (relativize.equals(fileKey))
                    {
                        matches.add(file);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            if (!matches.isEmpty())
            {
                return matches.iterator().next();
            }
        }
        catch (final IOException e)
        {
            throw AwsServiceException.create("Problem getting mock S3Element: ", e);
        }

        return null;
    }

    private Path find(final String bucketName)
    {
        return base.resolve(bucketName);
    }

    private S3Element parse(final Path element,
                            final Path bucket)
            throws IOException
    {
        final String bucketName = bucket.getFileName().toString();
        final String key = bucket.relativize(element).toString().replaceAll("%2F", "/");
        boolean isDirectory = key.endsWith("/") || key.isEmpty();

        final BasicFileAttributes attr = Files.readAttributes(element, BasicFileAttributes.class);
        final Instant lastModified = attr.lastAccessTime().toInstant();
        final long size = isDirectory ? 0L : attr.size();
        final ObjectStorageClass storageClass = ObjectStorageClass.STANDARD;
        final Owner owner = getOwner(bucketName);

        final S3Object object = S3Object.builder()
                                        .key(key)
                                        .lastModified(lastModified)
                                        .size(size)
                                        .storageClass(storageClass)
                                        .owner(owner)
                                        .build();

        final InputStream inputStream = getInputStream(isDirectory, element);

        return new S3Element(object, isDirectory, inputStream);
    }

    private InputStream getInputStream(final boolean isDirectory,
                                       final Path element)
    {
        InputStream inputStream = null;
        if (!isDirectory)
        {
            byte[] bytes;
            try
            {
                bytes = Files.readAllBytes(element);
            }
            catch (final IOException e)
            {
                throw SdkException.create("Error while reading the element: " + element, e);
            }

            inputStream = new ByteArrayInputStream(bytes);
        }
        return inputStream;
    }

    /**
     * create the org.carlspring.cloud.storage.s3fs.AccessControlList from a Path
     *
     * @param element Path
     * @param owner   Owner
     * @return GetObjectAclResponse never null
     */
    private GetObjectAclResponse createAclPermission(final Path element,
                                                     final Owner owner)
    {
        final Collection<Grant> grants = getGrants(element, owner);

        return GetObjectAclResponse.builder().owner(owner).grants(grants).build();
    }

    private Collection<Grant> getGrants(final Path element,
                                        final Owner owner)
    {
        final Grantee grantee = Grantee.builder().type(owner.id()).id(owner.id()).build();

        final Set<Permission> permissions = new HashSet<>();

        try
        {
            final Set<PosixFilePermission> posixFilePermissions = Files.readAttributes(element,
                                                                                       PosixFileAttributes.class).permissions();
            for (PosixFilePermission posixFilePermission : posixFilePermissions)
            {
                switch (posixFilePermission)
                {
                    case GROUP_READ:
                    case OTHERS_READ:
                    case OWNER_READ:
                        permissions.add(Permission.READ);
                        break;
                    case OWNER_WRITE:
                    case GROUP_WRITE:
                    case OTHERS_WRITE:
                        permissions.add(Permission.WRITE);
                        break;
                    case OWNER_EXECUTE:
                    case GROUP_EXECUTE:
                    case OTHERS_EXECUTE:
                        permissions.add(Permission.WRITE_ACP);
                        permissions.add(Permission.READ_ACP);
                        break;

                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return permissions.stream()
                          .map(permission -> Grant.builder().grantee(grantee).permission(permission).build())
                          .collect(Collectors.toList());
    }

    private GetObjectAclResponse createAllPermission(final String bucketName)
    {
        final Owner owner = getOwner(bucketName);
        final Grantee grantee = Grantee.builder().type(owner.id()).id(owner.id()).build();
        final Grant grantFullControl = Grant.builder().grantee(grantee).permission(
                Permission.FULL_CONTROL.name()).build();
        final Grant grantRead = Grant.builder().grantee(grantee).permission(Permission.READ).build();
        final Grant grantWrite = Grant.builder().grantee(grantee).permission(Permission.WRITE).build();
        final Collection<Grant> grants = asList(grantFullControl, grantRead, grantWrite);

        return GetObjectAclResponse.builder().owner(owner).grants(grants).build();
    }

    private Owner getOwner(final String bucketName)
    {
        if (!bucketOwners.containsKey(bucketName))
        {
            return defaultOwner;
        }

        return bucketOwners.get(bucketName);
    }

    private HeadObjectResponse parse(final GetObjectResponse object)
    {
        return HeadObjectResponse.builder()
                                 .lastModified(object.lastModified())
                                 .eTag(object.eTag())
                                 .contentLength(object.contentLength())
                                 .storageClass(object.storageClassAsString())
                                 .build();
    }

    @Override
    public PutObjectResponse putObject(final PutObjectRequest putObjectRequest,
                                       final RequestBody requestBody)
            throws AwsServiceException, SdkClientException
    {
        final String bucketName = putObjectRequest.bucket();
        final String key = putObjectRequest.key();
        final InputStream inputStream = requestBody.contentStreamProvider().newStream();

        final S3Element element = parse(inputStream, bucketName, key);

        persist(bucketName, element);

        final String eTag = "3a5c8b1ad448bca04584ecb55b836264";
        return PutObjectResponse.builder().eTag(eTag).build();
    }

    private S3Element parse(final InputStream inputStream,
                            final String bucketName,
                            final String key)
    {
        final S3Object.Builder builder = S3Object.builder();
        builder.key(key);

        final Owner owner = getOwner(bucketName);
        builder.owner(owner);

        try
        {
            builder.lastModified(Instant.now());
            builder.size((long) inputStream.available());

            return new S3Element(builder.build(), false, inputStream);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("the inputStream is closed", e);
        }
    }

    /**
     * store in the memory map
     *
     * @param bucketName bucket where persist
     * @param element
     */
    private void persist(final String bucketName,
                         final S3Element element)
    {
        final Path bucket = find(bucketName);

        final String key = element.getS3Object().key().replaceAll("/", "%2F");

        final Path resolve = bucket.resolve(key);

        if (Files.exists(resolve))
        {
            try
            {
                Files.delete(resolve);
            }
            catch (final IOException ignored)
            {
                // ignore
            }
        }

        try
        {
            Files.createFile(resolve);

            final InputStream inputStream = element.getInputStream();
            if (inputStream != null)
            {
                byte[] byteArray = IOUtils.toByteArray(inputStream);

                Files.write(resolve, byteArray);
            }
        }
        catch (final IOException e)
        {
            throw AwsServiceException.create("Problem creating mock element: ", e);
        }
    }

    public static class S3Element
    {

        private S3Object s3Object;

        private boolean directory;

        private InputStream inputStream;

        public S3Element(final S3Object s3Object,
                         final boolean directory,
                         final InputStream inputStream)
        {
            this.s3Object = s3Object;
            this.directory = directory;
            this.inputStream = inputStream;
        }

        public S3Object getS3Object()
        {
            return s3Object;
        }

        public void setS3Object(final S3Object s3Object)
        {
            this.s3Object = s3Object;
        }

        public boolean isDirectory()
        {
            return directory;
        }

        public void setDirectory(final boolean directory)
        {
            this.directory = directory;
        }

        public InputStream getInputStream()
        {
            return inputStream;
        }

        public void setInputStream(InputStream inputStream)
        {
            this.inputStream = inputStream;
        }

        @Override
        public boolean equals(final Object object)
        {
            if (object == null)
            {
                return false;
            }

            if (object instanceof S3Element)
            {
                S3Element elem = (S3Element) object;
                // only is the same if keys are not null and they are the same
                return elem.getS3Object() != null && this.getS3Object() != null &&
                       elem.getS3Object().key() != null &&
                       elem.getS3Object().key().equals(this.getS3Object().key());
            }

            return false;
        }

        @Override
        public int hashCode()
        {
            return 31 * (s3Object != null && s3Object.key() != null ? s3Object.key().hashCode() : 0);
        }
    }
}
