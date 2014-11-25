package com.upplication.s3fs.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.upplication.s3fs.S3Client;

public class AmazonS3ClientMock extends S3Client {
    /**
     * max elements amazon aws
     */
    private static final int LIMIT_AWS_MAX_ELEMENTS = 1000;

	// default owner
	Owner owner = new Owner() {
		private static final long serialVersionUID = 5510838843790352879L;
		{
			setDisplayName("Mock");
			setId("1");
		}
	};

	private Path base;

	public AmazonS3ClientMock(Path base) {
		super(null);
		this.base = base;
	}

    /**
     * list all objects without and return ObjectListing with all elements
     * and with truncated to false
     */
	@Override
	public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws AmazonClientException {
        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(listObjectsRequest.getBucketName());
        objectListing.setPrefix(listObjectsRequest.getPrefix());
        objectListing.setMarker(listObjectsRequest.getMarker());
        objectListing.setDelimiter(listObjectsRequest.getDelimiter());

        final Path bucket = find(listObjectsRequest.getBucketName());
        final Map<String, S3Element> elems = new HashMap<String, AmazonS3ClientMock.S3Element>();
		try {
			Files.walkFileTree(bucket, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
					S3Element element = parse(dir, bucket);
					if(!elems.containsKey(element.getS3Object().getKey()))
						elems.put(element.getS3Object().getKey(), element);
					return super.preVisitDirectory(dir, attrs);
				}
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					S3Element element = parse(file, bucket);
					if(!elems.containsKey(element.getS3Object().getKey()))
						elems.put(element.getS3Object().getKey(), element);
					return super.visitFile(file, attrs);
				}
			});
			for (Path elem : Files.newDirectoryStream(bucket)) {
				S3Element element = parse(elem, bucket);
				if(!elems.containsKey(element.getS3Object().getKey()))
					elems.put(element.getS3Object().getKey(), element);
			}
		} catch (IOException e) {
			throw new AmazonClientException(e);
		}
		Iterator<S3Element> iterator = elems.values().iterator();
        int i = 0;
        while(iterator.hasNext()){

            S3Element elem = iterator.next();
            if(elem.getS3Object().getKey().equals("/"))
            	continue;
            // TODO. add delimiter and marker support
            if (listObjectsRequest.getPrefix() != null && elem.getS3Object().getKey().startsWith(listObjectsRequest.getPrefix())) {

                S3ObjectSummary s3ObjectSummary = parseToS3ObjectSummary(elem);
                objectListing.getObjectSummaries().add(s3ObjectSummary);

                if (i + 1 == LIMIT_AWS_MAX_ELEMENTS && iterator.hasNext()){
                    objectListing.setTruncated(true);
                    objectListing.setNextMarker(iterator.next().getS3Object().getKey());
                    return objectListing;
                }
				objectListing.setTruncated(false);

                i++;
            }

        }

		return objectListing;
	}

    @Override
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) {
        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(previousObjectListing.getBucketName());
        objectListing.setPrefix(previousObjectListing.getPrefix());
        objectListing.setMarker(previousObjectListing.getMarker());
        objectListing.setDelimiter(previousObjectListing.getDelimiter());

        if (!previousObjectListing.isTruncated() ||
                previousObjectListing.getNextMarker() == null){
            return objectListing;
        }

        Path bucket = find(previousObjectListing.getBucketName());
        List<S3Element> elems = new ArrayList<AmazonS3ClientMock.S3Element>();
		try {
			for (Path elem : Files.newDirectoryStream(bucket)) {
				elems.add(parse(elem, bucket));
			}
		} catch (IOException e) {
			throw new AmazonClientException(e);
		}
		Iterator<S3Element> iterator = elems.iterator();
		
        int i = 0;
        boolean continueElement = false;

        while (iterator.hasNext()) {

            S3Element elem = iterator.next();

            if (!continueElement && elem.getS3Object().getKey().equals(previousObjectListing.getNextMarker())){
                continueElement = true;
            }

            if (continueElement) {
                // TODO. add delimiter and marker support
                if (previousObjectListing.getPrefix() != null
                        && elem.getS3Object().getKey()
                        .startsWith(previousObjectListing.getPrefix())) {

                    S3ObjectSummary s3ObjectSummary = parseToS3ObjectSummary(elem);
                    objectListing.getObjectSummaries().add(s3ObjectSummary);
                    // max 1000 elements at same time.
                    if (i + 1 == LIMIT_AWS_MAX_ELEMENTS && iterator.hasNext()){
                        objectListing.setTruncated(true);
                        objectListing.setNextMarker(iterator.next().getS3Object().getKey());
                        return objectListing;
                    }
					objectListing.setTruncated(false);
                    i++;
                }
            }
        }

        return objectListing;
    }

    /**
     * create a new S3ObjectSummary using the S3Element
     * @param elem S3Element to parse
     * @return S3ObjectSummary
     */
    private S3ObjectSummary parseToS3ObjectSummary(S3Element elem) {
        S3Object s3Object = elem.getS3Object();
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setBucketName(s3Object.getBucketName());
        s3ObjectSummary.setKey(s3Object.getKey());
		s3ObjectSummary.setLastModified(objectMetadata.getLastModified());
        s3ObjectSummary.setOwner(owner);
        s3ObjectSummary.setETag(objectMetadata.getETag());
        s3ObjectSummary.setSize(objectMetadata.getContentLength());
        return s3ObjectSummary;
    }

    @Override
	public Owner getS3AccountOwner() throws AmazonClientException {
		return owner;
	}

	@Override
	public List<Bucket> listBuckets() throws AmazonClientException {
		List<Bucket> result = new ArrayList<>();
		try {
			for (Path path : Files.newDirectoryStream(base)) {
				Bucket bucket = new Bucket(path.getFileName().toString());
				bucket.setOwner(getS3AccountOwner());
				bucket.setCreationDate(new Date(Files.readAttributes(path, BasicFileAttributes.class).creationTime().toMillis()));
				result.add(bucket);
			}
		} catch (IOException e) {
			throw new AmazonClientException(e);
		}
		return result;
	}

	@Override
	public Bucket createBucket(CreateBucketRequest createBucketRequest) throws AmazonClientException, AmazonServiceException {
		// TODO Auto-generated method stub
		return super.createBucket(createBucketRequest);
	}
	
	@Override
	public Bucket createBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
		// TODO Auto-generated method stub
		return super.createBucket(bucketName);
	}
	
	@Override
	public AccessControlList getObjectAcl(String bucketName, String key) throws AmazonClientException {
		Path elem = find(bucketName, key);
		if (elem != null) {
			try {
				return parse(elem, find(bucketName)).getPermission();
			} catch (IOException e) {
				throw new AmazonServiceException("Problem getting mock ACL: ", e);
			}
		}
		throw new AmazonServiceException("key not found, " + key);
	}

	@Override
	public AccessControlList getBucketAcl(String bucketName) throws AmazonClientException {
		Path bucket = find(bucketName);
		if (bucket == null) {
			throw new AmazonServiceException("bucket not found, " + bucketName);
		}
		return createAllPermission();
	}

	@Override
	public S3Object getObject(String bucketName, String key) throws AmazonClientException {
        Path result = find(bucketName, key);
        if (result == null || !Files.exists(result)){
            AmazonS3Exception amazonS3Exception = new AmazonS3Exception("not found with key: " + key);
            amazonS3Exception.setStatusCode(404);
            throw amazonS3Exception;
        }
		try {
			return parse(result, find(bucketName)).getS3Object();
		} catch (IOException e) {
			throw new AmazonServiceException("Problem getting Mock Object: ", e);
		}
	}

	@Override
	public PutObjectResult putObject(String bucketName, String key, File file) throws AmazonClientException {
		try {
			ByteArrayInputStream stream = new ByteArrayInputStream(Files.readAllBytes(file.toPath()));
			S3Element elem = parse(stream, bucketName, key);

            persist(bucketName, elem);

			PutObjectResult putObjectResult = new PutObjectResult();
			putObjectResult.setETag("3a5c8b1ad448bca04584ecb55b836264");
			return putObjectResult;
		} catch (IOException e) {
			throw new AmazonServiceException("",e);
		}

	}
	
	@Override
	public PutObjectResult putObject(String bucket, String keyName,
			InputStream inputStream, ObjectMetadata metadata) {
		S3Element elem = parse(inputStream, bucket, keyName);
		
        persist(bucket, elem);

		PutObjectResult putObjectResult = new PutObjectResult();
		putObjectResult.setETag("3a5c8b1ad448bca04584ecb55b836264");
		return putObjectResult;
		
	}

    /**
     * store in the memory map
     * @param bucket bucket where persist
     * @param elem
     */
    private void persist(String bucketName, S3Element elem) {
    	Path bucket = find(bucketName);
    	Path resolve = bucket.resolve(elem.getS3Object().getKey());
    	if(Files.exists(resolve))
			try {
				Files.delete(resolve);
			} catch (IOException e1) {
				// ignore
			}
    	try {
	        if(elem.getS3Object().getKey().endsWith("/"))
	        	Files.createDirectory(resolve);
	        else {
	        	Files.createFile(resolve);
	        	S3ObjectInputStream objectContent = elem.getS3Object().getObjectContent();
	        	Files.write(resolve, IOUtils.toByteArray(objectContent));
	        }
    	} catch (IOException e) {
    		throw new AmazonServiceException("Problem creating mock element: ", e);
    	}
    }

    @Override
	public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws AmazonClientException {

		Path src = find(sourceBucketName, sourceKey);

		if (src != null && Files.exists(src)) {
            Path bucket = find(destinationBucketName);
            Path dest = bucket.resolve(destinationKey);
			try {
				Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				throw new AmazonServiceException("Problem copying mock objects: ", e);
			}

			return new CopyObjectResult();
		}

		throw new AmazonServiceException("object source not found");
	}

	@Override
	public void deleteObject(String bucketName, String key) throws AmazonClientException {
		Path bucket = find(bucketName);
		Path resolve = bucket.resolve(key);
		if(Files.exists(resolve))
			try {
				Files.delete(resolve);
			} catch (IOException e) {
				throw new AmazonServiceException("Problem deleting mock object: ", e);
			}
	}
	
	private S3Element parse(InputStream stream, String bucket, String key) {
		S3Object object = new S3Object();
		object.setBucketName(bucket);
		object.setKey(key);
        byte[] content;
        try {
            content = IOUtils.toByteArray(stream);
        } catch (IOException e) {
            throw new IllegalStateException("the stream is closed", e);
        } finally {
        	try {
				object.close();
			} catch (IOException e) {
				// ignore
			}
        }

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setLastModified(new Date());
        metadata.setContentLength(content.length);

        object.setObjectContent(new ByteArrayInputStream(content));
		object.setObjectMetadata(metadata);
		// TODO: create converter between path permission and s3 permission
		AccessControlList permission = createAllPermission();
		return new S3Element(object, permission, false);
	}
	
	private S3Element parse(Path elem, Path bucket) throws IOException{
		boolean dir = false;
		if (Files.isDirectory(elem)) {
			dir = true;
		}

		S3Object object = new S3Object();
		
		object.setBucketName(bucket.getFileName().toString());
		
		String key = bucket.relativize(elem).toString();
		if (dir) {
			key += "/";
		}		
		object.setKey(key);

		ObjectMetadata metadata = new ObjectMetadata();
		BasicFileAttributes attr = Files.readAttributes(elem, BasicFileAttributes.class);
		metadata.setLastModified(new Date(attr.lastAccessTime().toMillis()));
		if (dir) {
			metadata.setContentLength(0);
			object.setObjectContent(null);
		} else {
			metadata.setContentLength(attr.size());
			object.setObjectContent( new ByteArrayInputStream(Files.readAllBytes(elem)));
		}

		object.setObjectMetadata(metadata);
		// TODO: create converter between path permission and s3 permission
		AccessControlList permission = createAllPermission();

		return new S3Element(object, permission, dir);
	}

	private AccessControlList createAllPermission() {
		AccessControlList res = new AccessControlList();
		res.setOwner(getS3AccountOwner());
		Grantee grant = new Grantee() {
			@Override
			public void setIdentifier(String id) {
				//
			}

			@Override
			public String getTypeIdentifier() {
				return getS3AccountOwner().getId();
			}

			@Override
			public String getIdentifier() {
				return getS3AccountOwner().getId();
			}
		};

		res.grantPermission(grant, Permission.FullControl);
		res.grantPermission(grant, Permission.Read);
		res.grantPermission(grant, Permission.Write);
		return res;
	}

	private Path find(String bucketName, final String key) {
		final Path bucket = find(bucketName);
		if (bucket == null || !Files.exists(bucket)) {
			return null;
		}
		try {
			final List<Path> matches = new ArrayList<Path>();
			Files.walkFileTree(bucket, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
					if(bucket.relativize(dir).toString().equals(key)) {
						matches.add(dir);
					}
					return super.preVisitDirectory(dir, attrs);
				}
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if(bucket.relativize(file).toString().equals(key)) {
						matches.add(file);
					}
					return super.visitFile(file, attrs);
				}
			});
			if(!matches.isEmpty())
				return matches.iterator().next();
		} catch (IOException e) {
			throw new AmazonServiceException("Problem getting mock S3Element: ", e);
		}

		return null;
	}

	private Path find(String bucketName) {
		return base.resolve(bucketName);
	}

	public static class S3Element {

		private S3Object s3Object;
        private boolean directory;
		private AccessControlList permission;

		public S3Element(S3Object s3Object, AccessControlList permission,
				boolean directory) {
			this.s3Object = s3Object;
			this.directory = directory;
			this.permission = permission;
		}

		public S3Object getS3Object() {
			return s3Object;
		}

		public void setS3Object(S3Object s3Object) {
			this.s3Object = s3Object;
		}

		public AccessControlList getPermission() {
			return permission;
		}

		public boolean isDirectory() {
			return directory;
		}

		public void setDirectory(boolean directory) {
			this.directory = directory;
		}

		public void setPermission(AccessControlList permission) {
			this.permission = permission;
		}


        @Override
        public boolean equals(Object object){

            if (object == null){
                return false;
            }

            if (object instanceof S3Element){
                S3Element elem = (S3Element)object;
                // only is the same if bucketname and key are not null and are the same
                if (elem.getS3Object() != null && this.getS3Object() != null &&
                        elem.getS3Object().getBucketName() != null &&
                        elem.getS3Object().getBucketName().equals(this.getS3Object().getBucketName()) &&
                        elem.getS3Object().getKey() != null && elem.getS3Object().getKey().equals(this.getS3Object().getKey())){
                    return true;
                }

                return false;
            }
			return false;
        }

        @Override
        public int hashCode() {
            int result = s3Object != null && s3Object.getBucketName() != null ? s3Object.getBucketName().hashCode() : 0;
            result = 31 * result + (s3Object != null && s3Object.getKey() != null? s3Object.getKey().hashCode() : 0);
            return result;
        }
    }
	
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String key) {
		S3Object object = getObject(bucketName, key);
		if(object.getKey().equals(key))
			return object.getObjectMetadata();
		return null;
	}
	
	@Override
	public boolean doesBucketExist(String bucketName) throws AmazonClientException, AmazonServiceException {
		return Files.exists(base.resolve(bucketName));
	}

	public Path addBucket(String bucketName) throws IOException {
		return Files.createDirectories(base.resolve(bucketName));
	}
	
	@Override
	public void deleteBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
		try {
			Path bucket = base.resolve(bucketName);
			Files.walkFileTree(bucket, new SimpleFileVisitor<Path>(){
				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
				
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			throw new AmazonClientException(e);
		}
	}

	public Path addFile(Path parent, String fileName) throws IOException {
		return Files.createFile(parent.resolve(fileName));
	}

	public void addFile(Path parent, String fileName, String content) throws IOException {
		Path file = Files.createFile(parent.resolve(fileName));
		OutputStream outputStream = Files.newOutputStream(file);
		outputStream.write(content.getBytes());
		outputStream.close();
	}

	public Path addDirectory(Path parent, String directoryName) throws IOException {
		return Files.createDirectories(parent.resolve(directoryName));
	}
}