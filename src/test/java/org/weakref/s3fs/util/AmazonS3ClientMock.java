package org.weakref.s3fs.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.weakref.s3fs.AmazonS3Client;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AmazonS3ClientMock extends AmazonS3Client{
	
	private final Path memoryBucket;

	public AmazonS3ClientMock(Path memoryBucket) {
		super(null);	
		this.memoryBucket = memoryBucket;
	}

	@Override
	public List<Bucket> listBuckets() {
		List<Bucket> res = new ArrayList<>();
		try(DirectoryStream<Path> dir = Files.newDirectoryStream(memoryBucket)){
			for (Path bucketPath : dir){
				BasicFileAttributes attr = Files.readAttributes(bucketPath, BasicFileAttributes.class);
				Bucket bucket = new Bucket();
				bucket.setCreationDate(new Date(attr.creationTime().toMillis()));
				bucket.setName(bucketPath.getFileName().toString());
				res.add(bucket);
			}
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
		return res;
	}

	@Override
	public ObjectListing listObjects(final ListObjectsRequest request) {
		
		final List<S3ObjectSummary> objects = new ArrayList<>();
		ObjectListing res = new ObjectListing(){
			@Override
			public List<S3ObjectSummary> getObjectSummaries(){
				return objects;
			}
		};
		res.setBucketName(request.getBucketName());
		res.setMaxKeys(request.getMaxKeys());
		// todo... 
		
		String[] folders = request.getPrefix().split("/");
		Path initial = memoryBucket.resolve(request.getBucketName());
		for (String folder : folders){
			initial = initial.resolve(folder);
		}
		// list all files & folders under the path
		try {
			Files.walkFileTree(initial, new SimpleFileVisitor<Path>(){
				 @Override
				    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
				        throws IOException{
					 	S3ObjectSummary summary = new S3ObjectSummary();
					 	summary.setBucketName(request.getBucketName());
					 	summary.setKey(memoryBucket.resolve(request.getBucketName()).relativize(dir).toString() + "/");
					 	objects.add(summary);
					 	return FileVisitResult.CONTINUE;
				    }

				    @Override
				    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
				        throws IOException {
				    	S3ObjectSummary summary = new S3ObjectSummary();
					 	summary.setBucketName(request.getBucketName());
					 	summary.setKey(memoryBucket.resolve(request.getBucketName()).relativize(file).toString() + "/");
					 	objects.add(summary);
				    	return FileVisitResult.CONTINUE;
				    }
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return res;
	}

	@Override
	public S3Object getObject(String bucketName, String key) {
		S3Object res = new S3Object();
		res.setBucketName(bucketName);
		res.setKey(key);
		Path initial = memoryBucket.resolve(bucketName);
		for (String folder : key.split("/")){
			initial = initial.resolve(folder);
		}
		try {
			res.setObjectContent(Files.newInputStream(initial));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return res;
	}

	@Override
	public PutObjectResult putObject(String bucket, String key, File file) {
		Path initial = memoryBucket.resolve(bucket);
		for (String folder : key.split("/")){
			initial = initial.resolve(folder);
		}
		try {
			Files.write(initial, Files.readAllBytes(file.toPath()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		PutObjectResult res = new PutObjectResult();
		return res;
	}

	@Override
	public PutObjectResult putObject(String bucket, String keyName,
			ByteArrayInputStream byteArrayInputStream, ObjectMetadata metadata) {
		Path initial = memoryBucket.resolve(bucket);
		for (String folder : keyName.split("/")){
			initial = initial.resolve(folder);
		}
		try {
			//byteArrayInputStream.
			byte[] array = new byte[byteArrayInputStream.available()];
			byteArrayInputStream.read(array);
			Files.write(initial, array);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		PutObjectResult res = new PutObjectResult();
		return res;
	}

	@Override
	public void deleteObject(String bucket, String key) {
		Path initial = memoryBucket.resolve(bucket);
		for (String folder : key.split("/")){
			initial = initial.resolve(folder);
		}
		
		try {
			Files.delete(initial);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public CopyObjectResult copyObject(String sourceBucketName,
			String sourceKey, String destinationBucketName,
			String destinationKey) {
		
		Path source = memoryBucket.resolve(sourceBucketName);
		for (String folder : sourceKey.split("/")){
			source = source.resolve(folder);
		}
		
		Path end = memoryBucket.resolve(destinationBucketName);
		for (String folder : destinationKey.split("/")){
			end = end.resolve(folder);
		}
		
		try {
			Files.copy(source, end);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		CopyObjectResult res = new CopyObjectResult();
		
		return res;
	}

	@Override
	public AccessControlList getBucketAcl(String bucket) {
		AccessControlList res = new AccessControlList();
		Owner owner = new Owner();
		owner.setDisplayName("mock");
		res.setOwner(owner);
		/*Grantee
		Grant grant = new Grant(grantee, permission);
		res.grantAllPermissions(grantsVarArg);
		return super.getBucketAcl(bucket);*/
		return res;
	}

	@Override
	public Owner getS3AccountOwner() {
		// TODO Auto-generated method stub
		return super.getS3AccountOwner();
	}

	@Override
	public void setEndpoint(String endpoint) {
		// TODO Auto-generated method stub
		super.setEndpoint(endpoint);
	}

	@Override
	public AccessControlList getObjectAcl(String bucketName, String key) {
		// TODO Auto-generated method stub
		return super.getObjectAcl(bucketName, key);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		super.finalize();
	}
}
