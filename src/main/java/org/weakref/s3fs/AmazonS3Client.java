package org.weakref.s3fs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

public class AmazonS3Client {
	
	private AmazonS3 client;
	
	public AmazonS3Client(AmazonS3 client){
		this.client = client;
	}
	public List<Bucket> listBuckets() {
		return client.listBuckets();
	}
	public ObjectListing listObjects(ListObjectsRequest request) {
		return client.listObjects(request);
	}
	public S3Object getObject(String bucketName, String key) {
		return client.getObject(bucketName, key);
	}
	public PutObjectResult putObject(String bucket, String key, File file) {
		return client.putObject(bucket, key, file);
	}
	public PutObjectResult putObject(String bucket, String keyName,
			ByteArrayInputStream byteArrayInputStream, ObjectMetadata metadata) {
		return client.putObject(bucket, keyName, byteArrayInputStream, metadata);
	}
	public void deleteObject(String bucket, String key) {
		client.deleteObject(bucket, key);
	}
	public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName,
			String destinationKey) {
		return client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
	}
	public AccessControlList getBucketAcl(String bucket) {
		return client.getBucketAcl(bucket);
	}
	public Owner getS3AccountOwner() {
		return client.getS3AccountOwner();
	}
	public void setEndpoint(String endpoint) {
		client.setEndpoint(endpoint);
	}
	/**
	 * @see com.amazonaws.services.s3.AmazonS3Client#getObjectAcl(String, String)
	 */
	public AccessControlList getObjectAcl(String bucketName, String key) {
		return client.getObjectAcl(bucketName, key);
	}
	/**
	 * @see com.amazonaws.services.s3.AmazonS3Client#getObjectMetadata(String, String)
	 */
	public ObjectMetadata getObjectMetadata(String bucketName, String key) {
		return client.getObjectMetadata(bucketName, key);
	}
}
