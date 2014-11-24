package com.upplication.s3fs;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
/**
 * Client Amazon S3
 * @see com.amazonaws.services.s3.AmazonS3Client
 */
public class S3Client implements AmazonS3 {
	
	private AmazonS3 client;
	
	public S3Client(AmazonS3 client){
		this.client = client;
	}

	/**
	 * @param endpoint
	 * @see com.amazonaws.services.s3.AmazonS3#setEndpoint(java.lang.String)
	 */
	public void setEndpoint(String endpoint) {
		client.setEndpoint(endpoint);
	}

	/**
	 * @param region
	 * @throws IllegalArgumentException
	 * @see com.amazonaws.services.s3.AmazonS3#setRegion(com.amazonaws.regions.Region)
	 */
	public void setRegion(com.amazonaws.regions.Region region) throws IllegalArgumentException {
		client.setRegion(region);
	}

	/**
	 * @param clientOptions
	 * @see com.amazonaws.services.s3.AmazonS3#setS3ClientOptions(com.amazonaws.services.s3.S3ClientOptions)
	 */
	public void setS3ClientOptions(S3ClientOptions clientOptions) {
		client.setS3ClientOptions(clientOptions);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param newStorageClass
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#changeObjectStorageClass(java.lang.String, java.lang.String, com.amazonaws.services.s3.model.StorageClass)
	 */
	public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass) throws AmazonClientException, AmazonServiceException {
		client.changeObjectStorageClass(bucketName, key, newStorageClass);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param newRedirectLocation
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectRedirectLocation(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation) throws AmazonClientException, AmazonServiceException {
		client.setObjectRedirectLocation(bucketName, key, newRedirectLocation);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listObjects(java.lang.String)
	 */
	public ObjectListing listObjects(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.listObjects(bucketName);
	}

	/**
	 * @param bucketName
	 * @param prefix
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listObjects(java.lang.String, java.lang.String)
	 */
	public ObjectListing listObjects(String bucketName, String prefix) throws AmazonClientException, AmazonServiceException {
		return client.listObjects(bucketName, prefix);
	}

	/**
	 * @param listObjectsRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest)
	 */
	public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws AmazonClientException, AmazonServiceException {
		return client.listObjects(listObjectsRequest);
	}

	/**
	 * @param previousObjectListing
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listNextBatchOfObjects(com.amazonaws.services.s3.model.ObjectListing)
	 */
	public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws AmazonClientException, AmazonServiceException {
		return client.listNextBatchOfObjects(previousObjectListing);
	}

	/**
	 * @param bucketName
	 * @param prefix
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listVersions(java.lang.String, java.lang.String)
	 */
	public VersionListing listVersions(String bucketName, String prefix) throws AmazonClientException, AmazonServiceException {
		return client.listVersions(bucketName, prefix);
	}

	/**
	 * @param previousVersionListing
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listNextBatchOfVersions(com.amazonaws.services.s3.model.VersionListing)
	 */
	public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing) throws AmazonClientException, AmazonServiceException {
		return client.listNextBatchOfVersions(previousVersionListing);
	}

	/**
	 * @param bucketName
	 * @param prefix
	 * @param keyMarker
	 * @param versionIdMarker
	 * @param delimiter
	 * @param maxResults
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listVersions(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.Integer)
	 */
	public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults)
			throws AmazonClientException, AmazonServiceException {
		return client.listVersions(bucketName, prefix, keyMarker, versionIdMarker, delimiter, maxResults);
	}

	/**
	 * @param listVersionsRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest)
	 */
	public VersionListing listVersions(ListVersionsRequest listVersionsRequest) throws AmazonClientException, AmazonServiceException {
		return client.listVersions(listVersionsRequest);
	}

	/**
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getS3AccountOwner()
	 */
	public Owner getS3AccountOwner() throws AmazonClientException, AmazonServiceException {
		return client.getS3AccountOwner();
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#doesBucketExist(java.lang.String)
	 */
	public boolean doesBucketExist(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.doesBucketExist(bucketName);
	}

	/**
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listBuckets()
	 */
	public List<Bucket> listBuckets() throws AmazonClientException, AmazonServiceException {
		return client.listBuckets();
	}

	/**
	 * @param listBucketsRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listBuckets(com.amazonaws.services.s3.model.ListBucketsRequest)
	 */
	public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest) throws AmazonClientException, AmazonServiceException {
		return client.listBuckets(listBucketsRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLocation(java.lang.String)
	 */
	public String getBucketLocation(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketLocation(bucketName);
	}

	/**
	 * @param getBucketLocationRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLocation(com.amazonaws.services.s3.model.GetBucketLocationRequest)
	 */
	public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) throws AmazonClientException, AmazonServiceException {
		return client.getBucketLocation(getBucketLocationRequest);
	}

	/**
	 * @param createBucketRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest)
	 */
	public Bucket createBucket(CreateBucketRequest createBucketRequest) throws AmazonClientException, AmazonServiceException {
		return client.createBucket(createBucketRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(java.lang.String)
	 */
	public Bucket createBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.createBucket(bucketName);
	}

	/**
	 * @param bucketName
	 * @param region
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(java.lang.String, com.amazonaws.services.s3.model.Region)
	 */
	public Bucket createBucket(String bucketName, Region region) throws AmazonClientException, AmazonServiceException {
		return client.createBucket(bucketName, region);
	}

	/**
	 * @param bucketName
	 * @param region
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#createBucket(java.lang.String, java.lang.String)
	 */
	public Bucket createBucket(String bucketName, String region) throws AmazonClientException, AmazonServiceException {
		return client.createBucket(bucketName, region);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectAcl(java.lang.String, java.lang.String)
	 */
	public AccessControlList getObjectAcl(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
		return client.getObjectAcl(bucketName, key);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param versionId
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectAcl(java.lang.String, java.lang.String, java.lang.String)
	 */
	public AccessControlList getObjectAcl(String bucketName, String key, String versionId) throws AmazonClientException, AmazonServiceException {
		return client.getObjectAcl(bucketName, key, versionId);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param acl
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, com.amazonaws.services.s3.model.AccessControlList)
	 */
	public void setObjectAcl(String bucketName, String key, AccessControlList acl) throws AmazonClientException, AmazonServiceException {
		client.setObjectAcl(bucketName, key, acl);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param acl
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, com.amazonaws.services.s3.model.CannedAccessControlList)
	 */
	public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl) throws AmazonClientException, AmazonServiceException {
		client.setObjectAcl(bucketName, key, acl);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param versionId
	 * @param acl
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, java.lang.String, com.amazonaws.services.s3.model.AccessControlList)
	 */
	public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl) throws AmazonClientException, AmazonServiceException {
		client.setObjectAcl(bucketName, key, versionId, acl);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param versionId
	 * @param acl
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setObjectAcl(java.lang.String, java.lang.String, java.lang.String, com.amazonaws.services.s3.model.CannedAccessControlList)
	 */
	public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl) throws AmazonClientException, AmazonServiceException {
		client.setObjectAcl(bucketName, key, versionId, acl);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketAcl(java.lang.String)
	 */
	public AccessControlList getBucketAcl(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketAcl(bucketName);
	}

	/**
	 * @param setBucketAclRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketAcl(com.amazonaws.services.s3.model.SetBucketAclRequest)
	 */
	public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws AmazonClientException, AmazonServiceException {
		client.setBucketAcl(setBucketAclRequest);
	}

	/**
	 * @param getBucketAclRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketAcl(com.amazonaws.services.s3.model.GetBucketAclRequest)
	 */
	public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest) throws AmazonClientException, AmazonServiceException {
		return client.getBucketAcl(getBucketAclRequest);
	}

	/**
	 * @param bucketName
	 * @param acl
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketAcl(java.lang.String, com.amazonaws.services.s3.model.AccessControlList)
	 */
	public void setBucketAcl(String bucketName, AccessControlList acl) throws AmazonClientException, AmazonServiceException {
		client.setBucketAcl(bucketName, acl);
	}

	/**
	 * @param bucketName
	 * @param acl
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketAcl(java.lang.String, com.amazonaws.services.s3.model.CannedAccessControlList)
	 */
	public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws AmazonClientException, AmazonServiceException {
		client.setBucketAcl(bucketName, acl);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectMetadata(java.lang.String, java.lang.String)
	 */
	public ObjectMetadata getObjectMetadata(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
		return client.getObjectMetadata(bucketName, key);
	}

	/**
	 * @param getObjectMetadataRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObjectMetadata(com.amazonaws.services.s3.model.GetObjectMetadataRequest)
	 */
	public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest) throws AmazonClientException, AmazonServiceException {
		return client.getObjectMetadata(getObjectMetadataRequest);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObject(java.lang.String, java.lang.String)
	 */
	public S3Object getObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
		return client.getObject(bucketName, key);
	}

	/**
	 * @param getObjectRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObject(com.amazonaws.services.s3.model.GetObjectRequest)
	 */
	public S3Object getObject(GetObjectRequest getObjectRequest) throws AmazonClientException, AmazonServiceException {
		return client.getObject(getObjectRequest);
	}

	/**
	 * @param getObjectRequest
	 * @param destinationFile
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getObject(com.amazonaws.services.s3.model.GetObjectRequest, java.io.File)
	 */
	public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile) throws AmazonClientException, AmazonServiceException {
		return client.getObject(getObjectRequest, destinationFile);
	}

	/**
	 * @param deleteBucketRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucket(com.amazonaws.services.s3.model.DeleteBucketRequest)
	 */
	public void deleteBucket(DeleteBucketRequest deleteBucketRequest) throws AmazonClientException, AmazonServiceException {
		client.deleteBucket(deleteBucketRequest);
	}

	/**
	 * @param bucketName
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucket(java.lang.String)
	 */
	public void deleteBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
		client.deleteBucket(bucketName);
	}

	/**
	 * @param putObjectRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#putObject(com.amazonaws.services.s3.model.PutObjectRequest)
	 */
	public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonClientException, AmazonServiceException {
		return client.putObject(putObjectRequest);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param file
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#putObject(java.lang.String, java.lang.String, java.io.File)
	 */
	public PutObjectResult putObject(String bucketName, String key, File file) throws AmazonClientException, AmazonServiceException {
		return client.putObject(bucketName, key, file);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param input
	 * @param metadata
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#putObject(java.lang.String, java.lang.String, java.io.InputStream, com.amazonaws.services.s3.model.ObjectMetadata)
	 */
	public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws AmazonClientException, AmazonServiceException {
		return client.putObject(bucketName, key, input, metadata);
	}

	/**
	 * @param sourceBucketName
	 * @param sourceKey
	 * @param destinationBucketName
	 * @param destinationKey
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#copyObject(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws AmazonClientException,
			AmazonServiceException {
		return client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
	}

	/**
	 * @param copyObjectRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#copyObject(com.amazonaws.services.s3.model.CopyObjectRequest)
	 */
	public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws AmazonClientException, AmazonServiceException {
		return client.copyObject(copyObjectRequest);
	}

	/**
	 * @param copyPartRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#copyPart(com.amazonaws.services.s3.model.CopyPartRequest)
	 */
	public CopyPartResult copyPart(CopyPartRequest copyPartRequest) throws AmazonClientException, AmazonServiceException {
		return client.copyPart(copyPartRequest);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteObject(java.lang.String, java.lang.String)
	 */
	public void deleteObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
		client.deleteObject(bucketName, key);
	}

	/**
	 * @param deleteObjectRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteObject(com.amazonaws.services.s3.model.DeleteObjectRequest)
	 */
	public void deleteObject(DeleteObjectRequest deleteObjectRequest) throws AmazonClientException, AmazonServiceException {
		client.deleteObject(deleteObjectRequest);
	}

	/**
	 * @param deleteObjectsRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteObjects(com.amazonaws.services.s3.model.DeleteObjectsRequest)
	 */
	public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws AmazonClientException, AmazonServiceException {
		return client.deleteObjects(deleteObjectsRequest);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param versionId
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteVersion(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void deleteVersion(String bucketName, String key, String versionId) throws AmazonClientException, AmazonServiceException {
		client.deleteVersion(bucketName, key, versionId);
	}

	/**
	 * @param deleteVersionRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteVersion(com.amazonaws.services.s3.model.DeleteVersionRequest)
	 */
	public void deleteVersion(DeleteVersionRequest deleteVersionRequest) throws AmazonClientException, AmazonServiceException {
		client.deleteVersion(deleteVersionRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLoggingConfiguration(java.lang.String)
	 */
	public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketLoggingConfiguration(bucketName);
	}

	/**
	 * @param setBucketLoggingConfigurationRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketLoggingConfiguration(com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest)
	 */
	public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest) throws AmazonClientException, AmazonServiceException {
		client.setBucketLoggingConfiguration(setBucketLoggingConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketVersioningConfiguration(java.lang.String)
	 */
	public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketVersioningConfiguration(bucketName);
	}

	/**
	 * @param setBucketVersioningConfigurationRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketVersioningConfiguration(com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest)
	 */
	public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest) throws AmazonClientException,
			AmazonServiceException {
		client.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketLifecycleConfiguration(java.lang.String)
	 */
	public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
		return client.getBucketLifecycleConfiguration(bucketName);
	}

	/**
	 * @param bucketName
	 * @param bucketLifecycleConfiguration
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketLifecycleConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketLifecycleConfiguration)
	 */
	public void setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
		client.setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration);
	}

	/**
	 * @param setBucketLifecycleConfigurationRequest
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketLifecycleConfiguration(com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest)
	 */
	public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
		client.setBucketLifecycleConfiguration(setBucketLifecycleConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketLifecycleConfiguration(java.lang.String)
	 */
	public void deleteBucketLifecycleConfiguration(String bucketName) {
		client.deleteBucketLifecycleConfiguration(bucketName);
	}

	/**
	 * @param deleteBucketLifecycleConfigurationRequest
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketLifecycleConfiguration(com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest)
	 */
	public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
		client.deleteBucketLifecycleConfiguration(deleteBucketLifecycleConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketCrossOriginConfiguration(java.lang.String)
	 */
	public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
		return client.getBucketCrossOriginConfiguration(bucketName);
	}

	/**
	 * @param bucketName
	 * @param bucketCrossOriginConfiguration
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketCrossOriginConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketCrossOriginConfiguration)
	 */
	public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
		client.setBucketCrossOriginConfiguration(bucketName, bucketCrossOriginConfiguration);
	}

	/**
	 * @param setBucketCrossOriginConfigurationRequest
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketCrossOriginConfiguration(com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest)
	 */
	public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
		client.setBucketCrossOriginConfiguration(setBucketCrossOriginConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketCrossOriginConfiguration(java.lang.String)
	 */
	public void deleteBucketCrossOriginConfiguration(String bucketName) {
		client.deleteBucketCrossOriginConfiguration(bucketName);
	}

	/**
	 * @param deleteBucketCrossOriginConfigurationRequest
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketCrossOriginConfiguration(com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest)
	 */
	public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
		client.deleteBucketCrossOriginConfiguration(deleteBucketCrossOriginConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketTaggingConfiguration(java.lang.String)
	 */
	public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
		return client.getBucketTaggingConfiguration(bucketName);
	}

	/**
	 * @param bucketName
	 * @param bucketTaggingConfiguration
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketTaggingConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketTaggingConfiguration)
	 */
	public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
		client.setBucketTaggingConfiguration(bucketName, bucketTaggingConfiguration);
	}

	/**
	 * @param setBucketTaggingConfigurationRequest
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketTaggingConfiguration(com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest)
	 */
	public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
		client.setBucketTaggingConfiguration(setBucketTaggingConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketTaggingConfiguration(java.lang.String)
	 */
	public void deleteBucketTaggingConfiguration(String bucketName) {
		client.deleteBucketTaggingConfiguration(bucketName);
	}

	/**
	 * @param deleteBucketTaggingConfigurationRequest
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketTaggingConfiguration(com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest)
	 */
	public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
		client.deleteBucketTaggingConfiguration(deleteBucketTaggingConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketNotificationConfiguration(java.lang.String)
	 */
	public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketNotificationConfiguration(bucketName);
	}

	/**
	 * @param setBucketNotificationConfigurationRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketNotificationConfiguration(com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest)
	 */
	public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest) throws AmazonClientException,
			AmazonServiceException {
		client.setBucketNotificationConfiguration(setBucketNotificationConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @param bucketNotificationConfiguration
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketNotificationConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketNotificationConfiguration)
	 */
	public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration) throws AmazonClientException,
			AmazonServiceException {
		client.setBucketNotificationConfiguration(bucketName, bucketNotificationConfiguration);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketWebsiteConfiguration(java.lang.String)
	 */
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketWebsiteConfiguration(bucketName);
	}

	/**
	 * @param getBucketWebsiteConfigurationRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketWebsiteConfiguration(com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest)
	 */
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest) throws AmazonClientException,
			AmazonServiceException {
		return client.getBucketWebsiteConfiguration(getBucketWebsiteConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @param configuration
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketWebsiteConfiguration(java.lang.String, com.amazonaws.services.s3.model.BucketWebsiteConfiguration)
	 */
	public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration) throws AmazonClientException, AmazonServiceException {
		client.setBucketWebsiteConfiguration(bucketName, configuration);
	}

	/**
	 * @param setBucketWebsiteConfigurationRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketWebsiteConfiguration(com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest)
	 */
	public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest) throws AmazonClientException, AmazonServiceException {
		client.setBucketWebsiteConfiguration(setBucketWebsiteConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketWebsiteConfiguration(java.lang.String)
	 */
	public void deleteBucketWebsiteConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
		client.deleteBucketWebsiteConfiguration(bucketName);
	}

	/**
	 * @param deleteBucketWebsiteConfigurationRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketWebsiteConfiguration(com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest)
	 */
	public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest) throws AmazonClientException,
			AmazonServiceException {
		client.deleteBucketWebsiteConfiguration(deleteBucketWebsiteConfigurationRequest);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketPolicy(java.lang.String)
	 */
	public BucketPolicy getBucketPolicy(String bucketName) throws AmazonClientException, AmazonServiceException {
		return client.getBucketPolicy(bucketName);
	}

	/**
	 * @param getBucketPolicyRequest
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#getBucketPolicy(com.amazonaws.services.s3.model.GetBucketPolicyRequest)
	 */
	public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) throws AmazonClientException, AmazonServiceException {
		return client.getBucketPolicy(getBucketPolicyRequest);
	}

	/**
	 * @param bucketName
	 * @param policyText
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketPolicy(java.lang.String, java.lang.String)
	 */
	public void setBucketPolicy(String bucketName, String policyText) throws AmazonClientException, AmazonServiceException {
		client.setBucketPolicy(bucketName, policyText);
	}

	/**
	 * @param setBucketPolicyRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#setBucketPolicy(com.amazonaws.services.s3.model.SetBucketPolicyRequest)
	 */
	public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest) throws AmazonClientException, AmazonServiceException {
		client.setBucketPolicy(setBucketPolicyRequest);
	}

	/**
	 * @param bucketName
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketPolicy(java.lang.String)
	 */
	public void deleteBucketPolicy(String bucketName) throws AmazonClientException, AmazonServiceException {
		client.deleteBucketPolicy(bucketName);
	}

	/**
	 * @param deleteBucketPolicyRequest
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#deleteBucketPolicy(com.amazonaws.services.s3.model.DeleteBucketPolicyRequest)
	 */
	public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) throws AmazonClientException, AmazonServiceException {
		client.deleteBucketPolicy(deleteBucketPolicyRequest);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param expiration
	 * @return
	 * @throws AmazonClientException
	 * @see com.amazonaws.services.s3.AmazonS3#generatePresignedUrl(java.lang.String, java.lang.String, java.util.Date)
	 */
	public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws AmazonClientException {
		return client.generatePresignedUrl(bucketName, key, expiration);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param expiration
	 * @param method
	 * @return
	 * @throws AmazonClientException
	 * @see com.amazonaws.services.s3.AmazonS3#generatePresignedUrl(java.lang.String, java.lang.String, java.util.Date, com.amazonaws.HttpMethod)
	 */
	public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method) throws AmazonClientException {
		return client.generatePresignedUrl(bucketName, key, expiration, method);
	}

	/**
	 * @param generatePresignedUrlRequest
	 * @return
	 * @throws AmazonClientException
	 * @see com.amazonaws.services.s3.AmazonS3#generatePresignedUrl(com.amazonaws.services.s3.model.GeneratePresignedUrlRequest)
	 */
	public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest) throws AmazonClientException {
		return client.generatePresignedUrl(generatePresignedUrlRequest);
	}

	/**
	 * @param request
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#initiateMultipartUpload(com.amazonaws.services.s3.model.InitiateMultipartUploadRequest)
	 */
	public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws AmazonClientException, AmazonServiceException {
		return client.initiateMultipartUpload(request);
	}

	/**
	 * @param request
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#uploadPart(com.amazonaws.services.s3.model.UploadPartRequest)
	 */
	public UploadPartResult uploadPart(UploadPartRequest request) throws AmazonClientException, AmazonServiceException {
		return client.uploadPart(request);
	}

	/**
	 * @param request
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listParts(com.amazonaws.services.s3.model.ListPartsRequest)
	 */
	public PartListing listParts(ListPartsRequest request) throws AmazonClientException, AmazonServiceException {
		return client.listParts(request);
	}

	/**
	 * @param request
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#abortMultipartUpload(com.amazonaws.services.s3.model.AbortMultipartUploadRequest)
	 */
	public void abortMultipartUpload(AbortMultipartUploadRequest request) throws AmazonClientException, AmazonServiceException {
		client.abortMultipartUpload(request);
	}

	/**
	 * @param request
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#completeMultipartUpload(com.amazonaws.services.s3.model.CompleteMultipartUploadRequest)
	 */
	public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) throws AmazonClientException, AmazonServiceException {
		return client.completeMultipartUpload(request);
	}

	/**
	 * @param request
	 * @return
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#listMultipartUploads(com.amazonaws.services.s3.model.ListMultipartUploadsRequest)
	 */
	public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws AmazonClientException, AmazonServiceException {
		return client.listMultipartUploads(request);
	}

	/**
	 * @param request
	 * @return
	 * @see com.amazonaws.services.s3.AmazonS3#getCachedResponseMetadata(com.amazonaws.AmazonWebServiceRequest)
	 */
	public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
		return client.getCachedResponseMetadata(request);
	}

	/**
	 * @param request
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#restoreObject(com.amazonaws.services.s3.model.RestoreObjectRequest)
	 */
	public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
		client.restoreObject(request);
	}

	/**
	 * @param bucketName
	 * @param key
	 * @param expirationInDays
	 * @throws AmazonServiceException
	 * @see com.amazonaws.services.s3.AmazonS3#restoreObject(java.lang.String, java.lang.String, int)
	 */
	public void restoreObject(String bucketName, String key, int expirationInDays) throws AmazonServiceException {
		client.restoreObject(bucketName, key, expirationInDays);
	}

	/**
	 * @param bucketName
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @see com.amazonaws.services.s3.AmazonS3#enableRequesterPays(java.lang.String)
	 */
	public void enableRequesterPays(String bucketName) throws AmazonServiceException, AmazonClientException {
		client.enableRequesterPays(bucketName);
	}

	/**
	 * @param bucketName
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @see com.amazonaws.services.s3.AmazonS3#disableRequesterPays(java.lang.String)
	 */
	public void disableRequesterPays(String bucketName) throws AmazonServiceException, AmazonClientException {
		client.disableRequesterPays(bucketName);
	}

	/**
	 * @param bucketName
	 * @return
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @see com.amazonaws.services.s3.AmazonS3#isRequesterPaysEnabled(java.lang.String)
	 */
	public boolean isRequesterPaysEnabled(String bucketName) throws AmazonServiceException, AmazonClientException {
		return client.isRequesterPaysEnabled(bucketName);
	}
	
}