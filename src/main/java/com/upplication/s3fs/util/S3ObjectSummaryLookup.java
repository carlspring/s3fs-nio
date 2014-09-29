package com.upplication.s3fs.util;

import com.amazonaws.services.s3.model.*;
import com.google.common.base.Throwables;
import com.upplication.s3fs.AmazonS3Client;
import com.upplication.s3fs.S3Path;

import java.io.IOException;
import java.nio.file.NoSuchFileException;


public class S3ObjectSummaryLookup {

    /**
     * Get the {@link com.amazonaws.services.s3.model.S3ObjectSummary} that represent this Path or her first child if this path not exists
     * @param s3Path {@link com.upplication.s3fs.S3Path}
     * @return {@link com.amazonaws.services.s3.model.S3ObjectSummary}
     * @throws java.nio.file.NoSuchFileException if not found the path and any child
     */
    public S3ObjectSummary lookup(S3Path s3Path) throws NoSuchFileException {

        AmazonS3Client client = s3Path.getFileSystem().getClient();

        S3Object object = getS3Object(s3Path);

        if (object != null) {
            S3ObjectSummary result = new S3ObjectSummary();
            result.setBucketName(object.getBucketName());
            result.setETag(object.getObjectMetadata().getETag());
            result.setKey(object.getKey());
            result.setLastModified(object.getObjectMetadata().getLastModified());
            result.setSize(object.getObjectMetadata().getContentLength());

            return result;
        }
        // is a virtual directory
        String key = s3Path.getKey() + "/";

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(s3Path.getBucket());
        request.setPrefix(key);
        request.setMaxKeys(1);
        ObjectListing current = client.listObjects(request);

        if (!current.getObjectSummaries().isEmpty()){
            return current.getObjectSummaries().get(0);
        }
        else{
            throw new NoSuchFileException(s3Path.toString());
        }
    }

    /**
     * get S3Object represented by this S3Path try to access with or without end slash '/'
     * @param s3Path S3Path
     * @return S3Object or null if not exists
     */
    private S3Object getS3Object(S3Path s3Path){
        try {
            S3Object object = s3Path.getFileSystem()
                    .getClient()
                    .getObject(s3Path.getBucket(), s3Path.getKey());
            // FIXME: how only get the metadata
            if (object.getObjectContent() != null){
                object.getObjectContent().close();
            }
            return object;
        }
        catch (AmazonS3Exception e){
            if (e.getStatusCode() != 404){
                throw e;
            }
            else{
                try {
                    S3Object object = s3Path.getFileSystem()
                        .getClient()
                        .getObject(s3Path.getBucket(), s3Path.getKey() + "/");
                    // FIXME: how only get the metadata
                    if (object.getObjectContent() != null){
                        object.getObjectContent().close();
                    }
                    return object;
                }
                catch (AmazonS3Exception e2) {
                    if (e2.getStatusCode() != 404){
                        throw e;
                    }
                    else{
                        return null;
                    }
                }
                catch (IOException e2){
                    throw new RuntimeException(e2);
                }
            }
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
