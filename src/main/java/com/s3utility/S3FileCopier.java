package com.s3utility;

import java.util.List;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3FileCopier {

    private final S3Client s3Client;
    private final String sourceBucket;
    private final String sourceFolder;
    private final String targetBucket;
    private final String targetFolder;

    public S3FileCopier(String accessKey, String secretKey, Region region,
                        String sourceBucket, String sourceFolder, String targetBucket, String targetFolder) {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        this.s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .region(region)
                .build();
        this.sourceBucket = sourceBucket;
        this.sourceFolder = sourceFolder;
        this.targetBucket = targetBucket;
        this.targetFolder = targetFolder;
    }

    public void copyFolderBetweenBuckets() {
        String continuationToken = null;

        do {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(sourceBucket)
                    .prefix(sourceFolder)
                    .continuationToken(continuationToken)
                    .build();

            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = listResponse.contents();

            for (S3Object object : objects) {
                String sourceKey = object.key();
                String targetKey = targetFolder + sourceKey.substring(sourceFolder.length());
                copyObject(sourceKey, targetKey);
            }

            continuationToken = listResponse.nextContinuationToken();
        } while (continuationToken != null);
    }

    private void copyObject(String sourceKey, String targetKey) {
        if (doesObjectExist(targetKey)) {
            return;
        }

        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(targetBucket)
                .destinationKey(targetKey)
                .build();

        s3Client.copyObject(copyRequest);
    }

    private boolean doesObjectExist(String key) {
        try {
            s3Client.headObject(b -> b.bucket(targetBucket).key(key));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
