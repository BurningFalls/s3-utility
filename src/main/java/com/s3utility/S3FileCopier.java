package com.s3utility;

import java.util.List;

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
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
//        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        this.s3Client = S3Client.builder()
//                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .credentialsProvider(InstanceProfileCredentialsProvider.create())
                .region(region)
                .build();
        this.sourceBucket = sourceBucket;
        this.sourceFolder = sourceFolder;
        this.targetBucket = targetBucket;
        this.targetFolder = targetFolder;
    }

    public void copyFolderBetweenBuckets() {
        int sourceFilesCount = 0;
        int successFilesCount = 0;
        String continuationToken = null;

        do {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(sourceBucket)
                    .prefix(sourceFolder)
                    .continuationToken(continuationToken)
                    .build();

            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = listResponse.contents();
            sourceFilesCount += objects.size();

            for (S3Object object : objects) {
                String sourceKey = object.key();
                String targetKey = targetFolder + sourceKey.substring(sourceFolder.length());
                try {
                    copyObject(sourceKey, targetKey);
                    successFilesCount += 1;
                    System.out.println("Success to copy file #" + successFilesCount);
                } catch (Exception e) {
                    System.err.println("Failed to copy file:" + e.getMessage());
                }
            }

            continuationToken = listResponse.nextContinuationToken();
        } while (continuationToken != null);

        if (sourceFilesCount != successFilesCount) {
            System.err.println(sourceFilesCount - successFilesCount + " files were not copied successfully.");
        } else {
            System.out.println("All " + sourceFilesCount + " files were copied successfully.");
        }
    }

    private void copyObject(String sourceKey, String targetKey) {
        if (doesObjectExist(targetKey)) {
            return;
        }

        System.out.println("sourceKey: " + sourceKey + ", " + "targetKey: " + targetKey);

        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(targetBucket)
                .destinationKey(targetKey)
                .acl("bucket-owner-full-control")
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
