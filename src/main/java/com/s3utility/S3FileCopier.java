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
    private static final String SOURCE_BUCKET = "source_bucket";
    private static final String TARGET_BUCKET = "target_bucket";
    private static final String SOURCE_FOLDER = "source_folder";
    private static final String TARGET_FOLDER = "target_folder";

    private final S3Client s3Client;

    public S3FileCopier(String accessKey, String secretKey, Region region) {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        this.s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .region(region)
                .build();
    }

    public void copyFolderBetweenBuckets() {
        String continuationToken = null;

        do {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(SOURCE_BUCKET)
                    .prefix(SOURCE_FOLDER)
                    .continuationToken(continuationToken)
                    .build();

            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = listResponse.contents();

            for (S3Object object : objects) {
                String sourceKey = object.key();
                String targetKey = TARGET_FOLDER + sourceKey.substring(SOURCE_FOLDER.length());
                copyObject(sourceKey, targetKey);
            }

            continuationToken = listResponse.nextContinuationToken();
        } while (continuationToken != null);
    }

    private void copyObject(String sourceKey, String targetKey) {
        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                .sourceBucket(SOURCE_BUCKET)
                .sourceKey(sourceKey)
                .destinationBucket(TARGET_BUCKET)
                .destinationKey(targetKey)
                .build();

        s3Client.copyObject(copyRequest);
    }
}
