package com.s3utility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3FileDownloader {

    private final S3Client s3Client;
    private final String sourceBucket;
    private final String sourceFolder;

    public S3FileDownloader(String accessKey, String secretKey, Region region,
                            String sourceBucket, String sourceFolder) {
        this.s3Client = S3Client.builder()
//                .credentialsProvider(StaticCredentialsProvider.create(
//                        AwsBasicCredentials.create(accessKey, secretKey)))
                .credentialsProvider(InstanceProfileCredentialsProvider.create())
                .region(region)
                .build();
        this.sourceBucket = sourceBucket;
        this.sourceFolder = sourceFolder;
    }

    public void downloadFiles() {
        Path destinationPath = Path.of(System.getProperty("user.dir"), "downloads");

        try {
            Files.createDirectories(destinationPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create destination directory: " + destinationPath, e);
        }

        String continuationToken = null;
        int successCount = 0;
        int totalObjectCount = 0;

        do {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(sourceBucket)
                    .prefix(sourceFolder)
                    .continuationToken(continuationToken)
                    .build();

            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            List<S3Object> objects = listResponse.contents();
            totalObjectCount += objects.size();

            for (S3Object object : objects) {
                String key = object.key();
                if (key.endsWith("/")) {
                    continue;
                }

                try {
                    downloadFile(key, destinationPath);
                    successCount++;
                    System.out.println("Successfully downloaded: " + successCount);
                } catch (Exception e) {
                    System.err.println("Failed to download: " + e.getMessage());
                }
            }
            continuationToken = listResponse.nextContinuationToken();
        } while (continuationToken != null);

        if (successCount == totalObjectCount) {
            System.out.println("All files were downloaded successfully!");
        } else {
            System.out.println("Some files failed to download. Total failures: " + (totalObjectCount - successCount));
        }
    }

    private void downloadFile(String key, Path destinationPath) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(sourceBucket)
                .key(key)
                .build();

        Path filePath = destinationPath.resolve(key);

        Files.createDirectories(filePath.getParent());
        s3Client.getObject(getObjectRequest, filePath);
    }
}
