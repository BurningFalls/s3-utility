package com.s3utility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3FileUploader {
    private static final String LOCAL_DIRECTORY = "/home/ubuntu/downloads/staccato/image-prod";

    private final S3Client s3Client;
    private final String targetBucket;
    private final String targetFolder;

    public S3FileUploader(String accessKey, String secretKey, Region region, String targetBucket, String targetFolder) {
        this.s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(region)
                .build();
        this.targetBucket = targetBucket;
        this.targetFolder = targetFolder;
    }

    public void uploadFiles() {
        Path sourcePath = Paths.get(LOCAL_DIRECTORY);
        if (!Files.exists(sourcePath) || !Files.isDirectory(sourcePath)) {
            throw new IllegalArgumentException("Invalid directory path: " + LOCAL_DIRECTORY);
        }

        int successCount = 0;
        int failureCount = 0;

        try (Stream<Path> files = Files.walk(sourcePath)) {
            for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                String s3Key = targetFolder + "/" + sourcePath.relativize(file);

                try {
                    uploadFile(file, s3Key);
                    successCount++;
                    System.out.println("Successfully uploaded " + successCount);
                } catch (Exception e) {
                    failureCount++;
                    System.err.println("Failed to upload: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read files from directory: " + e.getMessage());
        }

        if (failureCount == 0) {
            System.out.println("All files were uploaded successfully!");
        } else {
            System.out.println("Some files failed to upload. Total failures: " + failureCount);
        }
    }

    private void uploadFile(Path filePath, String s3Key) throws IOException {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(targetBucket)
                .key(s3Key)
                .build();

        s3Client.putObject(putObjectRequest, filePath);
    }
}