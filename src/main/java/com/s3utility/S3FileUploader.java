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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
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

        int totalObjectCount = 0;
        int successCount = 0;
        int skippedCount = 0;

        try (Stream<Path> files = Files.walk(sourcePath)) {
            for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                totalObjectCount++;

                String s3Key = targetFolder + "/" + sourcePath.relativize(file);

                System.out.println("s3Key: " + s3Key);

                if (doesObjectExist(s3Key)) {
                    skippedCount++;
                    System.out.println("Skipped existing file: " + totalObjectCount);
                    continue;
                }

                try {
                    uploadFile(file, s3Key);
                    successCount++;
                    System.out.println("Successfully uploaded " + totalObjectCount);
                } catch (Exception e) {
                    System.err.println("Failed to upload: " + totalObjectCount + " -> " + e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read files from directory: " + e.getMessage());
        }

        System.out.println("\nUpload Summary:");
        System.out.println("Total files processed: " + totalObjectCount);
        System.out.println("Successfully uploaded: " + successCount);
        System.out.println("Skipped (already exists): " + skippedCount);
        System.out.println("Failed to upload: " + (totalObjectCount - successCount - skippedCount));
    }

    private void uploadFile(Path filePath, String s3Key) throws IOException {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(targetBucket)
                .key(s3Key)
                .build();

        s3Client.putObject(putObjectRequest, filePath);
    }

    private boolean doesObjectExist(String s3Key) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(targetBucket)
                    .key(s3Key)
                    .build();
            HeadObjectResponse response = s3Client.headObject(headObjectRequest);
            return response != null;
        } catch (Exception e) {
            return false;
        }
    }
}