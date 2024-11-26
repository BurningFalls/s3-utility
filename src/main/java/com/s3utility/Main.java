package com.s3utility;

import java.util.Scanner;

import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.regions.Region;

public class Main {

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.load();
        String accessKey = dotenv.get("AWS_ACCESS_KEY");
        String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY");
        Region region = Region.of(dotenv.get("AWS_REGION"));
        String sourceBucket = dotenv.get("AWS_SOURCE_BUCKET");
        String sourceFolder = dotenv.get("AWS_SOURCE_FOLDER");
        String targetBucket = dotenv.get("AWS_TARGET_BUCKET");
        String targetFolder = dotenv.get("AWS_TARGET_FOLDER");

        Scanner scanner = new Scanner(System.in);
        System.out.println("Select operation:");
        System.out.println("1. Copy files");
        System.out.println("2. Download files");
        System.out.println("3. Upload files");
        System.out.print("Enter choice: ");

        int choice = scanner.nextInt();

        switch (choice) {
            case 1:
                s3FileCopy(accessKey, secretKey, region, sourceBucket, sourceFolder, targetBucket, targetFolder);
                break;
            case 2:
                s3FileDownload(accessKey, secretKey, region, sourceBucket, sourceFolder);
                break;
            case 3:
                s3FileUpload(accessKey, secretKey, region, targetBucket, targetFolder);
                break;
            default:
                System.out.println("Invalid choice. Exiting...");
        }

        scanner.close();
    }

    private static void s3FileCopy(String accessKey, String secretKey, Region region,
                                   String sourceBucket, String sourceFolder, String targetBucket, String targetFolder) {
        S3FileCopier copier = new S3FileCopier(accessKey, secretKey, region,
                sourceBucket, sourceFolder, targetBucket, targetFolder);

        copier.copyFolderBetweenBuckets();
    }

    private static void s3FileDownload(String accessKey, String secretKey, Region region,
                                       String sourceBucket, String sourceFolder) {
        S3FileDownloader downloader = new S3FileDownloader(accessKey, secretKey, region, sourceBucket, sourceFolder);

        downloader.downloadFiles();
    }

    private static void s3FileUpload(String accessKey, String secretKey, Region region,
                                     String targetBucket, String targetFolder) {
        S3FileUploader uploader = new S3FileUploader(accessKey, secretKey, region, targetBucket, targetFolder);

        uploader.uploadFiles();
    }
}
