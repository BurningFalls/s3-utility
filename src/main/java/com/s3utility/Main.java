package com.s3utility;

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

        s3FileCopy(accessKey, secretKey, region, sourceBucket, sourceFolder, targetBucket, targetFolder);
//        s3FileDownload(accessKey, secretKey, region, sourceBucket, sourceFolder);
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

        downloader.downloadFilesWithPrefix();
    }
}
