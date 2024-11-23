package com.s3utility;

import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.regions.Region;

public class Main {
    private static final Region REGION = Region.AP_NORTHEAST_2;

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.load();
        String accessKey = dotenv.get("AWS_ACCESS_KEY");
        String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY");

        s3FileCopy(accessKey, secretKey);
    }

    private static void s3FileCopy(String accessKey, String secretKey) {
        S3FileCopier copier = new S3FileCopier(accessKey, secretKey, REGION);

        copier.copyFolderBetweenBuckets();
    }

    private static void s3FileDownload(String accessKey, String secretKey) {
        S3FileDownloader downloader = new S3FileDownloader(accessKey, secretKey, REGION);

        downloader.downloadFilesWithPrefix();
    }
}
