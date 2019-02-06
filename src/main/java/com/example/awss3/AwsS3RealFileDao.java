package com.example.awss3;

import java.net.URL;
import java.util.Date;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

@Component
public class AwsS3RealFileDao {

  private static final Logger logger = LoggerFactory.getLogger(AwsS3RealFileDao.class);

  private AmazonS3 s3client;

  @Value("${amazon.s3.region}")
  private String region;
  @Value("${amazon.s3.publicBucketName}")
  private String publicBucketName;
  @Value("${amazon.s3.accessKey}")
  private String accessKey;
  @Value("${amazon.s3.secretKey}")
  private String secretKey;

  @PostConstruct
  private void initializeAmazon() {
    AWSCredentials credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
    this.s3client = AmazonS3ClientBuilder
        .standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build();
  }

  public URL getPreSignedFileUploadUrl(String objectId) {
    URL url = null;
    try {
      Date expiration = getExpiration();
      GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(publicBucketName, objectId)
          .withMethod(HttpMethod.PUT)
          .withExpiration(expiration);

      generatePresignedUrlRequest.addRequestParameter(Headers.S3_CANNED_ACL, CannedAccessControlList.BucketOwnerRead.toString());

      url = s3client.generatePresignedUrl(generatePresignedUrlRequest);
    } catch (RuntimeException ex) {
      logger.error("getPreSignedFileUploadUrl error: {}", ex);
    }
    return url;
  }

  public URL getPreSignedFileDownloadUrl(String objectId) {
    URL url = null;
    try {
      Date expiration = getExpiration();

      url = s3client.generatePresignedUrl(new GeneratePresignedUrlRequest(publicBucketName, objectId)
          .withExpiration(expiration));
    } catch (RuntimeException ex) {
      logger.error("getPreSignedFileDownloadUrl error: {}", ex);
    }
    return url;
  }

  private Date getExpiration() {
    Date expiration = new Date();
    long expTimeMillis = expiration.getTime();
    expTimeMillis += 1000 * 60 *60; //1 min
    expiration.setTime(expTimeMillis);
    return expiration;
  }

  private void cleanBucket(String bucketName) {
    try {
      logger.info(" - removing objects from bucket");
      ObjectListing object_listing = s3client.listObjects(bucketName);
      while (true) {
        for (S3ObjectSummary summary : object_listing.getObjectSummaries()) {
          s3client.deleteObject(bucketName, summary.getKey());
        }

        // more object_listing to retrieve?
        if (object_listing.isTruncated()) {
          object_listing = s3client.listNextBatchOfObjects(object_listing);
        } else {
          break;
        }
      }

      logger.info(" - removing versions from bucket");
      VersionListing version_listing = s3client.listVersions(
          new ListVersionsRequest().withBucketName(bucketName));
      while (true) {
        for (S3VersionSummary vs : version_listing.getVersionSummaries()) {
          s3client.deleteVersion(
              bucketName, vs.getKey(), vs.getVersionId());
        }

        if (version_listing.isTruncated()) {
          version_listing = s3client.listNextBatchOfVersions(
              version_listing);
        } else {
          break;
        }
      }
    } catch (RuntimeException ex) {
      logger.error("clean error: {}", ex);
    }
  }

  public void cleanBucket() {
    cleanBucket(publicBucketName);
  }
}
