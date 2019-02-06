package com.example.awss3;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.amazonaws.SdkClientException;
import com.amazonaws.util.IOUtils;

@Component
public class OnStartup implements ApplicationListener<ContextRefreshedEvent> {

  private static final Logger log = LoggerFactory.getLogger(OnStartup.class);

  @Autowired
  private AwsS3RealFileDao awsS3RealFileDao;

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    log.info("started!!!");

    String salt =  RandomStringUtils.randomAlphabetic(10);

    try {

      awsS3RealFileDao.cleanBucket();

      log.info("upload!!!");
      URL uploadUrl = awsS3RealFileDao.getPreSignedFileUploadUrl(salt + "_dummy.txt");

      // Create the connection and use it to upload the new object using the pre-signed URL.
      HttpURLConnection uploadConnection = (HttpURLConnection) uploadUrl.openConnection();
      uploadConnection.setDoOutput(true);
      uploadConnection.setRequestMethod("PUT");

      OutputStreamWriter out = new OutputStreamWriter(uploadConnection.getOutputStream());
      out.write("This text uploaded as an object via presigned URL. " + salt);
      out.close();

      System.out.println("HTTP response code: " + uploadConnection.getResponseCode());
      System.out.println("HTTP response message: " + uploadConnection.getResponseMessage());
      System.out.println("HTTP headers: " + uploadConnection.getHeaderFields());
      System.out.println("HTTP uploadUrl: " + uploadUrl);

      log.info("download!!!");
      URL downloadUrl = awsS3RealFileDao.getPreSignedFileDownloadUrl(salt + "_dummy.txt");

      HttpURLConnection downloadConnection = (HttpURLConnection) downloadUrl.openConnection();
      downloadConnection.setDoInput(true);
      downloadConnection.setRequestMethod("GET");
      String downloadBody = IOUtils.toString(downloadConnection.getInputStream());
      downloadConnection.getInputStream().close();

      System.out.println("HTTP response code: " + downloadConnection.getResponseCode());
      System.out.println("HTTP response message: " + downloadConnection.getResponseMessage());
      System.out.println("HTTP headers: " + downloadConnection.getHeaderFields());
      System.out.println("HTTP downloadUrl: " + downloadUrl);
      System.out.println("HTTP downloadBody: " + downloadBody);

    } catch (IOException | SdkClientException e) {
      e.printStackTrace();
    }

  }
}