package com.lambda.demo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Handler implements RequestHandler<S3Event, String> {

    //이미지 베이스 타입 (Content-Type)
    private final String baseType = "image/";

    //이미지를 줄이거나 늘릴 비율
    private final double SCALE = 0.5;

    //해당 확장자를 가진 이미지만 처리
    private final List<String> allowedTypes = List.of(".jpg", ".jpeg", ".png");

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        LambdaLogger logger = context.getLogger();

        logger.log(s3Event.getRecords().size() + " Images Uploaded Event Accepted \n");

        S3Client s3Client = null;
        InputStream inputStream = null;

        try{
            //S3 Event로 부터 해당 Bucket 이름, 파일 명을 받아옴
            S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

            //정규식을 이용하여 확장자 추출
            Matcher matcher = Pattern.compile("(.+/)*(.+)(\\..+)$").matcher(srcKey);
            if (!matcher.matches()) {
                logger.log("Unable to infer image type for key " + srcKey);
                return "";
            }

            String path = matcher.group(1);
            String fileName = matcher.group(2);
            String imgType = matcher.group(3);

            //원하는 이미지 타입이 아니면 무시
            if(!allowedTypes.contains(imgType)){
                logger.log(fileName + " has unsupported image type " + imgType);
                return "";
            }

            s3Client = S3Client.builder().build();
            inputStream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(srcBucket)
                    .key(srcKey)
                    .build());

            //이미지를 받아와서 메모리에 올림
            BufferedImage srcImage = ImageIO.read(inputStream);

            //받아온 이미지를 가지고 Resized된 이미지 생성
            BufferedImage newImage = resize(srcImage, SCALE);

            //크기를 조정한 이미지를 저장할 버킷
            String dstBucket = srcBucket + "-resized";

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(newImage, imgType.substring(1), outputStream);

            String contentType = baseType + imgType.substring(1);

            Map<String, String> metadata = new HashMap<String, String>();
            metadata.put("Content-Length", Integer.toString(outputStream.size()));
            metadata.put("Content-Type", contentType);


            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(dstBucket)
                    .key(srcKey)
                    .metadata(metadata)
                    .build();

            //크기 조정한 이미지를 버킷에 저장
            logger.log("Writing to: " + dstBucket + "/" + srcKey);
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(outputStream.toByteArray()));
            logger.log("Successfully resized " + srcBucket + "/" + srcKey + "and uploaded to " + dstBucket + "/" + srcKey);

            //기존 원본 이미지는 버킷에서 삭제
            logger.log("Deleting from: " + srcBucket + "/" + srcKey);
            s3Client.deleteObject(builder -> builder.bucket(srcBucket).key(srcKey));

            return "Ok";
        } catch (Exception e) {
            logger.log(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    logger.log("Error closing inputStream: " + e.getMessage());
                }
            }
            if (s3Client != null) {
                s3Client.close();
            }
        }
    }

    public BufferedImage resize(BufferedImage img, double scale) {
        int originalWidth = img.getWidth();
        int originalHeight = img.getHeight();

        // 비율을 유지하여 새로운 크기를 계산
        int newWidth = (int) (originalWidth * scale);
        int newHeight = (int) (originalHeight * scale);

        BufferedImage resizedImg = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = resizedImg.createGraphics();
        g.setPaint(Color.WHITE);
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g.drawImage(img, 0, 0, newWidth, newHeight, null);
        g.dispose();

        return resizedImg;
    }
}
