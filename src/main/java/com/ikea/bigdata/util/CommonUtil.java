package com.ikea.bigdata.util;

import com.ikea.bigdata.exception.FailureMetaData;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables;
import org.apache.commons.io.FileUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonUtil {
    public static String displaySizeByByteCount(long byteCount) {
        Preconditions.checkArgument(byteCount >= 0, "Byte count can not be less then 0");
        String message = FileUtils.byteCountToDisplaySize(byteCount);
        return "Size : " + message + " (" + byteCount + "  bytes)";
    }

    public static String getTimeStamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSXXX");
        Date date = new Date();
        return sdf.format(date.getTime());
    }

    public static FailureMetaData getDataValidationFailureResponse(
            String failedClassName, String errorDescription, String data) {
        return FailureMetaData.builder()
                .failedClass(failedClassName)
                .description(errorDescription)
                .precursorDataString(data)
                .timestamp(getTimeStamp())
                .build();
    }

    public static FailureMetaData getExceptionFailureResponse(
            String className, String data, Exception e) {
        return FailureMetaData.builder()
                .failedClass(className)
                .description(e.getMessage())
                .precursorDataString(data)
                .stackTrace(Throwables.getStackTraceAsString(e))
                .timestamp(getTimeStamp())
                .build();
    }
}
