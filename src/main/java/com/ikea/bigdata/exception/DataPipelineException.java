package com.ikea.bigdata.exception;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class DataPipelineException extends RuntimeException {

    public DataPipelineException(String message) {
        super(message);
    }
}
