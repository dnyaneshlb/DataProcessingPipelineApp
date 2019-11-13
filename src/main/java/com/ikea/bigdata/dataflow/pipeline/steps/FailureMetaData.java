package com.ikea.bigdata.dataflow.pipeline.steps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Describes failures in pipeline transforms
 */
@Builder
@Data
public class FailureMetaData implements Serializable {

    /**
     * Class name from where failure/exception occurs
     */
    private String failedClass;

    /**
     * Description about the failure/exception
     */
    private String description;

    /**
     * Precursor data on which failure/exception occurs
     */
    private String precursorDataString;

    /**
     * Deep stack trace detail
     */
    private String stackTrace;

    /**
     * timestamp for this failure event
     */
    private String timestamp;
}
