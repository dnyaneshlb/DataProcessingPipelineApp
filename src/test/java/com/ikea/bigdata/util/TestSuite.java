package com.ikea.bigdata.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestSuite {

    public static final String EVENT_PAYLOAD_JSON = "src/test/resources/cim-event_payload.json";

    public static final String EVENT_PAYLOAD_JSON_WITHOUT_CONTENT =
            "src/test/resources/mock/cim_event_payload_without_content.json";

    public static final String EVENT_PAYLOAD_JSON_WITHOUT_KEYS =
            "src/test/resources/mock/cim_event_payload_without_keys.json";

    public static final String EVENT_PAYLOAD_JSON_WITHOUT_SAC_NO =
            "src/test/resources/mock/cim_event_payload_without_sacno.json";

    public static final String EVENT_PAYLOAD_JSON_WITHOUT_SAC_UNIT =
            "src/test/resources/mock/cim_event_payload_without_sacunit.json";

    public static final String EVENT_PAYLOAD_JSON_WITHOUT_COUNTRY_CODE =
            "src/test/resources/mock/cim_event_payload_without_countrycode.json";

    public static final String EVENT_PAYLOAD_JSON_WITHOUT_TARGET =
            "src/test/resources/mock/cim_event_payload_without_target.json";

    public static final String EVENT_PAYLOAD_JSON_WITH_EMPTY_TARGETS =
            "src/test/resources/mock/cim_event_payload_with_empty_targets.json";

    public static final String EVENT_PAYLOAD_MALFORMED_JSON =
            "src/test/resources/mock/cim-event_payload_error.json";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String getEventPayloadJson(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }

}
