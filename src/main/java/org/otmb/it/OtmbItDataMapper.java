package org.otmb.it;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Getter
public class OtmbItDataMapper {

    private static final Logger logger = LoggerFactory.getLogger(OtmbItDataMapper.class);

    private final ObjectMapper objectMapper;

    public OtmbItDataMapper() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public <T> String toJson(T object, Class<T> clazz) throws JsonProcessingException {
        return this.objectMapper.writerFor(clazz).writeValueAsString(object);
    }

    public <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        return this.objectMapper.readerFor(clazz).readValue(json);
    }

    public String toMapJson(Map<String, Object> object) throws JsonProcessingException {
        return this.objectMapper.writeValueAsString(object);
    }

    public Map<String, Object> fromMapJson(String json) throws JsonProcessingException {
        TypeReference<HashMap<String, Object>> typeRef
                = new TypeReference<HashMap<String, Object>>() {};
        return this.objectMapper.readValue(json, typeRef);
    }
}
