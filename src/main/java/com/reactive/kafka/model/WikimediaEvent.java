package com.reactive.kafka.model;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikimediaEvent {
    @JsonProperty("meta")
    private Meta meta;
    @JsonProperty("id")
    private Long id;
    @JsonProperty("type")
    private String type;
    @JsonProperty("namespace")
    private Integer namespace;
    @JsonProperty("title")
    private String title;
    @JsonProperty("user")
    private String user;
    @JsonProperty("bot")
    private Boolean bot;
    @JsonProperty("server_name")
    private String serverName;
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @Data
    public static class Meta {
        @JsonProperty("uri")
        private String uri;
        @JsonProperty("request_id")
        private String requestId;
        @JsonProperty("id")
        private String id;
        @JsonProperty("dt")
        private String dt;
        @JsonProperty("domain")
        private String domain;
        @JsonProperty("stream")
        private String stream;
        @JsonProperty("topic")
        private String topic;
    }
}
