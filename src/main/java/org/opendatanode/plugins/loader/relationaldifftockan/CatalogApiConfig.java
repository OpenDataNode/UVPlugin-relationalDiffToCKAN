package org.opendatanode.plugins.loader.relationaldifftockan;

import java.util.Map;

public class CatalogApiConfig {

    private String catalogApiLocation;

    private String userId;

    private long pipelineId;

    private String token;

    private Map<String, String> additionalHttpHeaders;

    public CatalogApiConfig(String catalogApiLocation, long pipelineId, String userId, String token, Map<String, String> additionalHttpHeaders) {
        this.catalogApiLocation = catalogApiLocation;
        this.pipelineId = pipelineId;
        this.userId = userId;
        this.token = token;
        this.additionalHttpHeaders = additionalHttpHeaders;
    }

    public String getCatalogApiLocation() {
        return this.catalogApiLocation;
    }

    public String getUserId() {
        return this.userId;
    }

    public long getPipelineId() {
        return this.pipelineId;
    }

    public String getToken() {
        return this.token;
    }

    public Map<String, String> getAdditionalHttpHeaders() {
        return this.additionalHttpHeaders;
    }

}
