package eu.unifiedviews.plugins.loader.relationaldifftockan;

public class CatalogApiConfig {

    private String catalogApiLocation;

    private String userId;

    private long pipelineId;

    private String token;

    public CatalogApiConfig(String catalogApiLocation, long pipelineId, String userId, String token) {
        this.catalogApiLocation = catalogApiLocation;
        this.pipelineId = pipelineId;
        this.userId = userId;
        this.token = token;
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

}
