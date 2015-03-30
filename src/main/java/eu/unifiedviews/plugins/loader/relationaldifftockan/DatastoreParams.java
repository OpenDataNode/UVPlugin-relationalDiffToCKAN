package eu.unifiedviews.plugins.loader.relationaldifftockan;

import java.util.Date;
import java.util.List;

import javax.json.JsonArray;

public class DatastoreParams {

    private List<String> primaryKeys;

    private List<String> indexes;

    private String resourceId;

    private JsonArray fields;

    private JsonArray records;

    private Date modificationTimestamp;

    private DatastoreParams(List<String> primaryKeys, List<String> indexes, String resourceId, JsonArray fields, JsonArray records, Date modificationTimestamp) {
        super();
        this.primaryKeys = primaryKeys;
        this.indexes = indexes;
        this.resourceId = resourceId;
        this.fields = fields;
        this.records = records;
        this.modificationTimestamp = modificationTimestamp;
    }

    public List<String> getPrimaryKeys() {
        return this.primaryKeys;
    }

    public List<String> getIndexes() {
        return this.indexes;
    }

    public String getResourceId() {
        return this.resourceId;
    }

    public JsonArray getFields() {
        return this.fields;
    }

    public JsonArray getRecords() {
        return this.records;
    }

    public Date getModificationTimestamp() {
        return this.modificationTimestamp;
    }

    public static class DatastoreParamsBuilder {

        public static DatastoreParamsBuilder create() {
            return new DatastoreParamsBuilder();
        }

        private List<String> primaryKeys;

        private List<String> indexes;

        private String resourceId;

        private JsonArray fields;

        private JsonArray records;

        private Date modificationTimestamp;

        public DatastoreParamsBuilder setPrimaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public DatastoreParamsBuilder setIndexes(List<String> indexes) {
            this.indexes = indexes;
            return this;
        }

        public DatastoreParamsBuilder setResourceId(String resourceId) {
            this.resourceId = resourceId;
            return this;
        }

        public DatastoreParamsBuilder setFields(JsonArray fields) {
            this.fields = fields;
            return this;
        }

        public DatastoreParamsBuilder setRecords(JsonArray records) {
            this.records = records;
            return this;
        }

        public DatastoreParamsBuilder setModificationTimestamp(Date modificationTimestamp) {
            this.modificationTimestamp = modificationTimestamp;
            return this;
        }

        public DatastoreParams build() {
            return new DatastoreParams(this.primaryKeys, this.indexes, this.resourceId,
                    this.fields, this.records, this.modificationTimestamp);
        }
    }

}
