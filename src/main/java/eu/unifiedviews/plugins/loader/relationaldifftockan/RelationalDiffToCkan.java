package eu.unifiedviews.plugins.loader.relationaldifftockan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.relational.RelationalDataUnit;
import eu.unifiedviews.dataunit.relational.RelationalDataUnit.Entry;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.relational.RelationalHelper;
import eu.unifiedviews.helpers.dataunit.resource.Resource;
import eu.unifiedviews.helpers.dataunit.resource.ResourceConverter;
import eu.unifiedviews.helpers.dataunit.resource.ResourceHelpers;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;
import eu.unifiedviews.plugins.loader.relationaldifftockan.DatastoreParams.DatastoreParamsBuilder;

/**
 * Loader - updates relational data in external CKAN catalog
 * This DPU loads all input database tables into resources in dataset mapped in CKAN to pipeline ID
 * If table (resource) does not exist yet, datastore table is created
 * If table (resource) already exists, data is updated
 */
@DPU.AsLoader
public class RelationalDiffToCkan extends AbstractDpu<RelationalDiffToCkanConfig_V1> {

    private static Logger LOG = LoggerFactory.getLogger(RelationalDiffToCkan.class);

    public static final String PROXY_API_ACTION = "action";

    public static final String PROXY_API_PIPELINE_ID = "pipeline_id";

    public static final String PROXY_API_USER_ID = "user_id";

    public static final String PROXY_API_TOKEN = "token";

    public static final String CKAN_API_URL_TYPE = "url_type";

    public static final String CKAN_API_URL_TYPE_DATASTORE = "datastore";

    public static final String CKAN_API_PACKAGE_SHOW = "package_show";

    public static final String CKAN_API_RESOURCE_UPDATE = "resource_update";

    public static final String CKAN_API_RESOURCE_CREATE = "resource_create";

    public static final String CKAN_API_RESOURCE_DELETE = "resource_delete";

    public static final String CKAN_API_DATASTORE_CREATE = "audited_datastore_create";

    public static final String CKAN_API_DATASTORE_UPDATE = "audited_datastore_update";

    public static final String CKAN_DATASTORE_TIMESTAMP = "update_time";

    public static final String CKAN_API_ACTOR_ID = "actor_id";

    public static final String PROXY_API_STORAGE_ID = "storage_id";

    public static final String PROXY_API_DATA = "data";

    public static final String CKAN_API_RESOURCE_ID = "id";

    public static final String CKAN_DATASTORE_RESOURCE_ID = "resource_id";

    public static final String CKAN_DATASTORE_FIELDS = "fields";

    public static final String CKAN_DATASTORE_RECORDS = "records";

    public static final String CKAN_DATASTORE_PRIMARY_KEY = "primary_key";

    public static final String CKAN_DATASTORE_INDEXES = "indexes";

    /**
     * @deprecated Global configuration should be used {@link CONFIGURATION_SECRET_TOKEN}
     */
    @Deprecated
    public static final String CONFIGURATION_DPU_SECRET_TOKEN = "dpu.uv-l-relationalDiffToCkan.secret.token";

    /**
     * @deprecated Global configuration should be used {@link CONFIGURATION_CATALOG_API_URL}
     */
    @Deprecated
    public static final String CONFIGURATION_DPU_CATALOG_API_URL = "dpu.uv-l-relationalDiffToCkan.catalog.api.url";

    public static final String CONFIGURATION_SECRET_TOKEN = "org.opendatanode.CKAN.secret.token";

    public static final String CONFIGURATION_CATALOG_API_URL = "org.opendatanode.CKAN.api.url";

    public static final String CONFIGURATION_HTTP_HEADER = "org.opendatanode.CKAN.http.header.";

    private DPUContext context;

    private Date pipelineStart;

    @DataUnit.AsInput(name = "tablesInput")
    public RelationalDataUnit tablesInput;

    public RelationalDiffToCkan() {
        super(RelationalDiffToCkanVaadinDialog.class, ConfigHistory.noHistory(RelationalDiffToCkanConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        this.context = this.ctx.getExecMasterContext().getDpuContext();
        this.pipelineStart = new Date();

        ContextUtils.sendShortInfo(this.ctx, "dpu.ckan.starting", this.getClass().getSimpleName());

        Map<String, String> environment = this.context.getEnvironment();
        long pipelineId = this.context.getPipelineId();
        String userId = (this.context.getPipelineExecutionOwnerExternalId() != null) ? this.context.getPipelineExecutionOwnerExternalId()
                : this.context.getPipelineExecutionOwner();
        String token = environment.get(CONFIGURATION_SECRET_TOKEN);
        if (isEmpty(token)) {
            token = environment.get(CONFIGURATION_DPU_SECRET_TOKEN);
            if (isEmpty(token)) {
                throw ContextUtils.dpuException(this.ctx, "errors.token.missing");
            }
        }
        String catalogApiLocation = environment.get(CONFIGURATION_CATALOG_API_URL);
        if (isEmpty(catalogApiLocation)) {
            catalogApiLocation = environment.get(CONFIGURATION_DPU_CATALOG_API_URL);
            if (isEmpty(catalogApiLocation)) {
                throw ContextUtils.dpuException(this.ctx, "errors.api.missing");
            }
        }

        Map<String, String> additionalHttpHeaders = new HashMap<>();
        for (Map.Entry<String, String> configEntry : environment.entrySet()) {
            if (configEntry.getKey().startsWith(CONFIGURATION_HTTP_HEADER)) {
                String headerName = configEntry.getKey().replace(CONFIGURATION_HTTP_HEADER, "");
                String headerValue = configEntry.getValue();
                additionalHttpHeaders.put(headerName, headerValue);
            }
        }

        CatalogApiConfig apiConfig = new CatalogApiConfig(catalogApiLocation, pipelineId, userId, token, additionalHttpHeaders);

        Set<RelationalDataUnit.Entry> internalTables;
        try {
            internalTables = RelationalHelper.getTables(this.tablesInput);
        } catch (DataUnitException ex) {
            ContextUtils.sendError(this.ctx, "errors.dpu.failed", ex, "errors.tables.iterator");
            return;
        }

        if (internalTables.size() != 1) {
            LOG.error("DPU can process only one input database table; Actual count of tables: {}", internalTables.size());
            throw ContextUtils.dpuException(this.ctx, "errors.input.tables");
        }

        Map<String, String> existingResources = getExistingResources(apiConfig);

        try {
            final RelationalDataUnit.Entry entry = internalTables.iterator().next();
            final String sourceTableName = entry.getTableName();
            final String resourceName = this.config.getResourceName();
            LOG.debug("Going to load table {} to CKAN dataset as resource {}", sourceTableName, resourceName);
            try {
                if (existingResources.containsKey(resourceName)) {
                    LOG.info("Resource already exists, overwrite mode is enabled -> resource will be updated");
                    String resourceId = existingResources.get(resourceName);
                    updateCkanResource(entry, resourceId, apiConfig);
                    updateAuditedDatastoreFromTable(entry, resourceId, apiConfig);
                    LOG.info("Resource and datastore {} for table {} successfully updated", resourceName, sourceTableName);
                    ContextUtils.sendShortInfo(this.ctx, "dpu.resource.updated", resourceName);

                } else {
                    LOG.info("Resource does not exist yet, it will be created");
                    String resourceId = createCkanResource(entry, apiConfig);
                    try {
                        createAuditedDatastoreFromTable(entry, resourceId, apiConfig);
                        LOG.info("Resource and datastore {} for table {} successfully created", resourceName, sourceTableName);
                    } catch (Exception e) {
                        LOG.debug("Failed to create datastore for resource {}, going to delete resource", resourceId);
                        deleteCkanResource(resourceId, apiConfig);
                    }
                    ContextUtils.sendShortInfo(this.ctx, "dpu.resource.created", resourceName);
                }
            } catch (Exception e) {
                LOG.error("Failed to create resource / datastore {} for table {}", resourceName, sourceTableName, e);
                this.context.sendMessage(DPUContext.MessageType.ERROR,
                        this.ctx.tr("dpu.resource.upload", resourceName, sourceTableName));
            }
        } catch (DataUnitException e) {
            throw ContextUtils.dpuException(this.ctx, e, "errors.dpu.upload");
        }
    }

    /**
     * Get a map of existing resources for this pipeline. Key in this map is the name of the resource,
     * in this case it is database table name. Value is resource ID - unique identifier of the resource, generated by CKAN
     * 
     * @return Map of existing resources from CKAN. Key is the name (table name), value is resource ID
     * @throws DPUException
     *             If error occurs obtaining resources from CKAN
     */
    private Map<String, String> getExistingResources(CatalogApiConfig apiConfig) throws DPUException {
        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        Map<String, String> existingResources = new HashMap<>();
        try {
            URIBuilder uriBuilder = new URIBuilder(apiConfig.getCatalogApiLocation());

            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            for (Map.Entry<String, String> additionalHeader : apiConfig.getAdditionalHttpHeaders().entrySet()) {
                httpPost.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
            }

            HttpEntity entity = MultipartEntityBuilder.create()
                    .addTextBody(PROXY_API_ACTION, CKAN_API_PACKAGE_SHOW, ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_PIPELINE_ID, String.valueOf(apiConfig.getPipelineId()), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_USER_ID, apiConfig.getUserId(), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_TOKEN, apiConfig.getToken(), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_DATA, "{}", ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .build();
            httpPost.setEntity(entity);
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Response from CKAN: {}", EntityUtils.toString(response.getEntity()));
                throw ContextUtils.dpuException(this.ctx, "dpu.resource.dataseterror", EntityUtils.toString(response.getEntity()));
            }
            JsonReaderFactory readerFactory = Json.createReaderFactory(Collections.<String, Object> emptyMap());
            JsonReader reader = readerFactory.createReader(response.getEntity().getContent());
            JsonObject responseJson = reader.readObject();

            if (!checkResponseSuccess(responseJson)) {
                throw ContextUtils.dpuException(this.ctx, "dpu.resource.responseerror");
            }

            JsonArray resources = responseJson.getJsonObject("result").getJsonArray("resources");
            for (JsonObject resource : resources.getValuesAs(JsonObject.class)) {
                existingResources.put(resource.getString("name"), resource.getString("id"));
            }

        } catch (ParseException | URISyntaxException | IllegalStateException | IOException ex) {
            throw ContextUtils.dpuException(this.ctx, ex, "errors.dpu.dataset");
        } finally {
            RelationalDiffToCkanHelper.tryCloseHttpResponse(response);
            RelationalDiffToCkanHelper.tryCloseHttpClient(client);
        }

        return existingResources;
    }

    /**
     * Create CKAN resource from internal relational database table.
     * This method should be called only in case resource for given table does not exist yet.
     * 
     * @param table
     *            Internal input database table, from which the resource should be created
     * @return Resource ID of the created resource
     * @throws Exception
     *             If problem occurs during creating of the resource
     */
    private String createCkanResource(RelationalDataUnit.Entry table, CatalogApiConfig apiConfig) throws Exception {
        String resourceId = null;

        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object> emptyMap());
        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        try {
            String resourceName = this.config.getResourceName();
            Resource resource = ResourceHelpers.getResource(this.tablesInput, table.getSymbolicName());
            resource.setName(resourceName);
            JsonObjectBuilder resourceBuilder = buildResource(factory, resource);

            URIBuilder uriBuilder = new URIBuilder(apiConfig.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            for (Map.Entry<String, String> additionalHeader : apiConfig.getAdditionalHttpHeaders().entrySet()) {
                httpPost.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
            }

            MultipartEntityBuilder builder = buildCommonResourceParams(table, apiConfig);
            builder.addTextBody(PROXY_API_DATA, resourceBuilder.build().toString(), ContentType.APPLICATION_JSON.withCharset("UTF-8"));
            builder.addTextBody(PROXY_API_ACTION, CKAN_API_RESOURCE_CREATE, ContentType.TEXT_PLAIN.withCharset("UTF-8"));

            HttpEntity entity = builder.build();
            httpPost.setEntity(entity);

            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                JsonReaderFactory readerFactory = Json.createReaderFactory(Collections.<String, Object> emptyMap());
                JsonReader reader = readerFactory.createReader(response.getEntity().getContent());
                JsonObject responseJson = reader.readObject();

                if (!checkResponseSuccess(responseJson)) {
                    throw ContextUtils.dpuException(this.ctx, "dpu.resource.responseerror");
                }

                if (!responseJson.getJsonObject("result").containsKey(CKAN_API_RESOURCE_ID)) {
                    throw new Exception("Missing resource ID of the newly created CKAN resource");
                }
                resourceId = responseJson.getJsonObject("result").getString(CKAN_API_RESOURCE_ID);
            } else {
                LOG.error("Response: {}", EntityUtils.toString(response.getEntity()));
                throw new Exception("Failed to create CKAN resource");
            }
        } catch (ParseException | IOException | DataUnitException | URISyntaxException e) {
            throw new Exception("Failed to create CKAN resource", e);
        } finally {
            RelationalDiffToCkanHelper.tryCloseHttpResponse(response);
            RelationalDiffToCkanHelper.tryCloseHttpClient(client);
        }

        return resourceId;
    }

    private void deleteCkanResource(String resourceId, CatalogApiConfig apiConfig) {
        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        try {

            URIBuilder uriBuilder = new URIBuilder(apiConfig.getCatalogApiLocation());

            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            for (Map.Entry<String, String> additionalHeader : apiConfig.getAdditionalHttpHeaders().entrySet()) {
                httpPost.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
            }

            HttpEntity entity = MultipartEntityBuilder.create()
                    .addTextBody(PROXY_API_ACTION, CKAN_API_RESOURCE_DELETE, ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_PIPELINE_ID, String.valueOf(apiConfig.getPipelineId()), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_USER_ID, apiConfig.getUserId(), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_TOKEN, apiConfig.getToken(), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .addTextBody(PROXY_API_DATA, RelationalDiffToCkanHelper.buildDeleteResourceParamters(resourceId).toString(),
                            ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                    .build();
            httpPost.setEntity(entity);
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Response: {}", EntityUtils.toString(response.getEntity()));
                ContextUtils.sendWarn(this.ctx, "errors.resource.delete", "errors.resource.delete.long");
            } else {
                if (!checkResponseSuccess(response)) {
                    ContextUtils.sendWarn(this.ctx, "errors.resource.delete", "errors.resource.delete.long");
                } else {
                    LOG.debug("Resource {} was successfully deleted", resourceId);
                }
            }
        } catch (IOException | URISyntaxException | ParseException e) {
            LOG.error("Exception occurred during deleting CKAN resource", e);
            ContextUtils.sendWarn(this.ctx, "errors.resource.delete", "errors.resource.delete.long");
        } finally {
            RelationalDiffToCkanHelper.tryCloseHttpResponse(response);
            RelationalDiffToCkanHelper.tryCloseHttpClient(client);
        }
    }

    /**
     * Update existing CKAN resource based on internal relational database table.
     * This method should be called only in case resource for given already exists and its resource ID is known
     * 
     * @param table
     *            Internal input database table, based on which the resource should be updated
     * @return Resource ID of the created resource
     * @throws Exception
     *             If problem occurs during updating of the resource
     */
    private void updateCkanResource(RelationalDataUnit.Entry table, String resourceId, CatalogApiConfig apiConfig) throws Exception {
        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object> emptyMap());
        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        try {
            String resourceName = this.config.getResourceName();
            Resource resource = ResourceHelpers.getResource(this.tablesInput, table.getSymbolicName());
            resource.setCreated(null);
            resource.setName(resourceName);
            JsonObjectBuilder resourceBuilder = buildResource(factory, resource);
            resourceBuilder.add(CKAN_API_RESOURCE_ID, resourceId);

            URIBuilder uriBuilder = new URIBuilder(apiConfig.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            for (Map.Entry<String, String> additionalHeader : apiConfig.getAdditionalHttpHeaders().entrySet()) {
                httpPost.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
            }

            MultipartEntityBuilder builder = buildCommonResourceParams(table, apiConfig);
            builder.addTextBody(PROXY_API_DATA, resourceBuilder.build().toString(), ContentType.APPLICATION_JSON.withCharset("UTF-8"));
            builder.addTextBody(PROXY_API_ACTION, CKAN_API_RESOURCE_UPDATE, ContentType.TEXT_PLAIN.withCharset("UTF-8"));

            HttpEntity entity = builder.build();
            httpPost.setEntity(entity);

            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                if (!checkResponseSuccess(response)) {
                    throw new Exception("Failed to update CKAN resource " + resourceId);
                }
            } else {
                LOG.error("Response: {}", EntityUtils.toString(response.getEntity()));
                throw new Exception("Failed to update CKAN resource " + resourceId);
            }
        } catch (ParseException | IOException | DataUnitException | URISyntaxException e) {
            throw new Exception("Error updating resource " + resourceId, e);
        } finally {
            RelationalDiffToCkanHelper.tryCloseHttpResponse(response);
            RelationalDiffToCkanHelper.tryCloseHttpClient(client);
        }
    }

    /**
     * Create CKAN datastore from input internal relational database table.
     * It obtains all the information about columns, column types, primary keys and indexes.
     * It creates the corresponding datastore definition and also sends all the data from the table
     * 
     * @param table
     *            Input database table, from which the datastore should be created
     * @param resourceId
     *            CKAN resource ID
     * @throws Exception
     *             If error occurs during creating of the datastore definition or data upload
     */
    private void createAuditedDatastoreFromTable(RelationalDataUnit.Entry table, String resourceId, CatalogApiConfig apiConfig) throws Exception {
        Connection conn = null;
        ResultSet tableData = null;
        Statement stmnt = null;
        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        String sourceTableName = table.getTableName();
        try {

            conn = this.tablesInput.getDatabaseConnection();
            List<ColumnDefinition> columns = RelationalDiffToCkanHelper.getColumnsForTable(conn, sourceTableName);
            JsonArray fields = RelationalDiffToCkanHelper.buildFieldsDefinitionJson(columns);

            stmnt = conn.createStatement();
            tableData = stmnt.executeQuery("SELECT * FROM " + sourceTableName);
            JsonArray records = RelationalDiffToCkanHelper.buildRecordsJson(tableData, columns);
            List<String> primaryKeys = RelationalDiffToCkanHelper.getTablePrimaryKeys(conn, sourceTableName);
            List<String> indexes = RelationalDiffToCkanHelper.getTableIndexes(conn, sourceTableName, primaryKeys);
            if (primaryKeys == null || primaryKeys.isEmpty()) {
                ContextUtils.sendError(this.ctx, "errors.primarykeys", "");
                throw new Exception("Failed to create audited datastore because primary keys are not defined in source table");
            }

            DatastoreParams dataStoreParams = DatastoreParamsBuilder.create().setResourceId(resourceId)
                    .setFields(fields)
                    .setPrimaryKeys(primaryKeys)
                    .setIndexes(indexes)
                    .setRecords(records)
                    .setModificationTimestamp(this.pipelineStart).build();
            JsonObject datastoreApiJson = RelationalDiffToCkanHelper.buildDataStoreParameters(dataStoreParams);

            URIBuilder uriBuilder = new URIBuilder(apiConfig.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            for (Map.Entry<String, String> additionalHeader : apiConfig.getAdditionalHttpHeaders().entrySet()) {
                httpPost.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
            }

            MultipartEntityBuilder builder = buildCommonResourceParams(table, apiConfig);
            builder.addTextBody(PROXY_API_DATA, datastoreApiJson.toString(), ContentType.APPLICATION_JSON.withCharset("UTF-8"));
            builder.addTextBody(PROXY_API_ACTION, CKAN_API_DATASTORE_CREATE, ContentType.TEXT_PLAIN.withCharset("UTF-8"));

            HttpEntity entity = builder.build();
            httpPost.setEntity(entity);

            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Response: {}", EntityUtils.toString(response.getEntity()));
                throw new Exception("Failed to create CKAN datastore");
            }

            if (!checkResponseSuccess(response)) {
                throw new Exception("Failed to create CKAN datastore");
            }

        } catch (DataUnitException | SQLException | URISyntaxException | IOException ex) {
            throw new Exception("Failed to create CKAN datastore", ex);
        } finally {
            RelationalDiffToCkanHelper.tryCloseDbResources(conn, stmnt, tableData);
            RelationalDiffToCkanHelper.tryCloseHttpResponse(response);
            RelationalDiffToCkanHelper.tryCloseHttpClient(client);
        }
    }

    /**
     * Updates data from input database table into CKAN datastore
     * Can be called only if resource and datastore already exist
     * 
     * @param table
     * @param resourceId
     * @param apiConfig
     * @throws Exception
     */
    private void updateAuditedDatastoreFromTable(Entry table, String resourceId, CatalogApiConfig apiConfig) throws Exception {
        Connection conn = null;
        ResultSet tableData = null;
        Statement stmnt = null;
        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        String sourceTableName = table.getTableName();

        try {

            conn = this.tablesInput.getDatabaseConnection();
            List<ColumnDefinition> columns = RelationalDiffToCkanHelper.getColumnsForTable(conn, sourceTableName);

            stmnt = conn.createStatement();
            tableData = stmnt.executeQuery("SELECT * FROM " + sourceTableName);
            JsonArray records = RelationalDiffToCkanHelper.buildRecordsJson(tableData, columns);
            DatastoreParams dataStoreParams = DatastoreParamsBuilder.create().setResourceId(resourceId)
                    .setRecords(records)
                    .setModificationTimestamp(this.pipelineStart).build();
            JsonObject datastoreApiJson = RelationalDiffToCkanHelper.buildDataStoreParameters(dataStoreParams);

            URIBuilder uriBuilder = new URIBuilder(apiConfig.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            for (Map.Entry<String, String> additionalHeader : apiConfig.getAdditionalHttpHeaders().entrySet()) {
                httpPost.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
            }

            MultipartEntityBuilder builder = buildCommonResourceParams(table, apiConfig);
            builder.addTextBody(PROXY_API_DATA, datastoreApiJson.toString(), ContentType.APPLICATION_JSON.withCharset("UTF-8"));
            builder.addTextBody(PROXY_API_ACTION, CKAN_API_DATASTORE_UPDATE, ContentType.TEXT_PLAIN.withCharset("UTF-8"));

            HttpEntity entity = builder.build();
            httpPost.setEntity(entity);

            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Response: {}", EntityUtils.toString(response.getEntity()));
                throw new Exception("Failed to update CKAN datastore");
            }

            if (!checkResponseSuccess(response)) {
                throw new Exception("Failed to update CKAN datastore");
            }

        } catch (DataUnitException | SQLException | URISyntaxException | IOException ex) {
            throw new Exception("Failed to update CKAN datastore", ex);
        } finally {
            RelationalDiffToCkanHelper.tryCloseDbResources(conn, stmnt, tableData);
            RelationalDiffToCkanHelper.tryCloseHttpResponse(response);
            RelationalDiffToCkanHelper.tryCloseHttpClient(client);
        }

    }

    /**
     * Builds common multipart parameters for resource API calls
     * 
     * @param table
     *            Input database table name
     * @return Common multi-part parameters
     * @throws DataUnitException
     *             If error occurs
     */
    private static MultipartEntityBuilder buildCommonResourceParams(RelationalDataUnit.Entry table, CatalogApiConfig apiConfig) throws DataUnitException {
        String storageId = table.getTableName();

        MultipartEntityBuilder builder = MultipartEntityBuilder.create()
                .addTextBody(PROXY_API_PIPELINE_ID, String.valueOf(apiConfig.getPipelineId()), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                .addTextBody(PROXY_API_USER_ID, apiConfig.getUserId(), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                .addTextBody(PROXY_API_TOKEN, apiConfig.getToken(), ContentType.TEXT_PLAIN.withCharset("UTF-8"))
                .addTextBody(PROXY_API_STORAGE_ID, storageId, ContentType.TEXT_PLAIN.withCharset("UTF-8"));

        return builder;
    }

    /**
     * Create JSON object builder for resource
     * 
     * @param factory
     *            JsonBuilderFactory to be used to create Json objects
     * @param resource
     *            Resource to create Json builder from
     * @return JSON representation of provided resource
     */
    private JsonObjectBuilder buildResource(JsonBuilderFactory factory, Resource resource) {

        JsonObjectBuilder resourceBuilder = factory.createObjectBuilder();
        for (Map.Entry<String, String> mapEntry : ResourceConverter.resourceToMap(resource).entrySet()) {
            resourceBuilder.add(mapEntry.getKey(), mapEntry.getValue());
        }

        Map<String, String> extrasMap = ResourceConverter.extrasToMap(resource.getExtras());
        if (extrasMap != null && !extrasMap.isEmpty()) {
            for (Map.Entry<String, String> mapEntry : extrasMap.entrySet()) {
                resourceBuilder.add(mapEntry.getKey(), mapEntry.getValue());
            }
        }

        if (this.context.getPipelineExecutionActorExternalId() != null) {
            resourceBuilder.add(CKAN_API_ACTOR_ID, this.context.getPipelineExecutionActorExternalId());
        }
        resourceBuilder.add(CKAN_API_URL_TYPE, CKAN_API_URL_TYPE_DATASTORE);
        // fix of CKAN issue - this URL must be present in CKAN v 2.3 in order to display data preview
        resourceBuilder.add("url", "_datastore_only_resource");

        return resourceBuilder;
    }

    private static boolean checkResponseSuccess(JsonObject responseJson) throws IllegalStateException, IOException {
        boolean bSuccess = responseJson.getBoolean("success");

        LOG.debug("CKAN success response value: {}", bSuccess);
        if (!bSuccess) {
            String errorMessage = responseJson.getJsonObject("error").toString();
            LOG.error("CKAN error response: {}", errorMessage);
        }

        return bSuccess;
    }

    private static boolean checkResponseSuccess(CloseableHttpResponse response) throws IllegalStateException, IOException {
        JsonReaderFactory readerFactory = Json.createReaderFactory(Collections.<String, Object> emptyMap());
        JsonReader reader = readerFactory.createReader(response.getEntity().getContent());
        JsonObject responseJson = reader.readObject();

        boolean bSuccess = responseJson.getBoolean("success");

        LOG.debug("CKAN success response value: {}", bSuccess);
        if (!bSuccess) {
            String errorMessage = responseJson.getJsonObject("error").toString();
            LOG.error("CKAN error response: {}", errorMessage);
        }

        return bSuccess;
    }

    private static boolean isEmpty(String value) {
        if (value == null || value.isEmpty()) {
            return true;
        }
        return false;
    }

}
