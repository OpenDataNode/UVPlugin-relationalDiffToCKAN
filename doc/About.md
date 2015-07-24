### Description

Updates data in CKAN datastore from input relational data

### Configuration parameters

|Parameter|Description|
|:----|:----|
|**CKAN resource name** | Name of CKAN resource to be created, this has precedence over input from e-distributionMetadata, and if even that is not set, it will use VirtualPath or symbolic name as resource name |

### Inputs and outputs

|Name |Type | DataUnit | Description | Mandatory |
|:--------|:------:|:------:|:-------------|:---------------------:|
|tablesInput       |i| RelationalDataUnit |Tables to be updated in CKAN datastore |x|
|distributionInput |i| RDFDataUnit|Distribution metadata produced by e-distributionMetadata ||