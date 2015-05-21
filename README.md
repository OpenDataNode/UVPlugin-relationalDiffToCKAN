# L-RelationalDiffToCkan #
----------

###General###

|                              |                                                                               |
|------------------------------|-------------------------------------------------------------------------------|
|**Name:**                     |L-RelationalDiffToCkan                                                         |
|**Description:**              |Updates data in CKAN datastore from input relational data                      |
|                              |                                                                               |
|**DPU class name:**           |RelationalDiffToCkan                                                           |
|**Configuration class name:** |RelationalDiffToCkanConfig_V1                           |
|**Dialogue class name:**      |RelationalDiffToCkanVaadinDialog |


***

###Dialog configuration parameters###

|Parameter                                       |Description           |
|------------------------------------------------|----------------------|
|                                                |                      |

***
###Configuration parameters###
In order to work properly, this DPU needs configuration parameters properly set in UV backend config.properties

|Parameter                             |Description                             |
|--------------------------------------|----------------------------------------|
|**org.opendatanode.CKAN.secret.token**    |Token used to authenticate to CKAN, has to be set in backend.properties  |
|**org.opendatanode.CKAN.api.url** | URL where CKAN api is located, has to be set in backend.properties |

####Deprecated parameters###

These parameters are deprecated and kept only for backward compatibility with version 1.0.X.
They will be removed in 1.1.0 of DPU.

|Parameter                             |Description                             |
|------------------------------------------------|------------------------------------------|
|dpu.uv-l-relationalDiffToCkan.secret.token | alias to _org.opendatanode.CKAN.secret.token_  |
|dpu.uv-l-relationalDiffToCkan.catalog.api.url | alias to _org.opendatanode.CKAN.api.url_  |

####Examples####
```INI
org.opendatanode.CKAN.secret.token = 12345678901234567890123456789012
org.opendatanode.CKAN.api.url = ﻿http://localhost:9080/internalcatalog/api/action/internal_api
```

***

### Inputs and outputs ###

|Name          |Type           |DataUnit           |Description                                  |
|--------------|---------------|-------------------|---------------------------------------------|
|tablesInput   |i              |RelationalDataUnit |Tables to be updated in CKAN datastore       |

***

### Version history ###

|Version          |Release notes               |
|-----------------|----------------------------|
|1.0.2            | unification of config parameters |
|1.0.1            | bug fixes and update in build dependencies |
|1.0.0            | First version              |


***

### Developer's notes ###

|Author           |Notes                           |
|-----------------|--------------------------------|
|N/A              |Configuration and Dialog are empty |
