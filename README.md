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

|Parameter                                       |Description                                                              |
|------------------------------------------------|-------------------------------------------------------------------------|
|**CKAN resource name**                          |Name of CKAN resource to be created                                      |
|                                                |                                                                         |

***
###Configuration parameters###
In order to work properly, this DPU needs configuration parameters properly set in UV backend config.properties

|Parameter                             |Description                             |
|--------------------------------------|----------------------------------------|
|**org.opendatanode.CKAN.secret.token**    |Token used to authenticate to CKAN, has to be set in backend.properties  |
|**org.opendatanode.CKAN.api.url** | URL where CKAN api is located, has to be set in backend.properties |
|**org.opendatanode.CKAN.http.header.[key]** | Custom HTTP header added to requests on CKAN |

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
org.opendatanode.CKAN.http.header.X-Forwarded-Host = www.myopendatanode.org
org.opendatanode.CKAN.http.header.X-Forwarded-Proto = https
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
|1.1.0            | Resource name in CKAN is now configured by user; Only one table can be processed by DPU |
|                 | Changes in DPU API v 2.1.0, new actor ID parameter is sent to CKAN if available |
|1.0.2            | Added possibility to define custom HTTP headers and unification of config parameters |
|1.0.1            | bug fixes and update in build dependencies |
|1.0.0            | First version              |


***

### Developer's notes ###

|Author           |Notes                           |
|-----------------|--------------------------------|
|N/A              |N/A                             |
