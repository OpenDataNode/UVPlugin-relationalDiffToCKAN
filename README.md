# L-RelationalDiffToCkan #
----------

###General###

|                              |                                                                               |
|------------------------------|-------------------------------------------------------------------------------|
|**Name:**                     |L-RelationalDiffToCkan                                                         |
|**Description:**              |Updates data in CKAN datastore from input relational data                      |
|                              |                                                                               |
|**DPU class name:**           |RelationalDiffToCkan                                                           |


***

###Configuration parameters###

|Parameter                                       |Description                                                              |
|------------------------------------------------|-------------------------------------------------------------------------|
|                                                |                                                                         |

***

###Global UnifiedViews configuration###
In order to work properly, this DPU needs configuration parameters properly set in UV backend config.properties

|Parameter                                       |Description                                                              |
|------------------------------------------------|-------------------------------------------------------------------------|
|dpu.uv-l-relationalDiffToCkan.catalog.api.url   |URL of CKAN catalog with proper UV extension installed                   |
|dpu.uv-l-relationalDiffToCkan.secret.token      |Authentication token to CKAN                                             |

***

### Inputs and outputs ###

|Name          |Type           |DataUnit           |Description                                  |
|--------------|---------------|-------------------|---------------------------------------------|
|tablesInput   |i              |RelationalDataUnit |Tables to be updated in CKAN datastore       |

***

### Version history ###

|Version          |Release notes               |
|-----------------|----------------------------|
|1.0.1            | bug fixes and update in build dependencies |
|1.0.0            | First version              |


***

### Developer's notes ###

|Author           |Notes                           |
|-----------------|--------------------------------|
|N/A              |N/A                             |
