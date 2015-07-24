### Popis

Aktualizuje dáta v úložisku CKAN z relačných dát na vstupe

### Konfiguračné parametre dialógu

|Parameter|Description|
|:----|:----|
|**Názov zdroja CKAN:** |Názov zdroja CKAN, ktorý á vyť vytvorený, má prednosť pred vstupom z e-distributionMetadata a ak nie je zadaný, použije sa virtuálna cesta alebo symbolické meno ako meno zdroja|

### Konfiguračné parametre

| Meno | Popis |
|:----|:----|
|**org.opendatanode.CKAN.secret.token**|Reťazec použitý na CKAN autentifikáciu, nastavuje sa v backend.properties|
|**org.opendatanode.CKAN.api.url**|URL kde sa nachádza CKAN API, nastavuje sa v backend.properties|
|**org.opendatanode.CKAN.http.header.[key]**|Aktuálna HTTP hlavička pridaná k požiadavkam na CKAN|

#### Zastarané parametre

Nasledujúce parametre sú zastarané a uchované iba z dôvodu spätnej kompatibility s verziou 1.0.X.
Budú odstránené od verzie DPU 1.1.0.

| Meno | Popis |
|-----|-----|
|**dpu.uv-l-filesToCkan.secret.token**| alias k _org.opendatanode.CKAN.secret.token_  |
|**dpu.uv-l-filesToCkan.catalog.api.url** | alias k _org.opendatanode.CKAN.api.url_ |

#### Príklady
```INI
org.opendatanode.CKAN.secret.token = 12345678901234567890123456789012
org.opendatanode.CKAN.api.url = ﻿http://localhost:9080/internalcatalog/api/action/internal_api
org.opendatanode.CKAN.http.header.X-Forwarded-Host = www.myopendatanode.org
org.opendatanode.CKAN.http.header.X-Forwarded-Proto = https
```

### Vstupy a výstupy ###

|Meno |Typ | Dátová hrana | Popis | Povinné |
|:--------|:------:|:------:|:-------------|:---------------------:|
|tablesInput|i|RelationalDataUnit |Tabuľky, ktoré sa majú aktualizovať v úložisku CKAN|x|
|distributionInput|i|RDFDataUnit|Distribučné metadáta vytvorené z e-distributionMetadata||