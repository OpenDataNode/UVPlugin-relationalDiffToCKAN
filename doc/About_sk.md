### Popis

Aktualizuje dáta v úložisku CKAN z relačných dát na vstupe

### Konfiguračné parametre

| Meno | Popis |
|:----|:----|
|**Názov zdroja CKAN** | Názov zdroja CKAN, ktorý á vyť vytvorený, má prednosť pred vstupom z e-distributionMetadata a ak nie je zadaný, použije sa virtuálna cesta alebo symbolické meno ako meno zdroja |

### Vstupy a výstupy ###

|Meno |Typ | Dátová hrana | Popis | Povinné |
|:--------|:------:|:------:|:-------------|:---------------------:|
|tablesInput       |i| RelationalDataUnit | Tabuľky, ktoré sa majú aktualizovať v úložisku CKAN|x|
|distributionInput |i| RDFDataUnit| Distribučné metadáta vytvorené z e-distributionMetadata ||