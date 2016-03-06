# physician-compare



Build 
-----

To use the application you need to install [sbt](http://www.scala-sbt.org/).
After you install you are ready to run the application.

```
cd physician-compare
sbt assembly
```

Execute 
-------

In order to run analysis for relationship between PAC ID and NPI

    
    java -jar target/scala-2.11/physician_compare-assembly-1.0.jar PAC_ID
    
    
In order to run analysis for relationship between CCN and NPI
    
    
    java -jar target/scala-2.11/physician_compare-assembly-1.0.jar CCN_1
    
Output 
------

1. data/column_stats.json
    - column wise metrics on the csv

1. data/page_rank.json
    - top `com.shkr.physiciancompare.Configuration.PAGE_RANK_LIMIT`  
      values for selected column { here PAC_ID OR CCN 1 } with their
      page rank and associated properties

1. data/connected_components.json
    - top `com.shkr.physiciancompare.Configuration.PAGE_RANK_LIMIT`  
      clusters for each connected component by size for 
      selected column { here NPI & CCN 1 OR NPI & PAC ID} with their
      associated column values
      
Download
---
The application downloads the physician compare file available [here](https://data.medicare.gov/Physician-Compare/National-Downloadable-File/s63f-csi6#). 

Alternatively, if you have the file already you can copy it to `data/` path
from the root of the application. 

The file name must be `National_Downloadable_File.csv`.


