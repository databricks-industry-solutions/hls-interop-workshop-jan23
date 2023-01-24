# hls-interop-jan2023-workshop
1.24 Workshop Material for "Architecture &amp; Interoperability, Lakehouse Best Practices" 

To run, import this repo into your Databricks "repo" environment. Make sure you attach necessary cluster resources listed below for your use case.

Please feel free to submit an "issue" in this repo for help  

### Cluster Requirements and Recomendations

**Recommendations**

2 DBU per Hour
Runtime 9.1.x-scala2.12DBR 9.1 LTS (Spark 3.1.2 & Scala 2.12)  
1 **Driver** 8 GB Memory, 2 Cores   
4 **Workers** 32 GB Memory 8 Cores    


**Library Requirements**  

#### JDBC -> Database driver JAR file 
     - e.g. OHDSI has a fair amount consolidated on this page https://ohdsi.github.io/DatabaseConnectorJars/ 
     - For the presentation we conneted to another Spark instance. Jar is available on the OHDSI link under "Spark JDBC Driver"
#### FILE -> XML parsing 
     - For your cluster under "Libraries" -> Install New" -> "JAR" -> "MAVEN" and the resource name is "com.databricks:spark-xml_2.12:0.16.0"
#### HL7 -> Smolder JAR
     - https://github.com/databrickslabs/smolder/releases/tag/v0.0.1 contains the jar resource for you to attach to your cluster
#### FHIR -> pip install 
     - command is included in the beginning of the notebook
   
