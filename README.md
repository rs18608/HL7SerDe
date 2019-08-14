# HL7 Hive SerDe #


### What is this repository for? ###

* A customized Apache Hive SerDe for importing HL7 message files into a Hive table.
* 0.1


### How do I get set up? ###
* This project requires Apache Maven in order to build.  The following URL will help you with the installation and configuration of Maven on your system:   http://maven.apache.org/download.cgi
* After you have Maven installed and configured, run "mvn -U install" to generate the HL7SerDe.jar
* You must configure the hive-site.xml to include the following:


```
#!xml

<property> 
    <name>hive.aux.jars.path</name> 
    <value>file:///usr/local/hive/lib/Hl7SerDe.jar</value>   (use your Hive's installation path) 
</property>

```
* You must also copy the HL7SerDe.jar to the ./hive/lib directory.
* Hive must be restarted after making these changes.
* To use the HL7SerDe Hive SerDe you must specify "...ROW FORMAT SERDE "abaka.serde.HL7SerDe" STORED AS INPUTFORMAT "abaka.hadoop.HL7InputFormat" when creating a Hive table.  For example:



```
#!bash
CREATE TABLE hl7p (
patient_id string,
patient_name string,
dob string,
patient_gender string,
patient_address string,
obx ARRAY<STRUCT<
notes: string,
test_code: string,
test_name: string,
test_units: string,
test_results: string,
reference_range: string,
abnormal_flags: string,
probability: string,
obx_result_status: string,
date_of_obx: date,
producer_id: string,
responsible_observer: string,
obx_method: string,
obx_request_date: string,
ordering_provider: string,
order_description: string
>>)
ROW FORMAT SERDE "abaka.serde.HL7SerDe"
STORED AS INPUTFORMAT "abaka.hadoop.HL7InputFormat"
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
```
* The HL7Serde also supports custom field mappings.   What this means is that you can specify what HL7 value you want to map to a specific Hive column/field.   This functionality requires the Hive PARTITIONS feature and a mapping file in JSON format.

An example to create the Hive table with this option is as follows:


```
#!bash

CREATE TABLE my_hl7_table (
patient_id string,
patient_name string,
dob string,
patient_gender string,
patient_address string,
obx ARRAY<STRUCT<
notes: string,
test_code: string,
test_name: string,
test_units: string,
test_results: string,
reference_range: string,
abnormal_flags: string,
probability: string,
obx_result_status: string,
date_of_obx: date,
producer_id: string,
responsible_observer: string,
obx_method: string,
obx_request_date: string,
ordering_provider: string,
order_description: string
>>)
PARTITIONED BY (source string)
ROW FORMAT SERDE "abaka.serde.HL7SerDe"
STORED AS INPUTFORMAT "abaka.hadoop.HL7InputFormat"
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";```
```
Next you need to modify the table with the following commands to specify the partitions:



```
#!bash
alter table my_hl7_table add partition(source="<name of partition>") LOCATION "/user/hive/warehouse/my_hl7_table/<name of partition>";
alter table my_hl7_table add partition(source=“<name of partition>") LOCATION "/user/hive/warehouse/my_hl7_table/<name of partition>";


```


Of course you could include the above commands in the original "create" statement.

Next to support custom mappings, create the following directory structure in HDFS: "/hl7/mappings/".  Now for each Hive table partition you will need to create a directory with the exact name of your partition.  So for example, if you created the Hive table with partitions and you wanted to to have custom mappings from a source called "labcorp", then you would execute the following after creating the Hive table:

```
#!bash

alter table my_hl7_table add partition(source="labcorp") LOCATION "/user/hive/warehouse/my_hl7_table/labcorp";
```

After this you would create the following on HDFS:


```
#!bash

/hl7/mappings/labcorp
```

At this point you would want to put your JSON file into this folder.  The file must be named mapping.json.   Below is an example of the format:


```
#!JSON

{
    "patient_id": "/.PID-3",
    "patient_name": "/.PID-5",
    "dob": "/.PID-7",
    "patient_gender": "/.PID-8",
    "street": "/.PID-11",
    "city": "/.PID-11-3",
    "state": "/.PID-11-4",
    "zip": "/.PID-11-5",
    "notes": "/.OBX",
    "test_code": "/.OBX(0)-3-4",
    "test_name": "/.OBX(0)-3-5",
    "test_units": "/.OBX(0)-6",
    "test_results": "/.OBX(0)-5",
    "reference_range": "/.OBX(0)-7",
    "abnormal_flags": "/.OBX(0)-8",
    "probability": "/.OBX(0)-9",
    "obx_result_status": "/.OBX(0)-11",
    "date_of_obx": "/.OBX(0)-14",
    "producer_id": "/.OBX(0)-15",
    "responsible_observer": "/.OBX(0)-16",
    "obx_method": "/.OBX(0)-17",
    "obx_request_date": "/.OBX(0)-7",
    "ordering_provider": "/.OBX(0)-16",
    "order_description": "/.OBX(0)-4"
}

```

Note that in the above JSON code that the "key" exactly matches the name of the Hive table's column.  So for example the Hive column "patient_id" matches JSON key "partient_id".  The key's value would be the name of the HL7 field (prefixed with "/.") you want to map to the column.


Finally, to load data into this partition you would execute:


```
#!bash

load data inpath “<source on HDFS>" into table my_hl7_table PARTITION (source='labcorp');

```

From here you should be able to execute Hive queries.