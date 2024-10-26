import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_regex_extract
from awsglue import DynamicFrame
import gs_derived

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1729968371864 = glueContext.create_dynamic_frame.from_catalog(database="alura-datalake-aws", table_name="bronzebronze", transformation_ctx="AWSGlueDataCatalog_node1729968371864")

# Script generated for node Change Schema
ChangeSchema_node1729968599258 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1729968371864, mappings=[("case_enquiry_id", "long", "case_enquiry_id", "bigint"), ("open_dt", "string", "open_dt", "timestamp"), ("target_dt", "string", "target_dt", "timestamp"), ("closed_dt", "string", "closed_dt", "timestamp"), ("ontime", "string", "ontime", "string"), ("case_status", "string", "case_status", "string"), ("closure_reason", "string", "closure_reason", "string"), ("case_title", "string", "case_title", "string"), ("subject", "string", "subject", "string"), ("reason", "string", "reason", "string"), ("type", "string", "type", "string"), ("queue", "string", "queue", "string"), ("department", "string", "department", "string"), ("submittedphoto", "string", "submittedphoto", "string"), ("closedphoto", "string", "closedphoto", "string"), ("location", "string", "location", "string"), ("fire_district", "string", "fire_district", "string"), ("pwd_district", "string", "pwd_district", "string"), ("city_council_district", "string", "city_council_district", "string"), ("police_district", "string", "police_district", "string"), ("neighborhood", "string", "neighborhood", "string"), ("neighborhood_services_district", "string", "neighborhood_services_district", "string"), ("ward", "string", "ward", "string"), ("precinct", "string", "precinct", "string"), ("location_street_name", "string", "location_street_name", "string"), ("location_zipcode", "double", "location_zipcode", "string"), ("latitude", "double", "latitude", "string"), ("longitude", "double", "longitude", "string"), ("source", "string", "source", "string")], transformation_ctx="ChangeSchema_node1729968599258")

# Script generated for node Regex Extractor
RegexExtractor_node1729969354482 = ChangeSchema_node1729968599258.gs_regex_extract(colName="closure_reason", regex="^Case Closed\. Closed date :  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+  (.*)", newCols="closure_reason_normalized")

# Script generated for node Derived Column Year
DerivedColumnYear_node1729969988971 = RegexExtractor_node1729969354482.gs_derived(colName="open_year", expr="YEAR(open_dt)")

# Script generated for node Derived Column Month
DerivedColumnMonth_node1729970061132 = DerivedColumnYear_node1729969988971.gs_derived(colName="open_month", expr="MONTH(open_dt)")

# Script generated for node SQL Query
SqlQuery0 = '''
select tb_1.*,
round((unix_timestamp(closed_dt)-unix_timestamp(open_dt))/3600,0) AS duration_hours
from tb_1
'''
SQLQuery_node1729970113836 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"tb_1":DerivedColumnMonth_node1729970061132}, transformation_ctx = "SQLQuery_node1729970113836")

# Script generated for node Change Schema Final
ChangeSchemaFinal_node1729970316992 = ApplyMapping.apply(frame=SQLQuery_node1729970113836, mappings=[("case_enquiry_id", "bigint", "case_enquiry_id", "long"), ("open_dt", "timestamp", "open_dt", "timestamp"), ("target_dt", "timestamp", "target_dt", "timestamp"), ("closed_dt", "timestamp", "closed_dt", "timestamp"), ("ontime", "string", "ontime", "string"), ("case_status", "string", "case_status", "string"), ("case_title", "string", "case_title", "string"), ("subject", "string", "subject", "string"), ("reason", "string", "reason", "string"), ("type", "string", "type", "string"), ("queue", "string", "queue", "string"), ("department", "string", "department", "string"), ("submittedphoto", "string", "submittedphoto", "string"), ("closedphoto", "string", "closedphoto", "string"), ("location", "string", "location", "string"), ("fire_district", "string", "fire_district", "string"), ("pwd_district", "string", "pwd_district", "string"), ("city_council_district", "string", "city_council_district", "string"), ("police_district", "string", "police_district", "string"), ("neighborhood", "string", "neighborhood", "string"), ("neighborhood_services_district", "string", "neighborhood_services_district", "string"), ("ward", "string", "ward", "string"), ("precinct", "string", "precinct", "string"), ("location_street_name", "string", "location_street_name", "string"), ("location_zipcode", "string", "location_zipcode", "string"), ("latitude", "string", "latitude", "string"), ("longitude", "string", "longitude", "string"), ("source", "string", "source", "string"), ("closure_reason", "string", "closure_reason_normalized", "string"), ("open_dt", "timestamp", "duration_hours", "double"), ("open_dt", "timestamp", "open_year", "int"), ("open_dt", "timestamp", "open_month", "int")], transformation_ctx="ChangeSchemaFinal_node1729970316992")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1729970480061 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchemaFinal_node1729970316992, database="alura-datalake-aws", table_name="silver", additional_options={"partitionKeys": ["open_year", "open_month"]}, transformation_ctx="AWSGlueDataCatalog_node1729970480061")

job.commit()
