import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1760573757717 = glueContext.create_dynamic_frame.from_catalog(database="data_pipeline_db", table_name="raw", transformation_ctx="AWSGlueDataCatalog_node1760573757717")

# Script generated for node Change Schema
ChangeSchema_node1760573911484 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1760573757717, mappings=[("source", "string", "source", "string"), ("year", "long", "year", "long"), ("mean", "double", "mean", "decimal")], transformation_ctx="ChangeSchema_node1760573911484")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1760573911484, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760570074385", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760574028581 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1760573911484, connection_type="s3", format="csv", connection_options={"path": "s3://data-pipeline-joaoventura/processed/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1760574028581")

job.commit()