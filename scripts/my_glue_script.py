import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_S3', 'TARGET_S3'])

# Initialize Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV from source
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['SOURCE_S3']]},
    format="csv",
    format_options={"withHeader": True}
)

# Transform - drop null fields
cleaned_data = datasource.drop_null_fields()

# Write to target
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_data,
    connection_type="s3",
    connection_options={"path": args['TARGET_S3']},
    format="csv"
)

job.commit()
