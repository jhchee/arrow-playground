import awswrangler as wr
import os

wr.config.s3_endpoint_url = 'http://localhost:9000'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'password'
os.environ['AWS_ACCESS_KEY_ID'] = 'admin'

df = wr.s3.read_json(path="s3://warehouse/json_input/", orient="columns")
df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour
res = wr.s3.to_parquet(df=df,
                       path="s3://warehouse/parquet_output/",
                       pyarrow_additional_kwargs={"compression_level": 9},  # this is set to 1 by default
                       dataset=True,
                       compression="zstd",
                       partition_cols=["date", "hour"],
                       mode="overwrite"
                       )
print(res)
