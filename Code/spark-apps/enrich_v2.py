import polars as pl 

data = pl.read_ndjson("./Data/destination/log_search/category/mapping.jsonl")
print(data["category"].unique())