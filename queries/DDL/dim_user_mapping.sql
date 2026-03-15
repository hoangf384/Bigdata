CREATE OR REPLACE TABLE `bigdata.dim_user_mapping`
(
  user_id STRING NOT NULL OPTIONS(description="ID người dùng (Business Key)"),
  profile_1 STRING,
  profile_2 STRING,
  profile_3 STRING,
  profile_4 STRING,
  profile_5 STRING,
  create_date DATE DEFAULT CURRENT_DATE(),
  status STRING OPTIONS(description="active/inactive"),
  from_date DATE NOT NULL OPTIONS(description="Ngày bắt đầu hiệu lực"),
  to_date DATE OPTIONS(description="Ngày hết hiệu lực (9999-12-31 nếu là bản ghi mới nhất)"),
  PRIMARY KEY (user_id, from_date) NOT ENFORCED
)
PARTITION BY create_date 
CLUSTER BY user_id, status;