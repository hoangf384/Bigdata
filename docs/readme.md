# cấu trúc thư mục
bash-script
```bash
cd ~/project
tree -d -L 3
```

```
├── Airflow
│   ├── config
│   ├── dags
│   ├── logs
│   │   └── dag_processor
│   └── plugins
├── Code
│   ├── airflow-operators
│   ├── jupyter
│   ├── libs
│   └── spark-apps
│       └── scala
├── Data
│   ├── checkpoint
│   ├── destination
│   │   ├── 20220401
│   │   └── log_content
│   ├── output
│   ├── raw
│   │   ├── log_content
│   │   ├── __log_search
│   │   ├── log_search
│   │   └── socom
│   └── staging
├── Docs
├── DuckDB
│   ├── db
│   ├── init
│   └── query
├── Flowchart
├── Jupyter
├── Mysql
│   ├── conf.d
│   ├── data
│   │   ├── #innodb_redo
│   │   ├── #innodb_temp
│   │   ├── mydb
│   │   ├── mysql
│   │   ├── performance_schema
│   │   └── sys
│   └── init
├── queries
└── Spark
    ├── apps
    │   ├── batch
    │   ├── streaming
    │   └── utils
    ├── data
    ├── jars
    └── logs
```