# NextETL project
  This project is used for us to migrate the data from sqlserver to postgres with Spark. you can just configure your source table/view/query and the target table. The data will be moved to target. It supports baseline and incremental delta data update.


## Requirements
```
    Spark Version: 2.4.0
    Scala Version: 2.11.12
    Java Version: 1.8.0_201
    IDEA
```
## Features
* support baseline and delta
* support to build jar package with necessary dependencies.
* environment configurable


## Building From Source
The package is built by Maven. when you get the source code, run below command to build your own package.
```
    mvn package
```
The output jar package includes all dependencies.