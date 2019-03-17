

# Setting up Profile

Your database profile / credentials needs to be set up at `~/profile.yaml` or env var `PROFILE_YAML`.
Run command `xutil-create-profile` to create from template.

## Example Entry

```yaml
PG1:
  name: PG1
  host: pg1
  database: db1
  port: 5432
  user: user
  password: password
  type: postgresql
  url: "jdbc:postgresql://host:port/database?&ssl=false"
```

## SQL Server

### Using Windows Cred from Linux
<https://hammadr.wordpress.com/2017/09/26/ms-sql-connecting-python-on-linux-using-active-directory-credentials-and-kerberos/>



## ETL

### Database to Database

```python
from xutil.database.etl import db_table_to_ff_stream, db_to_db

# Select from Oracle
# Create into Hive via HDFS
db_to_db(
  src_db='ORCL_XENIAL',
  src_table='HR.EMPLOYEES',
  tgt_db='SPARK_HIVE',
  tgt_table='default.HR_EMPLOYEES',
  # partition_col="MANAGER_ID",
  # out_folder='/tmp',
  # hdfs_folder='tmp',
  partitions=5)

```