from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce, monotonically_increasing_id
from delta.tables import DeltaTable
from pyspark.sql import Window

spark.conf.set(
  "fs.azure.account.key.hospdata.dfs.core.windows.net",
   dbutils.secrets.get(scope="hosp-vault-scope", key="storage-connection")
  )

# Paths
silver_path = "abfss://silver@hospdata.core.windows.net/patient_flow"
gold_dim_patient = "abfss://gold_patient@<<Storageaccount_name>>.core.windows.net/dim_patient"
gold_dim_department = "abfss://gold_dept@<<Storageaccount_name>>.core.windows.net/dim_department"
gold_fact = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/fact_patient_flow"


silver_df = spark.read.format("delta").load(silver_path)
