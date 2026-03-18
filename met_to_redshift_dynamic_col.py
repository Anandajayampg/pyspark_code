from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, from_unixtime , concat_ws , when , upper , broadcast , to_utc_timestamp , rtrim, to_date
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor
import boto3
import time
import psycopg2

# -----------------------------
# Redshift COPY config
# -----------------------------
job_trigger = {"df_oz": False, "df_12z": False}

DB_CONFIG = {
    "dbname": "mt",
    "user": "informatica_user",
    "password": "421!5ca7H65d1a4",
    "host": "dev-aurora-postgresql-instance1.cajdxfri4upu.us-west-2.rds.amazonaws.com",
    "port": "5432"
}

CONFIG = {
    "df_oz": {
        "table": "psps.tx_h3_cfp_00z_clean",
        "s3": "s3://psps-datastore-dev/working/df_00z/"
    },
    "df_12z": {
        "table": "psps.tx_h3_cfp_12z_clean",
        "s3": "s3://psps-datastore-dev/working/df_12z/"
    },
    "df_tln": {
        "table": "psps.oa_structure_tline_2021_clean",
        "s3": "s3://psps-datastore-dev/working/df_tln/"
    },
    "df_score": {
        "table": "psps.tx_psps_2km_scope_target_clean",
        "s3": "s3://psps-datastore-dev/working/df_score/"
    },
    "df_time_place": {
        "table": "psps.meteorology_time_place_clean",
        "s3": "s3://psps-datastore-dev/working/df_time_place/"
    }
}
job_trigger = {
    "df_oz": True,
    "df_12z": True,
    "df_tln": True,
    "df_score": True,
    "df_time_place": True
}

table_name = []
# Build TABLES dynamically
table_name = [
    CONFIG[key]["table"]
    for key, enabled in job_trigger.items()
    if enabled and key in CONFIG
]
print("Tables to extract:", table_name)
def normalize_pg_type(dtype: str) -> str:
    if dtype is None:
        return None

    dtype = dtype.lower().strip()

    # Timestamp types
    if dtype.startswith("timestamp"):
        return "timestamp"
    if dtype == "bytea":
        return "binary"
    # Numeric types
    if dtype in ("numeric", "decimal"):
        return "decimal(18,6)"   # or "double" if you want strict precision

    # Character types
    if dtype in ("character varying", "varchar", "text"):
        return "string"

    # Integer types
    if dtype in ("integer", "int4"):
        return "int"

    if dtype in ("bigint", "int8"):
        return "bigint"

    # Boolean
    if dtype == "boolean":
        return "boolean"

    # Geometry or user-defined → string
    if dtype == "user-defined":
        return "string"

    return dtype

def extract_column_detail(table_name):
    try:
        if table_name == "met.oa_structure_tline_2021":
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'oa_structure_tline_2021';")
            columns = cursor.fetchall()
            dim_tline_column = {}
            for col, dtype in columns:
                dim_tline_column[col] = normalize_pg_type(dtype)
            cursor.close()
            conn.close()
            return dim_tline_column
        elif table_name == "met.tx_psps_2km_scope_target":
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tx_psps_2km_scope_target';")
            columns = cursor.fetchall()
            scope_target_column = {}
            for col, dtype in columns:
                scope_target_column[col] = normalize_pg_type(dtype)
            cursor.close()
            conn.close()    
            return scope_target_column
        elif table_name == "met.meteorology_time_place":
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'meteorology_time_place';")
            columns = cursor.fetchall()
            time_place_column = {}
            for col, dtype in columns:
                time_place_column[col] = normalize_pg_type(dtype)
            cursor.close()
            conn.close()

            return time_place_column
        elif table_name == "met.tx_h3_cfp_00z":
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tx_h3_cfp_00z';")
            columns = cursor.fetchall()
            df_00z_column = {}
            for col, dtype in columns:
                df_00z_column[col] = normalize_pg_type(dtype)
            cursor.close()
            conn.close()

            return df_00z_column
        elif table_name == "met.tx_h3_cfp_12z":
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tx_h3_cfp_12z';")
            columns = cursor.fetchall()
            df_12z_column = {}
            for col, dtype in columns:
                df_12z_column[col] = normalize_pg_type(dtype)   
            cursor.close()
            conn.close()

            return df_12z_column
        else:
            raise ValueError(f"Unknown table: {table_name}")
    except Exception as e:
        print(f"Error extracting column details for {table_name}: {e}")
        raise
# Always include the fact table
TABLES = ["psps.tx_2km_all_runs_with_structuretline_timeplace"]
S3_PATHS = ["s3://psps-datastore-dev/curated/fact_psps_tx_scoping/"]

# Dynamically append based on triggers
for key, enabled in job_trigger.items():
    if enabled and key in CONFIG:
        TABLES.append(CONFIG[key]["table"])
        S3_PATHS.append(CONFIG[key]["s3"])

print(TABLES)
print(S3_PATHS)

REDSHIFT_WORKGROUP = "psps-txscoping-dataextract-tfc"
REDSHIFT_DB = "gmcloudpspstxdb"
REDSHIFT_IAM_ROLE = "arn:aws:iam::412551746953:role/service-role/AmazonRedshift-CommandsAccessRole-20251229T161046"
AWS_REGION = "us-west-2"

def column_convert_spark():
    pass

def apply_casts_select(df, cast_config):
    select_exprs = []

    for col_name in df.columns:
        base_col = col(col_name)
        cast_type = cast_config.get(col_name)

        # If no cast type provided → keep as is
        if not cast_type:
            select_exprs.append(base_col.alias(col_name))
            continue

        # If dict says string → apply rtrim
        if cast_type == "string":
            expr = rtrim(base_col).cast("string")

        # Special conversions
        elif cast_type == "timestamp_millis":
            expr = from_unixtime(base_col / 1000).cast("timestamp")

        elif cast_type == "current_timestamp_long_zone":
            expr = to_utc_timestamp(
                from_unixtime(base_col / 1000),
                "America/Los_Angeles"
            ).cast("timestamp")

        elif cast_type == "current_timestamp_pacific":
            expr = to_utc_timestamp(base_col, "America/Los_Angeles")

        elif cast_type == "date_millis":
            expr = to_date(from_unixtime(base_col / 1000))

        else:
            # Normal cast using normalized type
            expr = base_col.cast(cast_type)

        select_exprs.append(expr.alias(col_name))

    return df.select(*select_exprs)
# -----------------------------
# Redshift COPY for ONE table
# -----------------------------
def copy_one_table(table, s3_path):
    try:
        client = boto3.client("redshift-data", region_name=AWS_REGION)
        # ------------------------- # 1. TRUNCATE TABLE # -------------------------
        truncate_sql = f"TRUNCATE TABLE {table};"
        print(f"\nTruncating {table}")
        response = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DB,
            Sql=truncate_sql
        )
        statement_id = response["Id"]
        print(f"{table} TRUNCATE submitted. Statement ID: {statement_id}")
        while True:
            status = client.describe_statement(Id=statement_id)
            state = status["Status"]
            if state in ["FINISHED", "FAILED", "ABORTED"]:
                print(f"{table} TRUNCATE final status: {state}")
                if state == "FAILED":
                    print("Error:", status.get("Error"))
                    raise Exception(f"Redshift TRUNCATE command failed for {table}")
                if state == "FINISHED":
                    print(f"{table} TRUNCATE completed successfully.")
                break
            time.sleep(2)
        sql = f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET;
        """

        print(f"\nSubmitting COPY for {table}")
        print(sql)

        response = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DB,
            Sql=sql
        )

        statement_id = response["Id"]
        print(f"{table} COPY submitted. Statement ID: {statement_id}")

        while True:
            status = client.describe_statement(Id=statement_id)
            state = status["Status"]
            if state in ["FINISHED", "FAILED", "ABORTED"]:
                print(f"{table} COPY final status: {state}")
                if state == "FAILED":
                    print("Error:", status.get("Error"))
                    raise Exception(f"Redshift COPY command failed for {table}")
                if state == "FINISHED":
                    print(f"{table} COPY completed successfully.")
                break
            time.sleep(2)

    except Exception as e:
        print(f"An error occurred during Redshift COPY for {table}: {e}")
        raise


# -----------------------------
# Run all COPYs in parallel
# -----------------------------
def copy_all_tables_parallel():
    with ThreadPoolExecutor(max_workers=len(TABLES)) as executor:
        futures = []
        for table, path in zip(TABLES, S3_PATHS):
            futures.append(executor.submit(copy_one_table, table, path))

        for f in futures:
            f.result()  # will raise if any COPY failed

    print("\nAll COPY operations completed.")


# -----------------------------
# Spark main job
# -----------------------------
def main():
    try:
        start_time = time.time()

        spark = (
            SparkSession.builder
            .appName("psps_tx_fact_builder")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.broadcastJoinThreshold", "200MB")
            .config ("spark.sql.autoBroadcastJoinThreshold","200MB")
            .getOrCreate()
        )

        # 1. Read 00z & 12z
        df_00z = spark.read.option("header", True).option("inferSchema", False).csv(
            "s3://psps-datastore-dev/aurora_extracts/met_tx_h3_cfp_00z.csv"
        )
        
        df_12z = spark.read.option("header", True).option("inferSchema",False).csv(
            "s3://psps-datastore-dev/aurora_extracts/met_tx_h3_cfp_12z.csv"
        )
        df_00z_column = extract_column_detail("met.tx_h3_cfp_00z")
        df_12z_column = extract_column_detail("met.tx_h3_cfp_12z")
        df_00z.printSchema()
        df_00z = apply_casts_select(df_00z, df_00z_column)
        df_12z = apply_casts_select(df_12z, df_12z_column)
        # 2. Filter condition
        print("test ts_model_run is 00z")
        df_00z.filter(col("ts_model_run_id").isNotNull()) \
        .select("ts_model_run_id") \
        .show()
        print("test ts_model_run is 12z")
        df_12z.filter(col("ts_model_run_id").isNotNull()) \
        .select("ts_model_run_id") \
        .show()


        filter_condition = (
            (col("psps_guidance_tx") == "below_guidance") |
            (col("psps_mfpc").isin(
                "minimum_fire_potential",
                "near_minimum_fire_potential"
            ))
        )
        
        cfp_00z = (
            df_00z
            .filter(filter_condition)
            .withColumn("model_run", lit("00z"))
        )

        cfp_12z = (
            df_12z
            .filter(filter_condition)
            .withColumn("model_run", lit("12z"))
        )
        print("test ts_model_run is 00z")
        cfp_00z.filter(col("ts_model_run_id").isNotNull()) \
        .select("ts_model_run_id") \
        .show()
        print("test ts_model_run is 12z")
        cfp_12z.filter(col("ts_model_run_id").isNotNull()) \
        .select("ts_model_run_id") \
        .show()


        # 3. Union filtered 00z + 12z
        cfp_00z = cfp_00z.withColumn("tx_00z_unique_identifier", concat_ws("_", cfp_00z.source, cfp_00z.model_run_id, cfp_00z.ts_model_run_id,
                                            cfp_00z.dt_utc, cfp_00z.etgis_id, cfp_00z.host_tline_nm)).withColumn("model_run", lit("00z"))
        cfp_12z = cfp_12z.withColumn("tx_12z_unique_identifier", concat_ws("_", cfp_12z.source, cfp_12z.model_run_id, cfp_12z.ts_model_run_id,
                                            cfp_12z.dt_utc, cfp_12z.etgis_id, cfp_12z.host_tline_nm)).withColumn("model_run", lit("12z"))
        
        if "cfpt_induction_grounded" not in cfp_00z.columns:
             cfp_00z = cfp_00z.withColumn("cfpt_induction_grounded", lit(None).cast(F.StringType()))
        if "cfpt_induction_ungrounded" not in cfp_00z.columns:
             cfp_00z = cfp_00z.withColumn("cfpt_induction_ungrounded", lit(None).cast(F.StringType()))
        cfp_00z = cfp_00z.dropDuplicates(["tx_00z_unique_identifier"])
        cfp_00z = cfp_00z.drop("tx_00z_unique_identifier")
        if "cfpt_induction_grounded" not in cfp_12z.columns:
             cfp_12z = cfp_12z.withColumn("cfpt_induction_grounded", lit(None).cast(F.StringType()))
        if "cfpt_induction_ungrounded" not in cfp_12z.columns:
             cfp_12z = cfp_12z.withColumn("cfpt_induction_ungrounded", lit(None).cast(F.StringType()))
        cfp_12z = cfp_12z.dropDuplicates(["tx_12z_unique_identifier"])
    # Droping duplicate records coming from source database in Foundry via db sink connection : tx_12z_unique_identifier col
        cfp_12z = cfp_12z.drop("tx_12z_unique_identifier")
        # cfp_00z 
        full_union = cfp_00z.unionByName(cfp_12z)
        # lfp_df.createOrReplaceTempView("lfp")
        print("Full union")
        print(full_union.printSchema())

        # 4. Read dimensions
        dim_tline = spark.read.option("header", True).option("inferSchema",False).csv(
            "s3://psps-datastore-dev/aurora_extracts/met_oa_structure_tline.csv"
        )
        dim_tline_column = extract_column_detail("met.oa_structure_tline_2021")
        dim_tline = apply_casts_select(dim_tline,dim_tline_column)
        met_oa_structure_tline = dim_tline. withColumn("tx_oa_structure_tline_unique_identifier",
                                                                concat_ws("_", dim_tline.etgis_id,
                                                                          dim_tline.sap_func_loc_no,
                                                                          dim_tline.load_date,
                                                                          dim_tline.partition_0))
        met_oa_structure_tline = met_oa_structure_tline.dropDuplicates(["tx_oa_structure_tline_unique_identifier"])
        # Droping duplicate records coming from source database in Foundry via db sink
        # connection:tx_oa_structure_tline_unique_identifier col
        met_oa_structure_tline = met_oa_structure_tline.drop("tx_oa_structure_tline_unique_identifier")
        met_oa_structure_tline = broadcast(met_oa_structure_tline)
        print("met_oa_structure_tline")
        print(met_oa_structure_tline.printSchema())
        # scope_target 
        scope_target = spark.read.option("header", True).option("inferSchema",False).csv(
            "s3://psps-datastore-dev/aurora_extracts/met_tx_psps_2km_scope_target.csv"
        )
        scope_target_column = extract_column_detail("met.tx_psps_2km_scope_target")
        scope_target = apply_casts_select(scope_target,scope_target_column)

        met_tx_psps_2km_scope_target = scope_target.withColumn("tx_psps_2km_scope_target_unique_identifier",
                                                                           concat_ws("_",
                                                                                     scope_target.
                                                                                     etgis_id,
                                                                                     scope_target.
                                                                                     scope_version,
                                                                                     scope_target.
                                                                                     time_place_id))
        met_tx_psps_2km_scope_target = met_tx_psps_2km_scope_target.dropDuplicates(["tx_psps_2km_scope_target_unique_identifier"])
        # Droping duplicate records coming from source database in Foundry via db sink
        # connection : tx_psps_2km_scope_target_unique_identifier col
        met_tx_psps_2km_scope_target = met_tx_psps_2km_scope_target.drop("tx_psps_2km_scope_target_unique_identifier")
        met_tx_psps_2km_scope_target = broadcast(met_tx_psps_2km_scope_target)

        print("met_tx_psps_2km_scope_target")
        print(met_tx_psps_2km_scope_target.printSchema())
        time_place = spark.read.option("header", True).csv(
            "s3://psps-datastore-dev/aurora_extracts/met_meteorology_time_place.csv"
        )
        time_place_column = extract_column_detail("met.tx_meteorology_time_place")
        time_place = apply_casts_select(time_place,time_place_column)
               
        met_meteorology_time_place = time_place.withColumn("tx_meteorology_time_place_unique_identifier",
                                                                        concat_ws("_", time_place.
                                                                                    scope_version,
                                                                                    time_place.
                                                                                    time_place_id))
        met_meteorology_time_place = met_meteorology_time_place.dropDuplicates(["tx_meteorology_time_place_unique_identifier"])
        # Droping duplicate records coming from source database in Foundry via db sink
        # connection : tx_meteorology_time_place_unique_identifier col
        met_meteorology_time_place = met_meteorology_time_place.drop("tx_meteorology_time_place_unique_identifier")
        print("met_meteorology_time_place")
        print(met_meteorology_time_place.printSchema())
        met_meteorology_time_place = broadcast(met_meteorology_time_place)
        full_union = full_union.repartition("etgis_id")
        fact_df = (
        full_union.join(met_oa_structure_tline, "etgis_id", "left")
        .join(met_tx_psps_2km_scope_target, ["etgis_id", "model_run_id"], "left")
        .join(met_meteorology_time_place, ["time_place_id", "scope_version"], "left")
        .select(
            full_union.model_run,
            full_union.model_run_id,
            full_union.ts_model_run_id,
            full_union.dt_local,
            full_union.psps_guidance_tx,
            full_union.psps_guidance_tx_summary, #added new column psps_guidance_tx_summary PSP-78
            full_union.psps_mfpc, #added new column psps_mfpc for ui filter
            full_union.pomms2km_we_sn,
            full_union.etgis_id,
            full_union.host_tline_nm,
            full_union.cfpt,
            full_union.cfpt_induction,
            full_union.oa_pf_wg,
            full_union.oa_pfprime_wg,
            full_union.area_acres_8hr,
            full_union.buildings_8hr,
            full_union.population_8hr,
            full_union.prob_cat,
            full_union.prob_small,
            full_union.prob_large,
            full_union.prob_critical,
            full_union.prob_critical_or_cat,
            full_union.prob_large_critical_or_cat,
            full_union.ws_mph,
            full_union.ws_mph_300m,
            full_union.tke_pbl_50m,
            full_union.rh_2m,
            full_union.dfm_10hr,
            full_union.dfm_100hr,
            full_union.dfm_1000hr,
            full_union.lfm_chamise_new,
            full_union.lfm_herb,
            full_union.geom,
            full_union.source,
            full_union.flame_length_ft_8hr,
            full_union.rate_of_spread_chhr_8hr,
            full_union.temp_f_2m,
            full_union.vpd_mb_2m,
            full_union.ustar_frc_vel,
            full_union.location,
            full_union.wg_cf_mph,
            full_union.ws_mph_50m,
            full_union.tke_pbl_300m,
            full_union.temp_f_50m,
            full_union.temp_f_300m,
            full_union.vpd_mb_50m,
            full_union.vpd_mb_300m,
            full_union.smois_0,
            full_union.dfm_1hr,
            full_union.lfm_chamise_old,
            full_union.avg_fuel_complexity,
            full_union.fuel_bed_depth_ft,
            full_union.slope_degree_mean,
            full_union.terrain_rugged_mean,
            full_union.h3_08,
            full_union.solar_rad,
            full_union.ndvi,
            full_union.cfpt_induction_grounded, # added new column cfpt_induction_grounded, Jira PSP-84
            full_union.cfpt_induction_ungrounded, # added new column cfpt_induction_ungrounded, Jira PSP-84
            met_oa_structure_tline.structure_no,
            met_oa_structure_tline.sap_equip_id.cast("int").alias("sap_equip_id"),
            met_oa_structure_tline.sap_func_loc_no,
            met_oa_structure_tline.host_guest,
            met_oa_structure_tline.tline_nm,
            met_oa_structure_tline.tline_etgisid,
            met_oa_structure_tline.hfra.cast("int").alias("hfra"),
            met_oa_structure_tline.all_clear_zone,
            met_oa_structure_tline.ratedkv.cast("int").alias("ratedkv"),
            met_oa_structure_tline.latitude.alias("latitude"),
            met_oa_structure_tline.longitude.alias("longitude"),
            met_oa_structure_tline.fia,
            met_tx_psps_2km_scope_target.time_place_id,
            met_meteorology_time_place.time_place_name,
            met_tx_psps_2km_scope_target.scope_version,
            met_meteorology_time_place.weather_start_time,
            met_meteorology_time_place.weather_end_time
        )
        .withColumn("location", when((col("location") == lit('hfra')) | (col("location") == lit('buffer')), upper(col("location"))).otherwise(lit('NA'))) #show location column in upper case
        .withColumn("processed_at",to_utc_timestamp(current_timestamp(), lit("America/Los_Angeles")))
    )

        # 6. Write 00z, 12z, and fact to S3 as Parquet
        #    (you can change partitioning if you decide differently later)
    #   from pyspark.sql.functions import current_timestamp, to_utc_timestamp, lit

        df_00z = df_00z.withColumn(
            "processed_at",
            to_utc_timestamp(current_timestamp(), lit("America/Los_Angeles"))
        )

        df_12z = df_12z.withColumn(
            "processed_at",
            to_utc_timestamp(current_timestamp(), lit("America/Los_Angeles"))
        )

        if job_trigger.get("df_oz"):
            # df_00z.select("area_acres_8hr", "avg_fuel_complexity", "buildings_8hr", "cfpt", "cfpt_induction", "cfpt_induction_grounded", "cfpt_induction_ungrounded", "dfm_1000hr", "dfm_100hr", "dfm_10hr", "dfm_1hr", "dt_local", "dt_utc", "etgis_id", "flame_length_ft_8hr", "fuel_bed_depth_ft", "geom", "h3_08", "host_tline_nm", "ignition_prob_grounded", "ignition_prob_ungrounded", "index", "latitude", "lfm_chamise_new", "lfm_chamise_old", "lfm_herb", "location", "longitude", "model_run_id", "ndvi", "oa_pf_wg", "oa_pfprime_wg", "pomms2km_we_sn", "population_8hr", "prob_cat", "prob_critical", "prob_critical_or_cat", "prob_large", "prob_large_critical_or_cat", "prob_small", "psps_guidance_tx", "psps_guidance_tx_summary", "psps_mfpc", "rate_of_spread_chhr_8hr", "rh_2m", "slope_degree_mean", "smois_0", "solar_rad", "source", "temp_f_2m", "temp_f_300m", "temp_f_50m", "terrain_rugged_mean", "tke_pbl_300m", "tke_pbl_50m", "ts_model_run_id", "ustar_frc_vel", "vpd_mb_2m", "vpd_mb_300m", "vpd_mb_50m", "wg_cf_mph", "wg_ec_mph", "ws_mph", "ws_mph_300m", "ws_mph_50m","processed_at").write.mode("overwrite") \
            #     .parquet("s3://psps-datastore-dev/working/df_00z/")
            pass
        print("Schema of df_00z:")
        df_00z.printSchema()
        print("df_12z schema:")
        df_12z.printSchema()
        print("fact schema")
        # fact_df.printSchema()
        if job_trigger.get("df_12z"):
            # df_12z.select("area_acres_8hr", "avg_fuel_complexity", "buildings_8hr", "cfpt", "cfpt_induction", "cfpt_induction_grounded", "cfpt_induction_ungrounded", "dfm_1000hr", "dfm_100hr", "dfm_10hr", "dfm_1hr", "dt_local", "dt_utc", "etgis_id", "flame_length_ft_8hr", "fuel_bed_depth_ft", "geom", "h3_08", "host_tline_nm", "ignition_prob_grounded", "ignition_prob_ungrounded", "index", "latitude", "lfm_chamise_new", "lfm_chamise_old", "lfm_herb", "location", "longitude", "model_run_id", "ndvi", "oa_pf_wg", "oa_pfprime_wg", "pomms2km_we_sn", "population_8hr", "prob_cat", "prob_critical", "prob_critical_or_cat", "prob_large", "prob_large_critical_or_cat", "prob_small", "psps_guidance_tx", "psps_guidance_tx_summary", "psps_mfpc", "rate_of_spread_chhr_8hr", "rh_2m", "slope_degree_mean", "smois_0", "solar_rad", "source", "temp_f_2m", "temp_f_300m", "temp_f_50m", "terrain_rugged_mean", "tke_pbl_300m", "tke_pbl_50m", "ts_model_run_id", "ustar_frc_vel", "vpd_mb_2m", "vpd_mb_300m", "vpd_mb_50m", "wg_cf_mph", "wg_ec_mph", "ws_mph", "ws_mph_300m", "ws_mph_50m","processed_at").write.mode("overwrite") \
            #     .parquet("s3://psps-datastore-dev/working/df_12z/")
            pass
        fact_df.select("ts_model_run_id").show()
        fact_df.printSchema()
        fact_df.write.mode("overwrite") \
            .parquet("s3://psps-datastore-dev/curated/fact_psps_tx_scoping/")
        # print("save in bucket")
        # fact_df.write \
        # .bucketBy(200, "sap_func_loc_no") \
        # .sortBy("sap_func_loc_no") \
        # .mode("overwrite") \
        # .saveAsTable("bucketed_tx_2km_cfp_allruns")

        # print(f"\nSpark job completed in {round(time.time() - start_time, 2)/60} minutes")

    except Exception as e:
        print("FAILED: Error during MET TX Fact Builder job")
        print("Error:", e)
        raise


if __name__ == "__main__":
    # export_post()
    job_timing = time.time()
    main()
    copy_all_tables_parallel()
    job__end = time.time()
    print(f"\n job completed and data copied to Redshift Table tx_2km_all_runs_with_structuretline_timeplace in {round(job__end - job_timing, 2)/60} minutes.")
