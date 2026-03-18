from pyspark.sql import functions as F, Window
from pyspark.sql.functions import broadcast , spark_partition_id
from pyspark.sql import SparkSession
import time
from concurrent.futures import ThreadPoolExecutor
import boto3
import json
spark = (
        SparkSession.builder
        .appName("psps_tx_unified_data")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.broadcastJoinThreshold","200MB")
        .config("spark.sql.autoBroadcastJoinThreshold", "200MB")
        .getOrCreate()
    )
# -----------------------------
# Redshift COPY config
# -----------------------------
# TABLES = [
#     "psps.tx_h3_cfp_00z_clean",
#     "psps.tx_h3_cfp_12z_clean"
# ]

TABLES = [
    "psps.tx_scoping_unified_source"
]

# S3_PATHS = [
#     "s3://psps-datastore-dev/working/df_00z/",
#     "s3://psps-datastore-dev/working/df_12z/"
# ]
S3_PATHS = [
    "s3://psps-datastore-dev/working/unified_data/"
]

column_mapping = {
    "Line_Type": "line_type",
    "Line_Removal_Status": "line_removal_status",
    "Potential_Impacted_Subs": "potential_impacted_subs",
    "Idle_Line_Status": "idle_line_status",
    "County": "county",
    "Energization_Status": "energization_status",
    "Foreign_Owner": "foreign_owner",
    "In_2020_HFTD": "in_2020_hftd",
    "What_is_the_section": "what_is_the_section",
    "Work_Center": "work_center",
    "Customers_at_Risk": "customers_at_risk",
    "Customer_Type": "customer_type",
    "Avg_Potential_Impacted_Customer_Count": "avg_potential_impacted_customer_count",
    "Max_Total_Count": "max_total_count"
}


REDSHIFT_WORKGROUP = "psps-txscoping-dataextract-tfc"
REDSHIFT_DB = "gmpspstxdb"
REDSHIFT_IAM_ROLE = "arn:aws:iam::412551746953:role/service-role/AmazonRedshift-CommandsAccessRole-20251229T161046"
AWS_REGION = "us-west-2"

def read_status(name):

    s3 = boto3.client("s3")
    BUCKET = "psps-datastore-dev"
    PREFIX = "state/"
    key = f"{PREFIX}{name}.json"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)["status"]

def wait_for_sources():
 
    sources = ["met"]   # do NOT include unified here
    max_retries = 5
    retry_count = 0

    while True:
        statuses = {src: read_status(src) for src in sources}
        unified_status = read_status("unified")

        print("Source statuses:", statuses, "| Unified:", unified_status)

        # Case 1: Unified already running
        if unified_status == "RUNNING":
            if retry_count >= max_retries:
                print("Unified still running after 5 retries. Stopping wait.")
                return False
            print(f"Unified job running. Waiting 60 seconds... (retry {retry_count+1}/5)")
            retry_count += 1
            time.sleep(60)
            continue

        # Case 2: Any source failed
        if any(v == "FAILED" for v in statuses.values()):
            print("A source job FAILED. Unified will not run.")
            return False

        # Case 3: Any source still running
        if any(v == "RUNNING" for v in statuses.values()):
            if retry_count >= max_retries:
                print("Sources still running after 5 retries. Stopping wait.")
                return False
            print(f"Sources still running. Waiting 60 seconds... (retry {retry_count+1}/5)")
            retry_count += 1
            time.sleep(60)
            continue

        # Case 4: All sources SUCCESS
        if all(v == "SUCCESS" for v in statuses.values()):
            print("All sources SUCCESS. Unified can run.")
            return True



def update_status(source_name, status):
    S3_BUCKET = "psps-datastore-dev"
    STATUS_PREFIX = "state/"   # folder where status files live

    s3 = boto3.client("s3")
    key = f"{STATUS_PREFIX}{source_name}.json"
    body = json.dumps({"status": status})

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json"
    )

    print(f"Updated status: {source_name} to {status}")
    print(f"S3 path: s3://{S3_BUCKET}/{key}")
def get_redshift_columns(table):
    try:
        client = boto3.client("redshift-data", region_name=AWS_REGION)

        schema, table_name = table.split(".")

        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table_name}_stg'
            ORDER BY ordinal_position;
        """

        print(f"\nFetching columns for {table}")
        print(sql)

        # Submit query
        response = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DB,
            Sql=sql
        )
        statement_id = response["Id"]
        print(f"{table} column fetch submitted. Statement ID: {statement_id}")

        # Poll for completion
        while True:
            status = client.describe_statement(Id=statement_id)
            state = status["Status"]

            if state in ["FINISHED", "FAILED", "ABORTED"]:
                print(f"{table} column fetch final status: {state}")
                if state == "FAILED":
                    print("Error:", status.get("Error"))
                    raise Exception(f"Redshift column fetch failed for {table}")
                break

            time.sleep(1)

        # Get results
        result = client.get_statement_result(Id=statement_id)
        rows = result.get("Records", [])

        # Extract column names
        columns = [row[0].get("stringValue") for row in rows]

        print(f"Columns for {table}: {columns}")
        return columns

    except Exception as e:
        print(f"Error fetching columns for {table}: {e}")
        raise

def add_missing_columns_to_redshift(table, spark_df, redshift_cols):
    try:
        print(f"\nChecking for missing columns in {table}_stg...")
        client = boto3.client("redshift-data", region_name=AWS_REGION)

        schema, table_name = table.split(".")
        table_name = f"{table_name}_stg"
        spark_cols = [f.name for f in spark_df.schema.fields]

        missing = [c for c in spark_cols if c not in redshift_cols]
        extra   = [c for c in redshift_cols if c not in spark_cols]

        print(f"Missing columns in Redshift for {table}: {missing}")
        print(f"Extra columns in Redshift for {table}: {extra}")

        # ---------------------------------------------------------
        # ADD missing columns
        # ---------------------------------------------------------
        for col in missing:
            spark_type = spark_df.schema[col].dataType.simpleString()

            if spark_type.startswith("decimal"):
                rs_type = spark_type.replace("decimal", "NUMERIC").upper()
            elif spark_type == "string":
                rs_type = "VARCHAR(5000)"
            elif spark_type == "int":
                rs_type = "INTEGER"
            elif spark_type in ("bigint", "long"):
                rs_type = "BIGINT"
            elif spark_type == "double":
                rs_type = "DOUBLE PRECISION"
            elif spark_type == "float":
                rs_type = "FLOAT4"
            elif spark_type == "timestamp":
                rs_type = "TIMESTAMP"
            elif spark_type == "date":
                rs_type = "DATE"
            else:
                rs_type = "VARCHAR(5000)"

            sql = f"""
                ALTER TABLE {schema}.{table_name}
                ADD COLUMN {col} {rs_type};
            """

            print(f"\nAdding missing column: {col} ({rs_type})")
            response = client.execute_statement(
                WorkgroupName=REDSHIFT_WORKGROUP,
                Database=REDSHIFT_DB,
                Sql=sql
            )
            statement_id = response["Id"]

            while True:
                status = client.describe_statement(Id=statement_id)
                state = status["Status"]
                if state in ["FINISHED", "FAILED", "ABORTED"]:
                    if state == "FAILED":
                        print("Error:", status.get("Error"))
                        raise Exception(f"Failed to add column {col} to {table}")
                    break
                time.sleep(1)

        # ---------------------------------------------------------
        # DROP extra columns
        # ---------------------------------------------------------
        for col in extra:
            sql = f"""
                ALTER TABLE {schema}.{table_name}
                DROP COLUMN {col};
            """

            print(f"\nDropping extra column: {col}")
            response = client.execute_statement(
                WorkgroupName=REDSHIFT_WORKGROUP,
                Database=REDSHIFT_DB,
                Sql=sql
            )
            statement_id = response["Id"]

            while True:
                status = client.describe_statement(Id=statement_id)
                state = status["Status"]
                if state in ["FINISHED", "FAILED", "ABORTED"]:
                    if state == "FAILED":
                        print("Error:", status.get("Error"))
                        raise Exception(f"Failed to drop column {col} from {table}")
                    break
                time.sleep(1)

        print(f"\nSchema sync completed for {table}_stg")

    except Exception as e:
        print(f"Error syncing schema for {table}_stg: {e}")
        raise


def reorder_fact_df_columns(fact_df, redshift_cols):
    # Only keep columns that exist in fact_df
    final_cols = [c for c in redshift_cols if c in fact_df.columns]
    return fact_df.select(*final_cols)

class ReadAllSource:

    def __init__(self):
        self.tx_2km_cfp_allruns = None
        self.hni_hnu = None
        self.veg_risk_score = None
        self.openab_tags = None
        self.pole_segments = None
        self.veg_risk_score_updated = None
        # self.oa_data_select = None

    def compute(self):

        tx_2km_cfp_allruns = self.tx_2km_cfp_allruns.repartition("sap_func_loc_no")
        hni_hnu = broadcast(self.hni_hnu)
        veg_risk_score = broadcast(self.veg_risk_score)
        openab_tags = broadcast(self.openab_tags)
        pole_segments = broadcast(self.pole_segments)
        veg_risk_score_updated = broadcast(self.veg_risk_score_updated)
        # oa_data_select = broadcast(oa_data_select)

        # ---------------------------------------------------------
        # CLEAN OPENAB TAGS
        # ---------------------------------------------------------
        # count_val = openab_tags.count()
        # openab_tags = openab_tags.withColumn("count_of_notification", F.lit(count_val).cast("long"))

        openab_tags = openab_tags.select(
            F.col('etl').alias('openab_etl'),
            F.col('priority').alias('openab_priority'),
            F.col('notification').alias('openab_notification'),
            F.col('equipment_number').alias('openab_equipment_number'),
            F.col('structure_number').alias('openab_structure_number'),
            F.col('status').alias('openab_status'),
            F.col('scheduled_start').alias('openab_scheduled_start'),
            F.col('scheduled_end').alias('openab_scheduled_end'),
            F.col("long_text").alias("openab_long_text"),
            F.col("gis_latitude").alias("openab_gis_latitude"),
            F.col("gis_longitude").alias("openab_gis_longitude"),
            "count_of_notification"
        )

        # ---------------------------------------------------------
        # CLEAN POLE SEGMENTS
        # ---------------------------------------------------------
        pole_segments = pole_segments.select(
            F.col('sap_func_loc_no').alias('pole_segments_sap_func_loc_no'),
            F.col('etgis_id').alias('pole_segments_etgis_id'),
            F.col('sap_structure_no').alias('pole_segments_structure_no'),
            F.col('sap_equip_id').alias('pole_segments_sap_equip_id'),
            F.col('segments').alias('pole_segments_segments'),
            F.col('hfra').alias('pole_segments_hfra'),
            F.col('hftd').alias('pole_segments_hftd'),
            "bounds_1", "bounds_2", "bounds_3", "bounds_4", "bounds_5", "bounds_6"
        )
        pole_segments.select("pole_segments_segments").show(5, truncate= False)
        # ---------------------------------------------------------
        # CLEAN HNI/HNU
        # ---------------------------------------------------------
        hni_hnu = hni_hnu.select(
            F.col('line_name').alias('hni_hnu_line_name'),
            F.col('itreelocid').alias('hni_hnu_itreelocid'),
            F.col('itreerecsid').alias('hni_hnu_itreerecsid'),
            F.col('priority').alias('hni_hnu_priority'),
            F.col('snotification').alias('hni_hnu_notification'),
            F.col('status').alias('hni_hnu_status'),
            F.col('remarks').alias('hni_hnu_longtext'),
            F.col('sap_functional_location'),
            F.col('tree_rec_lat').cast("double"),
            F.col('tree_rec_lon').cast("double")
        )
        # If the source schema no longer has `riskscore`, build a stable `risk_score_sum` column anyway.
        if "riskscore" in veg_risk_score_updated.columns:
            risk_score_sum_col = (
                (F.col("riskscore") * F.col("probablity_any_tree_hit_on_structure").cast("double"))
                .cast("long")
            )
        elif "risk_score_sum" in veg_risk_score_updated.columns:
            risk_score_sum_col = F.col("risk_score_sum").cast("long")
        else:
            risk_score_sum_col = F.lit(None).cast("long")

        # Ensure veg_risk_score_updated contains risk_score_sum for downstream joins
        veg_risk_score_updated = (
            veg_risk_score_updated
                .withColumn("risk_score_sum", risk_score_sum_col)
                .drop("tree_count")
        )

        # select this column from veg_risk_score_updated  |-- tree_count: integer (nullable = true)
#  |-- product_probability_not_to_fail: string (nullable = true)
#  |-- probablity_any_tree_hit_on_structure: string (nullable = true)
#  |-- nn_sap_fl: string (nullable = true)
#  |-- nn_etgis_id: string (nullable = true)
        # veg_risk_score_updated = veg_risk_score_updated.select("nn_sap_fl", "nn_etgis_id", "risk_score_sum", "probablity_any_tree_hit_on_structure", "product_probability_not_to_fail")
        veg_risk_score = veg_risk_score.withColumnRenamed("latitude", "veg_latitude") \
                               .withColumnRenamed("longitude", "veg_longitude")
        

        base_columns_met = tx_2km_cfp_allruns.columns
        print("Base MET columns:", base_columns_met)
        # veg_risk_score = veg_risk_score.withColumnRenamed("latitude", "veg_latitude") \
        #                        .withColumnRenamed("longitude", "veg_longitude")


    #     final_df = final_df.select(*base_columns_met, *base_columns_hni_hnu, *base_columns_veg_risk_score, *base_columns_veg_risk_score_updated, *base_columns_openab_tags, *base_columns_pole_segments)
    #    # ---------------------------------------------------------
        # JOIN CONDITIONS
        # ---------------------------------------------------------
        tx_2km_cfp_allruns = tx_2km_cfp_allruns.select("*", F.concat_ws("_", "etgis_id", "sap_func_loc_no").alias("join_key"))
        veg_risk_score = veg_risk_score.select("*", F.concat_ws("_", "nearest_etgisid", "sap_func_loc").alias("join_key")) 
        veg_risk_score_updated = veg_risk_score_updated.select("*", F.concat_ws("_", "nn_etgis_id", "nn_sap_fl").alias("join_key"))
        pole_segments = pole_segments.select("*", F.concat_ws("_", "pole_segments_etgis_id", "pole_segments_sap_func_loc_no").alias("join_key"))

        # ---------------------------------------------------------
        # FINAL JOIN
        # ---------------------------------------------------------
        final_df = (
           tx_2km_cfp_allruns.join(hni_hnu, tx_2km_cfp_allruns["sap_func_loc_no"] == hni_hnu["sap_functional_location"], "left") 
           .join(veg_risk_score, "join_key", "left") 
           .join(openab_tags, tx_2km_cfp_allruns["sap_func_loc_no"] == openab_tags["openab_etl"], "left") 
           .join(pole_segments, "join_key", "left") 
           .join(veg_risk_score_updated, "join_key", "left")
            )
        # final_df = final_df.select(*base_columns_met, all the column)
        final_df = final_df.cache()
        final_df.count()
        final_df.printSchema()
        print("fact table after joins")
        # print(final_df.show(5))
        # final_df.cache()
        # final_df = final_df.limit(12363)
        final_df = final_df.withColumn("cfb", F.when(F.col("psps_guidance_tx").isin(["catastrophic_behavior", "catastrophic_behavior_and_probability", "catastrophic_behavior_and_induction_ungrounded_probability"]), 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("cfpt_asset", F.when(F.col("psps_guidance_tx").isin(["catastrophic_probability", "catastrophic_induction_ungrounded_probability", "catastrophic_behavior_and_probability", "catastrophic_behavior_and_induction_ungrounded_probability"]), 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("cfpt_induction_measure", F.when(F.col("psps_guidance_tx").isin(["catastrophic_induction_probability", "catastrophic_behavior_and_induction_probability"]), 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("cfpt_induction_ungrounded_measure", F.when(F.col("psps_guidance_tx").isin(["catastrophic_induction_ungrounded_probability", "catastrophic_behavior_and_induction_ungrounded_probability"]), 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("fire_potential", F.when(F.col("psps_mfpc") == "minimum_fire_potential", 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("near_fpc", F.when(F.col("psps_mfpc") == "near_minimum_fire_potential", 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("hni_hnu_risk", F.when(F.col("sap_functional_location").isNotNull(), 1).otherwise(F.lit(0).cast("double"))) \
                   .withColumn("open_a_tags", F.when((F.col("openab_etl").isNotNull()) & (F.col("openab_priority") == "A"), 1).otherwise(F.lit(0).cast("double")))
        final_df = final_df.withColumn('total_time', (F.col('weather_end_time').cast("long") - F.col('weather_start_time').cast("long"))/(60*60))
        final_df = final_df.withColumn("Line_Type", F.lit(None).cast("string")) \
                   .withColumn("Line_Removal_Status", F.lit(None).cast("string")) \
                   .withColumn("Potential_Impacted_Subs", F.lit(None).cast("string")) \
                   .withColumn("Idle_Line_Status", F.lit(None).cast("string")) \
                   .withColumn("County", F.lit(None).cast("string")) \
                   .withColumn("Energization_Status", F.lit(None).cast("string")) \
                   .withColumn("Foreign_Owner", F.lit(None).cast("string")) \
                   .withColumn("In_2020_HFTD", F.lit(None).cast("string")) \
                   .withColumn("What_is_the_section", F.lit(None).cast("string")) \
                   .withColumn("Work_Center", F.lit(None).cast("string")) \
                   .withColumn("Customers_at_Risk", F.lit(None).cast("string")) \
                   .withColumn("Customer_Type", F.lit(None).cast("string")) \
                   .withColumn("Avg_Potential_Impacted_Customer_Count", F.lit(None).cast("string")) \
                   .withColumn("Max_Total_Count", F.lit(None).cast("string"))
        
       


        final_df = final_df.na.fill("ETL.XXXX", ["pole_segments_segments"]) \
                            .withColumn("section", F.explode(F.split(F.col("pole_segments_segments"), ",  "))) \
                            .withColumn("unexploded_section", F.col("pole_segments_segments")) \
                            .withColumn("bounds_first_seg", F.col("bounds_1")) \
                            .withColumn("hftd", F.col("pole_segments_hftd")) \
                            .withColumn("source", F.concat(F.initcap(F.col("source")), F.lit("."))) \
                            .withColumn(
                                "model_source",
                                F.concat(
                                    F.date_format(F.col("model_run_id"), "yyyy-MM-dd"),
                                    F.lit(" "),
                                    F.col("model_run"),
                                    F.lit(" "),
                                    F.initcap(F.col("source")),
                                    F.lit(".")
                                )
                            ) \
                            .withColumn("tree_id_count", F.col("treecount").cast("long")) \
                            .withColumn("pole_segment_st_no", F.col("pole_segments_structure_no")) \
                            .withColumn("area_acres_8hr", F.col("area_acres_8hr").cast("double")) \
                            .withColumn("cfpt", F.col("cfpt").cast("double")) \
                            .withColumn("cfpt_induction", F.col("cfpt_induction").cast("double")) \
                            .withColumn("dfm_1000hr", F.col("dfm_1000hr").cast("double")) \
                            .withColumn("dfm_100hr", F.col("dfm_100hr").cast("double")) \
                            .withColumn("dfm_10hr", F.col("dfm_10hr").cast("double")) \
                            .withColumn("flame_length_ft_8hr", F.col("flame_length_ft_8hr").cast("double")) \
                            .withColumn("ustar_frc_vel", F.col("ustar_frc_vel").cast("double")) \
                            .withColumn("ws_mph_50m", F.col("ws_mph_50m").cast("double")) \
                            .withColumn("tke_pbl_300m", F.col("tke_pbl_300m").cast("double")) \
                            .withColumn("temp_f_50m", F.col("temp_f_50m").cast("double")) \
                            .withColumn("temp_f_300m", F.col("temp_f_300m").cast("double")) \
                            .withColumn("vpd_mb_50m", F.col("vpd_mb_50m").cast("double")) \
                            .withColumn("vpd_mb_300m", F.col("vpd_mb_300m").cast("double")) \
                            .withColumn("smois_0", F.col("smois_0").cast("double")) \
                            .withColumn("solar_rad", F.col("solar_rad").cast("double")) \
                            .withColumn("dfm_1hr", F.col("dfm_1hr").cast("double")) \
                            .withColumn("lfm_chamise_old", F.col("lfm_chamise_old").cast("double")) \
                            .withColumn("avg_fuel_complexity", F.col("avg_fuel_complexity").cast("double")) \
                            .withColumn("fuel_bed_depth_ft", F.col("fuel_bed_depth_ft").cast("double")) \
                            .withColumn("slope_degree_mean", F.col("slope_degree_mean").cast("double")) \
                            .withColumn("terrain_rugged_mean", F.col("terrain_rugged_mean").cast("double")) \
                            .withColumn("lfm_chamise_new", F.col("lfm_chamise_new").cast("double")) \
                            .withColumn("lfm_herb", F.col("lfm_herb").cast("double")) \
                            .withColumn("oa_pf_wg", F.col("oa_pf_wg").cast("double")) \
                            .withColumn("oa_pfprime_wg", F.col("oa_pfprime_wg").cast("double")) \
                            .withColumn("prob_cat", F.col("prob_cat").cast("double")) \
                            .withColumn("prob_large_critical_or_cat", F.col("prob_large_critical_or_cat").cast("double")) \
                            .withColumn("prob_small", F.col("prob_small").cast("double")) \
                            .withColumn("prob_critical", F.col("prob_critical").cast("double")) \
                            .withColumn("prob_critical_or_cat", F.col("prob_critical_or_cat").cast("double")) \
                            .withColumn("rate_of_spread_chhr_8hr", F.col("rate_of_spread_chhr_8hr").cast("double")) \
                            .withColumn("rh_2m", F.col("rh_2m").cast("double")) \
                            .withColumn("temp_f_2m", F.col("temp_f_2m").cast("double")) \
                            .withColumn("vpd_mb_2m", F.col("vpd_mb_2m").cast("double")) \
                            .withColumn("wg_cf_mph", F.col("wg_cf_mph").cast("double")) \
                            .withColumn("ws_mph", F.col("ws_mph").cast("double")) \
                            .withColumn("ws_mph_300m", F.col("ws_mph_300m").cast("double")) \
                            .withColumn("cfpt_asset", F.col("cfpt_asset").cast("double")) \
                            .withColumn("hni_hnu_risk", F.col("hni_hnu_risk").cast("double")) \
                            .withColumn("openab_etl", F.col("openab_etl").cast("double")) \
                            .withColumn("openab_priority", F.col("openab_priority").cast("double")) \
                            .withColumn("openab_notification", F.col("openab_notification").cast("int")) \
                            .withColumn("openab_equipment_number", F.col("openab_equipment_number").cast("int")) \
                            .withColumn("openab_gis_latitude", F.col("openab_gis_latitude").cast("double")) \
                            .withColumn("openab_gis_longitude", F.col("openab_gis_longitude").cast("double")) \
                            .withColumn("product_probability_not_to_fail", F.col("product_probability_not_to_fail").cast("double")) \
                            .withColumn("probablity_any_tree_hit_on_structure", F.col("probablity_any_tree_hit_on_structure").cast("double")) \
                            .withColumn("cfpt_induction_grounded", F.col("cfpt_induction_grounded").cast("double")) \
                            .withColumn("cfpt_induction_ungrounded", F.col("cfpt_induction_ungrounded").cast("double"))

        
        # Select columns in a stable order; if a column is missing, fill with NULL to avoid failures.
        final_df = final_df.select(
            *base_columns_met,
            # openab_tags
            "openab_etl",
            "openab_priority",
            "openab_notification",
            "openab_equipment_number",
            "openab_structure_number",
            "openab_status",
            "openab_scheduled_start",
            "openab_scheduled_end",
            "openab_long_text",
            "openab_gis_latitude",
            "openab_gis_longitude",
            "count_of_notification",
            # pole_segments
            "pole_segments_sap_func_loc_no",
            "pole_segments_etgis_id",
            "pole_segments_structure_no",
            "pole_segments_sap_equip_id",
            "pole_segments_segments",
            "pole_segments_hfra",
            "pole_segments_hftd",
            "hftd",
            "bounds_first_seg",
            "bounds_1","bounds_2","bounds_3","bounds_4","bounds_5","bounds_6",
            "pole_segment_st_no",
            # hni_hnu
            "hni_hnu_line_name",
            "hni_hnu_itreelocid",
            "hni_hnu_itreerecsid",
            "hni_hnu_priority",
            "hni_hnu_notification",
            "hni_hnu_status",
            "hni_hnu_longtext",
            "sap_functional_location",
            "tree_rec_lat",
            "tree_rec_lon",
            # veg_risk_score_updated
            "nn_sap_fl",
            "nn_etgis_id",
            "probablity_any_tree_hit_on_structure",
            "product_probability_not_to_fail",
            
            # derived columns
            "section",
            "unexploded_section",
            "risk_score_sum",
            "tree_id_count",
            "cfb",
            "cfpt_asset",
            "cfpt_induction_measure",
            "cfpt_induction_ungrounded_measure",
            "fire_potential",
            "near_fpc",
            "hni_hnu_risk",
            "open_a_tags",
            "total_time",
            "model_source",
            "Line_Type",
            "Line_Removal_Status",
            "Potential_Impacted_Subs",
            "Idle_Line_Status",
            "County",
            "Energization_Status",
            "Foreign_Owner",
            "In_2020_HFTD",
            "What_is_the_section",
            "Work_Center",
            "Customers_at_Risk",
            "Customer_Type",
            "Avg_Potential_Impacted_Customer_Count",
            "Max_Total_Count"
            # "cfpt_induction_grounded",
            # "cfpt_induction_ungrounded",
        )
        print("after base seleect ")
        # from collections import Counter

        # col_counts = Counter(final_df.columns)

        # for col_name, count in col_counts.items():
        #     if count > 1:
        #         occurrences = [c for c in final_df.columns if c == col_name]
        #         for dup in occurrences[1:]:
        #             final_df = final_df.drop(dup)

        final_df.printSchema()
        print("After casting columns and select specific Columns")
        # print(final_df.show(5))
        # Hardcoding null values for section to sap_func_loc_no values 
        final_df = final_df.repartition("model_source","dt_local","section")
        final_df = final_df.withColumn("section", F.when(F.col('section').contains('ETL.XXXX'), F.col('sap_func_loc_no')).otherwise(F.col('section')))\
                           .withColumn("unexploded_section", F.when(F.col('unexploded_section').contains('ETL.XXXX'), F.col('sap_func_loc_no')).otherwise(F.col('unexploded_section')))
       
        # calculate new veg risk score
        final_df = final_df.withColumn("probablity_any_tree_hit_on_structure", F.when(F.col('probablity_any_tree_hit_on_structure').isNotNull(), F.col('probablity_any_tree_hit_on_structure'))\
                           .otherwise(F.lit(0).cast("Double")))\
                           .withColumn("product_probability_not_to_fail", F.when(F.col('product_probability_not_to_fail').isNotNull(), F.col('product_probability_not_to_fail'))\
                           .otherwise(F.lit(1).cast("Double")))
        final_df = final_df.withColumn("structure_risk_score_fall", F.col('prob_cat') * F.col('probablity_any_tree_hit_on_structure'))\
                           .withColumn("structure_risk_score_not_fall", 1 - F.col('structure_risk_score_fall'))
        windowSpec_section = Window.partitionBy("model_source","dt_local","section")
        final_df = final_df.withColumn("probablity_that_no_tree_strike_any_structure_on_the_section",F.product(F.col("product_probability_not_to_fail")).over(windowSpec_section)) \
                           .withColumn("section_risk_score_based_on_tree_not_failure",F.sum(F.col("structure_risk_score_not_fall")).over(windowSpec_section))

        psps_mfpc_conditions = ["minimum_fire_potential"]
        final_df = final_df.withColumn("probablity_that_any_tree_strike_any_structure_on_the_section", 1 - F.col('probablity_that_no_tree_strike_any_structure_on_the_section'))\
                           .withColumn("section_risk_score_based_on_tree_failure",F.round(F.sum(F.when((F.col('psps_mfpc').isin(psps_mfpc_conditions)),F.col('structure_risk_score_fall')*1000).otherwise(F.lit(0).cast("Double"))).over(windowSpec_section),18))    
        # mapping cfpt_veg_flag with new risk core model

        final_df = final_df.withColumn("cfpt_veg_flag", F.when((F.col('section_risk_score_based_on_tree_failure') > 5.5), 1).otherwise(F.lit(0).cast("Double"))) ##changed calculations from 4.5to 5.5 PSP-224
        # calculate max CFPt_Veg_Risk_Base_Calc ,CFPt_Veg,avg acount

# calculate CFPt_Veg_Flag
        # final_df.unpersist()
        input_dataset = final_df.select(F.col("model_run_id").alias("model_run_id1"),
                                    F.col("source").alias("source1"),
                                    F.col("sap_func_loc_no").alias("sap_func"),
                                    F.col("section").alias("segments1"),
                                    F.col("unexploded_section").alias("unexploded_section1"),
                                    F.col("dt_local").alias("dt_local1"),
                                    "etgis_id",
                                    F.col("psps_guidance_tx").alias("psps_guidance_tx1"),
                                    "prob_cat",
                                    F.coalesce("risk_score_sum", F.lit(0)).alias("veg_risk_score")
                                    )
        input_dataset.cache()
        print("input_dataset")
        # print(input_dataset.show(5))
        final_dataset = input_dataset.groupBy("model_run_id1", "source1","sap_func","segments1","unexploded_section1","dt_local1","etgis_id","psps_guidance_tx1")\
                                     .agg(F.max(F.col('prob_cat') * F.col("veg_risk_score")).alias("cfpt_veg_risk_base_calc"))
        input_dataset.unpersist()
        final_dataset = F.broadcast(final_dataset)
        final_dataset = final_dataset.select("model_run_id1", "source1","sap_func","segments1","unexploded_section1","dt_local1","etgis_id","psps_guidance_tx1","cfpt_veg_risk_base_calc") 
        final_dataset = final_dataset.groupBy("model_run_id1", "source1", "sap_func", "segments1", "unexploded_section1", "dt_local1", "psps_guidance_tx1").agg(F.sum("cfpt_veg_risk_base_calc").alias("cfpt_veg_risk_base"))
        # calculate cfptvegriskbase psps_guidance doe not contains near

        final_dataset = final_dataset.withColumn('cfpt_veg', F.when((F.col('psps_guidance_tx1').contains('near_')), F.lit(0).cast("Double")).otherwise(F.col('cfpt_veg_risk_base')))
        final_dataset.cache()
        print("Final dataset dataframe")
        # print(final_dataset.show(5))
        print("final_df_count", final_df.count(), "final_dataset dataframe count", final_dataset.count())
        
        print("final dataset")
        final_dataset.select("unexploded_section1").show(5, truncate= False)
        print("final_df")
        final_df.select("model_source").show(5)
        DivCircuit_join_condition = [
            final_df["sap_func_loc_no"] == final_dataset["sap_func"],
            final_df["model_run_id"] == final_dataset["model_run_id1"],
            final_df["source"] == final_dataset["source1"],
            final_df["section"] == final_dataset["segments1"],
            final_df["unexploded_section"] == final_dataset["unexploded_section1"],
            final_df["dt_local"] == final_dataset["dt_local1"],
            final_df["psps_guidance_tx"] == final_dataset["psps_guidance_tx1"]
        ]
     
        print ("final and final_dataset_next ")

        print(final_df.printSchema())
        print(final_dataset.printSchema())
        # DivCircuit_join_condition.cache()
        # enable this later
        final_df = final_df.join(final_dataset, DivCircuit_join_condition, "inner") \
                   .drop("sap_func", "model_run_id1", "source1", "segments1",
                         "dt_local1", "psps_guidance_tx1", "unexploded_section1")
      
        print("After Divcircuit_join Condition")
        # print(final_df.show(5))
        # DivCircuit_join_condition.unpersist()
        final_dataset.unpersist()
        grouped_section_dataset = final_df.\
        groupBy(F.col("sap_func_loc_no").alias("sap_fun_loc_nos"),
                F.col("ratedkv").alias("ratedkvs"),
                F.col("tline_nm").alias("tline_name"),
                F.col("psps_guidance_tx").alias("psps_guidance_txs"),
                F.col("model_source").alias("model_sources"),
                F.col("time_place_id").alias("time_place_ids"),
                F.col("scope_version").alias("scope_versions"),
                F.col("section").alias("sections"),
                F.col("unexploded_section").alias("unexploded_sections")).\
                agg(F.countDistinct("structure_no").cast("double").alias("pole_counts"))
        grouped_section_dataset.cache()
        grouped_section_dataset = F.broadcast(grouped_section_dataset)
        print("=== final_df join columns ===")
        final_df.select(
            "sap_func_loc_no",
            "ratedkv",
            "tline_nm",
            "psps_guidance_tx",
            "model_source",
            "section",
            "unexploded_section"
        ).printSchema()
        grouped_section_dataset.show(5)
        print("=== grouped_section_dataset join columns ===")
        grouped_section_dataset.select(
            "sap_fun_loc_nos",
            "ratedkvs",
            "tline_name",
            "psps_guidance_txs",
            "model_sources",
            "sections",
            "unexploded_sections"
        ).printSchema()


        filtered_cond = ((final_df['sap_func_loc_no'] == grouped_section_dataset['sap_fun_loc_nos']) &
                     (final_df['ratedkv'] == grouped_section_dataset['ratedkvs']) &
                     (final_df['tline_nm'] == grouped_section_dataset['tline_name']) &
                     (final_df['psps_guidance_tx'] == grouped_section_dataset['psps_guidance_txs']) &
                     (final_df['model_source'] == grouped_section_dataset['model_sources']) &
                     (final_df['section'] == grouped_section_dataset['sections']) &
                     (final_df['unexploded_section'] == grouped_section_dataset['unexploded_sections']))
        final_df = final_df.\
        join(grouped_section_dataset, filtered_cond, "inner").\
        drop("sap_fun_loc_nos", "ratedkvs", "tline_name", "psps_guidance_txs", "model_sources", "time_place_ids",
             "scope_versions", "sections", "unexploded_sections")
        
        print ("final after group by ")
        final_df.limit(5)
        cols_to_sum = ['cfb',
                    'cfpt_asset',
                    'cfpt_induction_measure',
                    'cfpt_induction_ungrounded_measure', #Added new column cfpt_induction_ungrounded_measure, Jira PSP-188
                    'fire_potential',
                    'near_fpc', #renamed near_guidance to near_fpc
                    'hni_hnu_risk',
                    'open_a_tags',
                    'cfpt_veg_flag']
        # N = final_df.count()

    
        final_df = final_df.\
            withColumn("scoping_points", F.expr('+'.join(cols_to_sum)).cast("double")).\
            withColumn("opena_total_count", F.when((F.col('openab_structure_number') == (F.col('structure_no'))), F.col('count_of_notification')).otherwise(F.lit("0"))).\
            withColumn("update_timestamp", F.from_utc_timestamp(F.current_timestamp(), "PST").cast("Timestamp")).\
            withColumn("coordinates", F.concat(F.col("latitude"),F.lit(","), F.col("longitude")).cast("string")).\
            withColumn("primarykey_id", F.concat_ws('_', F.col("sap_func_loc_no"), F.col("section"), F.col("etgis_id"), 
                                                    F.col("structure_no"),F.regexp_replace(F.col("model_source"), ' ', '_'), F.unix_timestamp(F.col("dt_local")),
                                                    F.monotonically_increasing_id()))
        print("Final Data to return")
            ##PSP-232/PSP-225, Adding max_cfpt_veg_by_section  aggregation
        # final_df = final_df.repartition("section")
        windowSpec_vegBySec = Window.partitionBy("section")
        final_df = final_df.withColumn("max_by_section_risk_score_based_on_tree_failure", \
                                    F.max(F.col("section_risk_score_based_on_tree_failure")).over(windowSpec_vegBySec))
        
        print("Final Dataframe after adding max_by_section_risk_score_based_on_tree_failure")
        final_df.limit(5)
        return final_df                 

# -----------------------------
# Redshift COPY for ONE table
# -----------------------------
# def copy_one_table(table, s3_path):
#     try:
#         client = boto3.client("redshift-data", region_name=AWS_REGION)
#         # ------------------------- # 1. TRUNCATE TABLE # -------------------------
#         truncate_sql = f"TRUNCATE TABLE {table};"
#         print(f"\nTruncating {table}")
#         response = client.execute_statement(
#             WorkgroupName=REDSHIFT_WORKGROUP,
#             Database=REDSHIFT_DB,
#             Sql=truncate_sql
#         )
#         statement_id = response["Id"]
#         print(f"{table} TRUNCATE submitted. Statement ID: {statement_id}")
#         while True:
#             status = client.describe_statement(Id=statement_id)
#             state = status["Status"]
#             if state in ["FINISHED", "FAILED", "ABORTED"]:
#                 print(f"{table} TRUNCATE final status: {state}")
#                 if state == "FAILED":
#                     print("Error:", status.get("Error"))
#                     raise Exception(f"Redshift TRUNCATE command failed for {table}")
#                 if state == "FINISHED":
#                     print(f"{table} TRUNCATE completed successfully.")
#                 break
#             time.sleep(2)
#         sql = f"""
#         COPY {table}
#         FROM '{s3_path}'
#         IAM_ROLE '{REDSHIFT_IAM_ROLE}'
#         FORMAT AS PARQUET;
#         """

#         print(f"\nSubmitting COPY for {table}")
#         print(sql)

#         response = client.execute_statement(
#             WorkgroupName=REDSHIFT_WORKGROUP,
#             Database=REDSHIFT_DB,
#             Sql=sql
#         )

#         statement_id = response["Id"]
#         print(f"{table} COPY submitted. Statement ID: {statement_id}")

#         while True:
#             status = client.describe_statement(Id=statement_id)
#             state = status["Status"]
#             if state in ["FINISHED", "FAILED", "ABORTED"]:
#                 print(f"{table} COPY final status: {state}")
#                 if state == "FAILED":
#                     print("Error:", status.get("Error"))
#                     raise Exception(f"Redshift COPY command failed for {table}")
#                 if state == "FINISHED":
#                     print(f"{table} COPY completed successfully.")
#                 break
#             time.sleep(2)
            
#             # update_status("unified", "SUCCESS")
#     except Exception as e:
#         print(f"An error occurred during Redshift COPY for {table}: {e}")
#         update_status("unified", "FAILED")
#         raise

# -----------------------------
# Redshift COPY for ONE table
# -----------------------------
def copy_one_table(table, s3_path):
    try:
        client = boto3.client("redshift-data", region_name=AWS_REGION)
        schema , table = table.split(".")
        stage_table = f"{schema}.{table}_stg"

        # ------------------------- #
        # 1. COPY INTO STAGE TABLE  #
        # ------------------------- #
        copy_sql = f"""
        COPY {stage_table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET;
        """

        print(f"\nSubmitting COPY for {stage_table}")
        print(copy_sql)

        response = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DB,
            Sql=copy_sql
        )

        statement_id = response["Id"]
        print(f"{stage_table} COPY submitted. Statement ID: {statement_id}")

        while True:
            status = client.describe_statement(Id=statement_id)
            state = status["Status"]
            if state in ["FINISHED", "FAILED", "ABORTED"]:
                print(f"{stage_table} COPY final status: {state}")
                if state == "FAILED":
                    print("Error:", status.get("Error"))
                    raise Exception(f"Redshift COPY command failed for {stage_table}")
                break
            time.sleep(2)

        # ------------------------- #
        # 2. CALL STORED PROCEDURE  #
        # ------------------------- #
        sp_sql = f"CALL {schema}.sp_promote_stage_to_target('{schema}', '{table}');"
        print(f"\nCalling SP for {table}: {sp_sql}")

        response = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DB,
            Sql=sp_sql
        )

        statement_id = response["Id"]
        print(f"SP call submitted. Statement ID: {statement_id}")

        while True:
            status = client.describe_statement(Id=statement_id)
            state = status["Status"]
            if state in ["FINISHED", "FAILED", "ABORTED"]:
                print(f"SP final status: {state}")
                if state == "FAILED":
                    print("Error:", status.get("Error"))
                    raise Exception(f"Stored procedure failed for {table}")
                break
            time.sleep(2)

        print(f"{table} load completed successfully.")

    except Exception as e:
        print(f"An error occurred during Redshift load for {table}: {e}")
        update_status("unified", "FAILED")
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


def main():
    try:
        start_time = time.time()

        # spark = (
        #     SparkSession.builder
        #     .appName("psps_tx_unified_data")
        #     .config("spark.sql.adaptive.enabled", "true")
        #     .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        #     .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
        #     .config("spark.sql.shuffle.partitions", "200")
        #     .config("spark.sql.adaptive.broadcastJoinThreshold","200MB")
        #     .config("spark.sql.autoBroadcastJoinThreshold", "200MB")
        #     .getOrCreate()
        # )

        # 1. tx_2km_cfp_allruns
        df_tx_2km_cfp_allruns = spark.read.parquet("s3://psps-datastore-dev/curated/fact_psps_tx_scoping/")
        print ("df_tx_2km_cfp_allruns - Schema")
        # print(df_tx_2km_cfp_allruns.printSchema())
        df_tx_2km_cfp_allruns =df_tx_2km_cfp_allruns.drop("processed_at")
        
        # 2. hni_hnu
        df_hni_hnu = spark.read.parquet("s3://psps-datastore-dev/working/fact_hni_hnu/")
        print("HNI_HNU - schema")
        # print(df_hni_hnu.printSchema())
        df_hni_hnu = df_hni_hnu.drop("processed_at")
        
        # 3. veg_risk_score
        df_veg_risk_score = spark.read.parquet("s3://psps-datastore-dev/working/veg_risk_cleaned/")
        print("veg_rick_score - schema")
        # print(df_veg_risk_score.printSchema())
        df_veg_risk_score = df_veg_risk_score.drop("processed_at")
        
        # 4. openab_tag
        df_openab_tags = spark.read.parquet("s3://psps-datastore-dev/working/opentags_processed/*.parquet")
        print("opentag - schema")
        # print(df_openab_tags.printSchema())
        df_openab_tags = df_openab_tags.drop("processed_at")

        # 5. Structure Report
        df_pole_segments = spark.read.parquet("s3://psps-datastore-dev/working/structure_cleaned/")
        print("structure report - Schema ")
        # print(df_pole_segments.printSchema())
        df_pole_segments = df_pole_segments.drop("processed_at")                
        # 6 . Veg rick_score automated
        df_veg_risk_score_updated = spark.read.parquet("s3://psps-datastore-dev/working/veg_risk_score_automated_file/")
        print("Veg_rick_score_updated - Schema")
        # print(df_veg_risk_score_updated.printSchema())
        df_veg_risk_score_updated = df_veg_risk_score_updated.drop("processed_at")

        # OA model data
        oa_data = spark.read.parquet("s3://psps-datastore-dev/working/oa/")
        df_oa_data_select = oa_data.select("sap_equip_id", "structure_oa")
        print("oa value")
        # df_oa_data_select.filter(F.col("sap_equip_id")== 40575929).show()


        df_oa_data_select = broadcast(df_oa_data_select)
        src = ReadAllSource()
        src.tx_2km_cfp_allruns = df_tx_2km_cfp_allruns
        src.hni_hnu = df_hni_hnu
        src.veg_risk_score = df_veg_risk_score
        src.openab_tags = df_openab_tags
        src.pole_segments = df_pole_segments
        src.veg_risk_score_updated= df_veg_risk_score_updated
        # src.oa_data_select = df_oa_data_select
        final_unified_df = src.compute()
        final_unified_df = final_unified_df.withColumn("processed_at", F.to_utc_timestamp(F.current_timestamp(), F.lit("PST")))
        final_data_oa = final_unified_df.alias("f").join(
                    df_oa_data_select.alias("oa"),
                    on="sap_equip_id",
                    how="left"
                ).select("f.*", F.col("oa.structure_oa").cast("numeric(30,25)"))

        final_unified_df = final_data_oa.withColumnsRenamed(column_mapping)
        print("Final unified Schema ++++++++++++++++++")
        print(final_unified_df.printSchema())
        # final_unified_df.filter(F.col("sap_equip_id")==40575929).select("structure_oa","sap_equip_id").show()
          # 1. Get Redshift column order
        rs_cols = get_redshift_columns("psps.tx_scoping_unified_source")

        # 2. Add missing columns to Redshift
        add_missing_columns_to_redshift("psps.tx_scoping_unified_source", final_unified_df, rs_cols)

        # 3. Refresh Redshift column order (after ALTER)
        rs_cols = get_redshift_columns("psps.tx_scoping_unified_source")

        # 4. Reorder fact_df columns to match Redshift
        final_unified_df = reorder_fact_df_columns(final_unified_df, rs_cols)

        # print(final_unified_df.explain(extended = True))
        final_unified_df.write.mode("overwrite").parquet("s3://psps-datastore-dev/working/unified_data/")     
        # print(f"\nSpark job completed in {round(time.time() - start_time, 2)/60} minutes.")

    except Exception as e:
        print("FAILED: Error during unified Fact Builder job")
        print("Error:", e)
        update_status("unified", "FAILED")
        raise
if __name__ == "__main__":
    try:
        job_timing = time.time()

        # Wait for all sources to finish
        ready = wait_for_sources()
        if not ready:
            update_status("unified", "FAILED")
            raise SystemExit("Unified aborted due to source failure.")

        # Mark unified as running

        update_status("unified", "RUNNING")
        print("All sources are ready. Starting unified Fact Builder job.")
        
        main()
        copy_all_tables_parallel()
        update_status("unified", "SUCCESS")

    except Exception as e:
        update_status("unified", "FAILED")
        raise

    job_end = time.time()
    print(f"\nJob completed in {round((job_end - job_timing)/60, 2)} minutes.")

