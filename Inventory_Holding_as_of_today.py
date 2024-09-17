# Databricks notebook source
# DBTITLE 1,Connection To ADLS
# MAGIC %run "/Workspace/Shared/NUSA/SupplyChain/Integration/AzureConnections"

# COMMAND ----------

# DBTITLE 1,Reading table from source ADLS Location
material_master_df = spark.read.format("delta").load(
    dev_prod+"solutions/datamarts/md/pro/inbound/materialmaster_delta"
)

mat_hierarachy = (
    spark.read.format("parquet")
    .option("header", "true")
    .load(
        dev_prod+"solutions/datamarts/sc/md/inbound/materialhierarchy_parquet/Year=9999/Month=12/Day=31/*"
    )
)


batch_inventory_actual_df = spark.read.format("parquet").load(
    dev_prod+"solutions/datamarts/sc/wh/inbound/batch_inventory_actual_parquet/"
)

apo_snp_35d_df = (
    spark.read.format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        dev_prod+"solutions/datamarts/sc/sp/inbound/apo_snp_35d_parquet/archive/*/*/*/apo_snp_35d"
    )
)

otr_var = (
    spark.read.format("delta")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        dev_prod+"solutions/abovemarket/sc/cs/inbound/OrderTrackingReport"
    )
)

# COMMAND ----------

# DBTITLE 1,Material Master Selected Columns Query
selected_material_master_df = material_master_df.select(
    sf.col("MaterialID"),
    sf.col("ProductPlanningUnitsPerPallet"),
    sf.col("ProductPlanningUnitsPerCase"),
    sf.col("ProductListPrice"),
    sf.col("ProductStandardCost"),
    sf.col("ProductID"),
    sf.col("ProductDescription")
)

# COMMAND ----------

# DBTITLE 1,Assume today's date
today = datetime.today()

yesterday = today - timedelta(days=1)
last_week = today - timedelta(days=7)

today_str = today.strftime('%Y-%m-%d')
yesterday_str = yesterday.strftime('%Y-%m-%d')
last_week_str = last_week.strftime('%Y-%m-%d')

doh_y_d = yesterday.strftime('%d')

# COMMAND ----------

# DBTITLE 1,Day's On hand Yesterday Calculation
day_on_hand_y = batch_inventory_actual_df.filter(sf.col("AvailableDate") == yesterday_str).select(
    sf.col("AvailableDate"),
    sf.col("MaterialID"),
    sf.col("LocationID"),
    sf.col("Day").alias("DOH_Yesterday"),
)

# COMMAND ----------

# DBTITLE 1,Calculate Daily Inventory
daily_apo_filter = apo_snp_35d_df.filter(sf.col("DATE")==today_str).select(
    sf.col("DATE"),
    sf.col("LocationID"),
    sf.col("MaterialID"),
    sf.col("UnitofMeasure"),
    sf.col("SnapshotDate"),
)

daily_batch_filter = batch_inventory_actual_df.filter(sf.col("AvailableDate") == today_str).select(
    sf.col("LocationID"),
    sf.col("MaterialID"),
    sf.col("Day"),
    sf.col("InventoryStockUnRestricted"),
    sf.col("InventoryStockRestricted"),
    sf.col("InventoryStockBlocked"),
    sf.col("PalletStockUnRestricted"),
    sf.col("PalletStockRestricted"),
    sf.col("PalletStockBlocked"),
    sf.col("SnapshotDate"),
)

daily_joined_df = daily_apo_filter.alias("da").join(
    daily_batch_filter.alias("db"),
    (sf.col("da.MaterialID") == sf.col("db.MaterialID")) & (sf.col("da.LocationID") == sf.col("db.LocationID")),
    "left"
).select(
    sf.col("da.DATE"),
    sf.col("db.Day"),
    sf.col("da.MaterialID"),
    sf.col("da.LocationID"),
    sf.col("db.SnapshotDate"),
    sf.col("db.InventoryStockUnRestricted"),
    sf.col("db.InventoryStockRestricted"),
    sf.col("db.InventoryStockBlocked"),
    sf.col("db.PalletStockUnRestricted"),
    sf.col("db.PalletStockRestricted"),
    sf.col("db.PalletStockBlocked")
)

dily_joined_df_2 = daily_joined_df.alias("da").join(
    selected_material_master_df.alias("sm"),
    sf.col("da.MaterialID") == sf.col("sm.MaterialID"),
    "left"
).select(
    sf.col("da.DATE"),
    sf.col("da.Day"),
    sf.col("da.MaterialID"),
    sf.col("da.LocationID"),
    sf.col("da.SnapshotDate"),
    sf.col("da.InventoryStockUnRestricted"),
    sf.col("da.InventoryStockRestricted"),
    sf.col("da.InventoryStockBlocked"),
    sf.col("da.PalletStockUnRestricted"),
    sf.col("da.PalletStockRestricted"),
    sf.col("da.PalletStockBlocked"),
    sf.col("sm.ProductPlanningUnitsPerPallet"),
    sf.col("sm.ProductPlanningUnitsPerCase"),
    sf.col("sm.ProductListPrice"),
    sf.col("sm.ProductStandardCost"),
    sf.col("sm.ProductID"),
    sf.col("sm.ProductDescription")
)

dily_joined_df_2 = dily_joined_df_2.dropDuplicates()

# COMMAND ----------

daily_output_df = (
    dily_joined_df_2.withColumn(
        "InventoryClear_pallets",
        sf.coalesce(sf.col("PalletStockUnRestricted"), sf.lit(0)),
    )
    .withColumn(
        "InventoryRestricted_pallets",
        sf.coalesce(sf.col("PalletStockRestricted"), sf.lit(0)),
    )
    .withColumn(
        "InventoryBlocked_pallets", sf.coalesce(sf.col("PalletStockBlocked"), sf.lit(0))
    )
    .withColumn(
        # Inventory in Cases
        "InventoryClear_cases",
        (
            sf.coalesce(sf.col("PalletStockUnRestricted"), sf.lit(0))
            * sf.coalesce(sf.col("ProductPlanningUnitsPerPallet"), sf.lit(0))
        )
        / sf.coalesce(sf.col("ProductPlanningUnitsPerCase"), sf.lit(1)),
    )
    .withColumn(
        "InventoryBlocked_cases",
        (
            sf.coalesce(sf.col("PalletStockBlocked"), sf.lit(0))
            * sf.coalesce(sf.col("ProductPlanningUnitsPerPallet"), sf.lit(0))
        )
        / sf.coalesce(sf.col("ProductPlanningUnitsPerCase"), sf.lit(1)),
    )
    .withColumn(
        "InventoryRestricted_cases",
        (
            sf.coalesce(sf.col("PalletStockRestricted"), sf.lit(0))
            * sf.coalesce(sf.col("ProductPlanningUnitsPerPallet"), sf.lit(0))
        )
        / sf.coalesce(sf.col("ProductPlanningUnitsPerCase"), sf.lit(1)),
    )
    .withColumn(
        # Inventory in Dollar
        "InventoryClear_dollar",
        (
            (
                sf.coalesce(sf.col("PalletStockUnRestricted"), sf.lit(0))
                * sf.coalesce(sf.col("ProductPlanningUnitsPerPallet"), sf.lit(0))
            )
            / sf.coalesce(sf.col("ProductPlanningUnitsPerCase"), sf.lit(1))
        )
        * sf.coalesce(
            sf.col("ProductListPrice"), sf.col("ProductStandardCost"), sf.lit(0)
        ),
    )
    .withColumn(
        "InventoryBlocked_dollar",
        (
            (
                sf.coalesce(sf.col("PalletStockBlocked"), sf.lit(0))
                * sf.coalesce(sf.col("ProductPlanningUnitsPerPallet"), sf.lit(0))
            )
            / sf.coalesce(sf.col("ProductPlanningUnitsPerCase"), sf.lit(1))
        )
        * sf.coalesce(
            sf.col("ProductListPrice"), sf.col("ProductStandardCost"), sf.lit(0)
        ),
    )
    .withColumn(
        "InventoryRestricted_dollar",
        (
            (
                sf.coalesce(sf.col("PalletStockRestricted"), sf.lit(0))
                * sf.coalesce(sf.col("ProductPlanningUnitsPerPallet"), sf.lit(0))
            )
            / sf.coalesce(sf.col("ProductPlanningUnitsPerCase"), sf.lit(1))
        )
        * sf.coalesce(
            sf.col("ProductListPrice"), sf.col("ProductStandardCost"), sf.lit(0)
        ),
    )
    .select(
        # Select the required columns
        sf.col("DATE").alias("Date"),
        sf.coalesce(sf.col("Day"), sf.lit(0)).alias("DOH_Today"),
        sf.col("LocationID").alias("Location"),
        sf.col("MaterialID").alias("Material ID"),
        sf.col("InventoryClear_pallets"),
        sf.col("InventoryRestricted_pallets"),
        sf.col("InventoryBlocked_pallets"),
        sf.col("InventoryClear_cases"),
        sf.col("InventoryRestricted_cases"),
        sf.col("InventoryBlocked_cases"),
        sf.col("InventoryClear_dollar"),
        sf.col("InventoryRestricted_dollar"),
        sf.col("InventoryBlocked_dollar"),
        sf.coalesce(sf.col("SnapshotDate"), sf.lit(None).cast("timestamp")).alias("SnapshotDate")
    )
)

# COMMAND ----------

daily_output_df.display()

# COMMAND ----------

# DBTITLE 1,Calculate Weekly Inventory
weekly_filter = apo_snp_35d_df.filter(sf.col("DATE")==last_week_str).select(
    sf.col("DATE"),
    sf.col("LocationID"),
    sf.col("MaterialID"),
    sf.col("UnitofMeasure"),
    sf.col("SnapshotDate"),
)

weekly_batch_filter = batch_inventory_actual_df.filter(sf.col("AvailableDate") == last_week_str).select(
    sf.col("LocationID"),
    sf.col("MaterialID"),
    sf.col("Day"),
    sf.col("InventoryStockUnRestricted"),
    sf.col("InventoryStockRestricted"),
    sf.col("InventoryStockBlocked"),
    sf.col("PalletStockUnRestricted"),
    sf.col("PalletStockRestricted"),
    sf.col("PalletStockBlocked"),
    sf.col("SnapshotDate"),
)

weekly_joined_df = weekly_filter.alias("wa").join(
    weekly_batch_filter.alias("wb"),
    (sf.col("wa.MaterialID") == sf.col("wb.MaterialID")) & (sf.col("wa.LocationID") == sf.col("wb.LocationID")),
    "left"
).select(
    sf.col("wa.DATE"),
    sf.col("wb.Day"),
    sf.col("wa.MaterialID"),
    sf.col("wa.LocationID"),
    sf.col("wb.SnapshotDate"),
    sf.col("wb.InventoryStockUnRestricted"),
    sf.col("wb.InventoryStockRestricted"),
    sf.col("wb.InventoryStockBlocked"),
    sf.col("wb.PalletStockUnRestricted"),
    sf.col("wb.PalletStockRestricted"),
    sf.col("wb.PalletStockBlocked")
)

weekly_joined_df_2 = weekly_joined_df.alias("wa").join(
    selected_material_master_df.alias("sm"),
    sf.col("wa.MaterialID") == sf.col("sm.MaterialID"),
    "left"
).select(
    sf.col("wa.DATE"),
    sf.col("wa.Day"),
    sf.col("wa.MaterialID"),
    sf.col("wa.LocationID"),
    sf.col("wa.SnapshotDate"),
    sf.col("wa.InventoryStockUnRestricted"),
    sf.col("wa.InventoryStockRestricted"),
    sf.col("wa.InventoryStockBlocked"),
    sf.col("wa.PalletStockUnRestricted"),
    sf.col("wa.PalletStockRestricted"),
    sf.col("wa.PalletStockBlocked"),
    sf.col("sm.ProductPlanningUnitsPerPallet"),
    sf.col("sm.ProductPlanningUnitsPerCase"),
    sf.col("sm.ProductListPrice"),
    sf.col("sm.ProductStandardCost"),
    sf.col("sm.ProductID"),
    sf.col("sm.ProductDescription")
)

weekly_joined_df_2 = weekly_joined_df_2.dropDuplicates()

# COMMAND ----------

# Define the window specification: Partition by MaterialID and LocationID, ordered by DATE
window_spec = Window.partitionBy("MaterialID", "LocationID").orderBy("DATE")

# Use the lag function to get last week's inventory values for each matrix
weekly_joined_df_2 = (
    weekly_joined_df_2.withColumn(
        "Last_Week_Inventory_Clear",
        sf.coalesce(sf.lag("PalletStockUnRestricted", 1).over(window_spec), sf.lit(0)),
    )
    .withColumn(
        "Last_Week_Inventory_Restricted",
        sf.coalesce(sf.lag("PalletStockRestricted", 1).over(window_spec), sf.lit(0)),
    )
    .withColumn(
        "Last_Week_Inventory_Blocked",
        sf.coalesce(sf.lag("PalletStockBlocked", 1).over(window_spec), sf.lit(0)),
    )
)

# Calculate the weekly percentage for all three matrices
weekly_joined_df_2 = (
    weekly_joined_df_2.withColumn(
        "Last_Week_Inventory_Clear_%",
        sf.when(
            sf.col("Last_Week_Inventory_Clear") != 0,
            (
                (
                    sf.col("PalletStockUnRestricted")
                    - sf.col("Last_Week_Inventory_Clear")
                )
                / sf.col("Last_Week_Inventory_Clear")
            )
            * 100,
        ).otherwise(0),
    )
    .withColumn(
        "Last_Week_Inventory_Restricted_%",
        sf.when(
            sf.col("Last_Week_Inventory_Restricted") != 0,
            (
                (
                    sf.col("PalletStockRestricted")
                    - sf.col("Last_Week_Inventory_Restricted")
                )
                / sf.col("Last_Week_Inventory_Restricted")
            )
            * 100,
        ).otherwise(0),
    )
    .withColumn(
        "Last_Week_Inventory_Blocked_%",
        sf.when(
            sf.col("Last_Week_Inventory_Blocked") != 0,
            (
                (sf.col("PalletStockBlocked") - sf.col("Last_Week_Inventory_Blocked"))
                / sf.col("Last_Week_Inventory_Blocked")
            )
            * 100,
        ).otherwise(0),
    )
)

# Select the required columns for the output
weekly_output_df = weekly_joined_df_2.select(
    sf.col("DATE").alias("Date"),
    sf.col("LocationID").alias("Location"),
    sf.col("MaterialID").alias("Material ID"),
    sf.col("Last_Week_Inventory_Clear"),
    sf.col("Last_Week_Inventory_Restricted"),
    sf.col("Last_Week_Inventory_Blocked"),
    sf.col("Last_Week_Inventory_Clear_%"),
    sf.col("Last_Week_Inventory_Restricted_%"),
    sf.col("Last_Week_Inventory_Blocked_%"),
    sf.coalesce(sf.col("SnapshotDate"), sf.lit(None).cast("timestamp")).alias(
        "SnapshotDate"
    ),
)

# COMMAND ----------

# DBTITLE 1,Final Inventory On Hand Today
output_df = daily_output_df.alias("daily").join(
    weekly_output_df.alias("weekly"),
    (sf.col("daily.Location") == sf.col("weekly.Location")) &
    (sf.col("daily.Material ID") == sf.col("weekly.Material ID")),
    "left",
)

# Select the columns as per the required output format
output_df = output_df.select(
    sf.col("daily.Date").alias("Date"),
    sf.col("daily.Location").alias("Location"),
    sf.col("daily.Material ID").alias("Material ID"),
    # Daily Data - Inventory matrices and units
    sf.col("daily.InventoryClear_pallets").alias("Inventory_Clear"),
    sf.col("daily.InventoryRestricted_pallets").alias("Inventory_Restricted"),
    sf.col("daily.InventoryBlocked_pallets").alias("Inventory_Blocked"),
    # Daily Data - Pallets
    sf.col("daily.InventoryClear_pallets").alias("InventoryClear_pallets"),
    sf.col("daily.InventoryRestricted_pallets").alias("InventoryRestricted_pallets"),
    sf.col("daily.InventoryBlocked_pallets").alias("InventoryBlocked_pallets"),
    # Daily Data - Cases
    sf.col("daily.InventoryClear_cases").alias("InventoryClear_cases"),
    sf.col("daily.InventoryRestricted_cases").alias("InventoryRestricted_cases"),
    sf.col("daily.InventoryBlocked_cases").alias("InventoryBlocked_cases"),
    # Daily Data - Dollar
    sf.col("daily.InventoryClear_dollar").alias("InventoryClear_dollar"),
    sf.col("daily.InventoryRestricted_dollar").alias("InventoryRestricted_dollar"),
    sf.col("daily.InventoryBlocked_dollar").alias("InventoryBlocked_dollar"),
    # Weekly Data - Inventory matrices
    sf.col("weekly.Last_Week_Inventory_Clear"),
    sf.col("weekly.Last_Week_Inventory_Restricted"),
    sf.col("weekly.Last_Week_Inventory_Blocked"),
    # Weekly Data - Percentages
    sf.col("weekly.Last_Week_Inventory_Clear_%"),
    sf.col("weekly.Last_Week_Inventory_Restricted_%"),
    sf.col("weekly.Last_Week_Inventory_Blocked_%"),
    # Snapshot Date
    sf.coalesce(sf.col("daily.SnapshotDate"), sf.col("weekly.SnapshotDate")).alias("SnapshotDate"),
    sf.col("daily.DOH_Today").alias("DOH_Today")  # Keep DOH_Today
)

# Join with day_on_hand_y to include DOH_Yesterday
output_df = output_df.alias("o").join(
    day_on_hand_y.alias("doh_y"),
    (sf.col("o.Material ID") == sf.col("doh_y.MaterialID")) &
    (sf.col("o.Location") == sf.col("doh_y.LocationID")),
    "left"
).select(
    # Select all columns from output_df
    "o.Date",
    "o.Location",
    "o.Material ID",
    "o.Inventory_Clear",
    "o.Inventory_Restricted",
    "o.Inventory_Blocked",
    "o.InventoryClear_pallets",
    "o.InventoryRestricted_pallets",
    "o.InventoryBlocked_pallets",
    "o.InventoryClear_cases",
    "o.InventoryRestricted_cases",
    "o.InventoryBlocked_cases",
    "o.InventoryClear_dollar",
    "o.InventoryRestricted_dollar",
    "o.InventoryBlocked_dollar",
    "o.Last_Week_Inventory_Clear",
    "o.Last_Week_Inventory_Restricted",
    "o.Last_Week_Inventory_Blocked",
    "o.Last_Week_Inventory_Clear_%",
    "o.Last_Week_Inventory_Restricted_%",
    "o.Last_Week_Inventory_Blocked_%",
    "o.SnapshotDate",
    "o.DOH_Today",
    sf.coalesce(sf.col("doh_y.DOH_Yesterday"), sf.lit(0)).alias("DOH_Yesterday")
)
# output_df = output_df.filter(sf.col("SnapshotDate")==today_str)

# COMMAND ----------

# DBTITLE 1,Bias Calculation for 90's days
filtered_apo_df = apo_snp_35d_df.filter(
    F.col("SnapshotDate") >= F.date_sub(F.current_date(), 90)
)

demand_forecast_df = filtered_apo_df.groupBy("DATE", "MaterialID", "LocationID").agg(
    F.sum("ConsensusDemandPlan").alias("DemandForecast")
)

otr_with_last_8_digits_df = otr_var.withColumn(
    "MaterialID", F.expr("substring(MaterialNumber, -8, 8)")
)

actual_demand_df = otr_with_last_8_digits_df.groupBy("MaterialID").agg(
    F.sum("ConfirmedQuantity").alias("ActualDemand")
)

total_demand_joined_df = (
    demand_forecast_df.alias("d")
    .join(
        actual_demand_df.alias("a"),
        F.col("d.MaterialID") == F.col("a.MaterialID"),
        "left",
    )
    .select(
        sf.col("d.DATE"),
        sf.col("d.MaterialID"),
        sf.col("d.LocationID"),
        sf.col("d.DemandForecast"),
        sf.col("a.ActualDemand"),
    )
)

final_df = total_demand_joined_df.select(
    F.col("d.DATE"),
    F.col("d.LocationID"),
    F.col("d.MaterialID"),
    F.col("DemandForecast"),
    F.col("ActualDemand"),
    (
        F.round(F.abs(
            (F.coalesce(F.col("DemandForecast"), F.lit(0)) - F.coalesce(F.col("ActualDemand"), F.lit(0))) /
            F.coalesce(F.col("ActualDemand"), F.lit(1))
        ), 3).alias("Bias")  # Rounding bias to 3 decimal places and making it positive
    )
)
