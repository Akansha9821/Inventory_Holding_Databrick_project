from datetime import datetime, timedelta
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

class AzureDataLoader:
    """
    A class to handle Azure Data Lake connections and data loading using PySpark.
    """

    def __init__(self, spark: SparkSession, key_vault_scope: str, adls_client_secret: str, adls_client_id: str, tenant_id: str):
        """
        Initializes the AzureDataLoader class and sets up the necessary configurations.
        Args:
            spark (SparkSession): The active Spark session.
            key_vault_scope (str): Azure KeyVault scope name for fetching secrets.
            adls_client_secret (str): Secret name for ADLS client secret.
            adls_client_id (str): Secret name for ADLS client ID.
            tenant_id (str): Azure Tenant ID for OAuth2 endpoint.
        """
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        self.adls_client_secret = adls_client_secret
        self.adls_client_id = adls_client_id
        self.tenant_id = tenant_id
        self._set_azure_config()

    def _set_azure_config(self):
        """Sets up the Azure Data Lake authentication using OAuth2."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
        except ImportError:
            raise ImportError("dbutils is not available outside Databricks notebooks.")

        authkey = dbutils.secrets.get(scope=self.key_vault_scope, key=self.adls_client_secret)
        clientid = dbutils.secrets.get(scope=self.key_vault_scope, key=self.adls_client_id)

        self.spark.conf.set("fs.azure.account.auth.type", "OAuth")
        self.spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set("fs.azure.account.oauth2.client.id", clientid)
        self.spark.conf.set("fs.azure.account.oauth2.client.secret", authkey)
        self.spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token")

    def load_delta_data(self, path: str):
        """Loads data from Delta format."""
        return self.spark.read.format("delta").load(path)

    def load_parquet_data(self, path: str, header: bool = True, infer_schema: bool = True):
        """Loads data from Parquet format."""
        return self.spark.read.format("parquet").option("header", header).option("inferSchema", infer_schema).load(path)


class DataProcessor:
    """
    A class to handle data processing and transformations.
    """

    def __init__(self, material_df, hierarchy_df, batch_inventory_df, apo_snp_df, otr_var_df):
        """
        Initializes the DataProcessor class with required DataFrames.
        """
        self.material_df = material_df
        self.hierarchy_df = hierarchy_df
        self.batch_inventory_df = batch_inventory_df
        self.apo_snp_df = apo_snp_df
        self.otr_var_df = otr_var_df

    def join_dataframes(self):
        """Joins the necessary DataFrames based on MaterialID and LocationID."""
        return (
            self.apo_snp_df.alias("a")
            .join(self.material_df.alias("m"), sf.col("a.MaterialID") == sf.col("m.MaterialID"), "left")
            .join(self.batch_inventory_df.alias("b"), (sf.col("a.MaterialID") == sf.col("b.MaterialID")) & (sf.col("a.LocationID") == sf.col("b.LocationID")), "left")
            .select(
                sf.col("a.MaterialID"),
                sf.col("a.LocationID"),
                sf.col("b.PalletStockUnRestricted").alias("clear_in_pallets"),
                sf.col("b.PalletStockBlocked").alias("blocked_in_pallets"),
                sf.col("b.PalletStockRestricted").alias("restricted_in_pallets"),
                sf.col("m.ProductPlanningUnitsPerPallet"),
                sf.col("m.ProductPlanningUnitsPerCase"),
                sf.coalesce(sf.col("m.ProductListPrice"), sf.col("m.ProductStandardCost")).alias("Price"),
                sf.col("b.Snapshotdate"),
            )
        )

    def add_date_columns(self, df):
        """Adds the necessary date columns to the DataFrame."""
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime('%Y-%m-%d')
        yesterday_str = yesterday.strftime('%d')
    
        df = df.withColumn("Date", sf.lit(today_str))
        df = df.withColumn("DOH_Today", sf.when(sf.date_format(sf.col("Snapshotdate"), "yyyy-MM-dd") == today_str, sf.date_format(sf.col("Snapshotdate"), "dd")).otherwise(None))
        df = df.withColumn("DOH_Yesterday", sf.lit(yesterday_str))
        return df
    
    def calculate_inventory_metrics(self, df):
        """Calculates the inventory metrics."""
        df = df.dropDuplicates()
        return df.select(
            "Date",
            "LocationID",
            "MaterialID",
            "clear_in_pallets",
            "blocked_in_pallets",
            "restricted_in_pallets",
            (sf.col("clear_in_pallets") * sf.col("ProductPlanningUnitsPerPallet") / sf.col("ProductPlanningUnitsPerCase")).alias("InventoryClear_cases"),
            (sf.col("blocked_in_pallets") * sf.col("ProductPlanningUnitsPerPallet") / sf.col("ProductPlanningUnitsPerCase")).alias("InventoryBlocked_cases"),
            ((sf.col("blocked_in_pallets") + sf.col("restricted_in_pallets")) * sf.col("ProductPlanningUnitsPerPallet") / sf.col("ProductPlanningUnitsPerCase")).alias("InventoryRestricted_cases"),
            ((sf.col("clear_in_pallets") * sf.col("ProductPlanningUnitsPerPallet") / sf.col("ProductPlanningUnitsPerCase")) * sf.coalesce(sf.col("Price"), sf.lit(0))).alias("InventoryClear_dollar"),
            ((sf.col("blocked_in_pallets") * sf.col("ProductPlanningUnitsPerPallet") / sf.col("ProductPlanningUnitsPerCase")) * sf.coalesce(sf.col("Price"), sf.lit(0))).alias("InventoryBlocked_dollar"),
            (((sf.col("blocked_in_pallets") + sf.col("restricted_in_pallets")) * sf.col("ProductPlanningUnitsPerPallet") / sf.col("ProductPlanningUnitsPerCase")) * sf.coalesce(sf.col("Price"), sf.lit(0))).alias("InventoryRestricted_dollar"),
            "Snapshotdate",
            "DOH_Today",
            "DOH_Yesterday"
        )

    def get_last_week_data(self, last_week_str, joined_df):
        """
        Retrieves the last week's inventory data, checking if the necessary columns are present in apo_snp_df.
        If not, falls back on joined_df where SnapshotDate matches last_week_str.
        """
        required_columns = ["MaterialID", "LocationID", "clear_in_pallets", "blocked_in_pallets", "restricted_in_pallets", "SnapshotDate"]

        last_week_filter_df = self.apo_snp_df.filter(sf.col("SnapshotDate") == last_week_str)    
        last_week_join_df = joined_df.join(last_week_filter_df, (joined_df.MaterialID == last_week_filter_df.MaterialID) & (joined_df.LocationID == last_week_filter_df.LocationID), "left")

        last_week_selected_df = last_week_join_df.select(
            sf.col("MaterialID"),
            sf.col("LocationID"),
            sf.col("clear_in_pallets"),
            sf.col("blocked_in_pallets"),
            sf.col("restricted_in_pallets"),
            sf.col(),
            sf.col(),
            sf.col(),
        )
        return last_week_selected_df


# Usage Example:
def main():
    tenant_id = "12a3af23-a769-4654-847f-958f3d479f4a"
    key_vault_scope = "nusa-kv-ift"
    adls_client_secret = "adls2clientsecret"
    adls_client_id = "adls2clientid"
    
    spark = SparkSession.builder.appName("AzureDataProcessing").getOrCreate()

    # Initialize AzureDataLoader
    azure_loader = AzureDataLoader(spark, key_vault_scope, adls_client_secret, adls_client_id, tenant_id)

    # Load DataFrames
    material_master_df = azure_loader.load_delta_data(
        "abfss://root@nusadhprsadatalakeprod.dfs.core.windows.net/solutions/datamarts/md/pro/inbound/materialmaster_delta"
    )
    mat_hierarchy_df = azure_loader.load_parquet_data(
        "abfss://root@nusadhprsadatalakeprod.dfs.core.windows.net/solutions/datamarts/sc/md/inbound/materialhierarchy_parquet/Year=9999/Month=12/Day=31/*"
    )
    batch_inventory_actual_df = azure_loader.load_parquet_data(
        "abfss://root@nusadhprsadatalakeprod.dfs.core.windows.net/solutions/datamarts/sc/wh/inbound/batch_inventory_actual_parquet/"
    )
    apo_snp_35d_df = azure_loader.load_parquet_data(
        "abfss://root@nusadhprsadatalakeprod.dfs.core.windows.net/solutions/datamarts/sc/sp/inbound/apo_snp_35d_parquet/archive/*/*/*/apo_snp_35d"
    )
    otr_var_df = azure_loader.load_delta_data(
        "abfss://root@nusadhprsadatalakeprod.dfs.core.windows.net/solutions/abovemarket/sc/cs/inbound/OrderTrackingReport"
    )

    # Process data
    data_processor = DataProcessor(material_master_df, mat_hierarchy_df, batch_inventory_actual_df, apo_snp_35d_df, otr_var_df)
    joined_df = data_processor.join_dataframes()
    dated_df = data_processor.add_date_columns(joined_df)

    inventory_metrics_df = data_processor.calculate_inventory_metrics(dated_df)

    # Calculate last week's date string
    today = datetime.today()
    last_week = today - timedelta(days=7)
    last_week_str = last_week.strftime('%Y-%m-%d')

    # Get last week's inventory data
    # last_week_df = data_processor.get_last_week_data(last_week_str, inventory_metrics_df)


    # Display the final DataFrame
    inventory_metrics_df.display()

if __name__ == "__main__":
    main()