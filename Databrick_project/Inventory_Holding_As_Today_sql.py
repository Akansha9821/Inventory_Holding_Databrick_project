# Databricks notebook source
# Dc_condition, Network_condition, PL3_condition,Product_condition,Division_condition = 5385, "AMBIENT", "USFF2", 11000324, "BAKING"

query = f"""
WITH latest_inventory AS (
    SELECT
        MaterialID,
        LocationID,
        Snapshotdate,
        MAX(Snapshotdate) OVER (PARTITION BY MaterialID, LocationID ORDER BY Snapshotdate DESC) AS latest_date
    FROM batch_inventory_actual_temp
    GROUP BY MaterialID, LocationID, Snapshotdate
),
aggregated_inventory AS (
    SELECT
        b.MaterialID,
        b.LocationID,
        b.Snapshotdate,
        m.ProductPlanningUnitsPerPallet,
        m.ProductPlanningUnitsPerCase,
        COALESCE(m.ProductListPrice, m.ProductStandardCost) AS Price,
        
        b.InventoryStockUnRestricted AS clear_pallets,
        b.InventoryStockBlocked AS blocked_pallets,
        (b.InventoryStockInQualityInspection + b.InventoryStockRestricted) AS restricted_pallets,
        
        -- Calculate cases based on pallet conversion
        (b.InventoryStockUnRestricted * m.ProductPlanningUnitsPerPallet) / m.ProductPlanningUnitsPerCase AS clear_cases,
        (b.InventoryStockBlocked * m.ProductPlanningUnitsPerPallet) / m.ProductPlanningUnitsPerCase AS blocked_cases,
        ((b.InventoryStockInQualityInspection + b.InventoryStockRestricted) * m.ProductPlanningUnitsPerPallet) / m.ProductPlanningUnitsPerCase AS restricted_cases,
        
        -- Calculate dollars based on cases and price
        ((b.InventoryStockUnRestricted * m.ProductPlanningUnitsPerPallet) / m.ProductPlanningUnitsPerCase) * COALESCE(m.ProductListPrice, m.ProductStandardCost) AS clear_dollars,
        ((b.InventoryStockBlocked * m.ProductPlanningUnitsPerPallet) / m.ProductPlanningUnitsPerCase) * COALESCE(m.ProductListPrice, m.ProductStandardCost) AS blocked_dollars,
        (((b.InventoryStockInQualityInspection + b.InventoryStockRestricted) * m.ProductPlanningUnitsPerPallet) / m.ProductPlanningUnitsPerCase) * COALESCE(m.ProductListPrice, m.ProductStandardCost) AS restricted_dollars
        
    FROM batch_inventory_actual_temp b
    LEFT JOIN materialmaster_temp m ON b.MaterialID = m.MaterialID
),
weekly_inventory AS (
    SELECT
        MaterialID,
        LocationID,
        YEAR(Snapshotdate) AS year,
        WEEKOFYEAR(Snapshotdate) AS week,
        
        SUM(clear_pallets) AS clear_pallets_weekly,
        SUM(blocked_pallets) AS blocked_pallets_weekly,
        SUM(restricted_pallets) AS restricted_pallets_weekly,
        
        SUM(clear_cases) AS clear_cases_weekly,
        SUM(blocked_cases) AS blocked_cases_weekly,
        SUM(restricted_cases) AS restricted_cases_weekly,
        
        SUM(clear_dollars) AS clear_dollars_weekly,
        SUM(blocked_dollars) AS blocked_dollars_weekly,
        SUM(restricted_dollars) AS restricted_dollars_weekly
    FROM aggregated_inventory
    GROUP BY MaterialID, LocationID, YEAR(Snapshotdate), WEEKOFYEAR(Snapshotdate)
),
daily_inventory AS (
    SELECT
        MaterialID,
        LocationID,
        DATE(Snapshotdate) AS day,
        
        SUM(clear_pallets) AS clear_pallets_daily,
        SUM(blocked_pallets) AS blocked_pallets_daily,
        SUM(restricted_pallets) AS restricted_pallets_daily,
        
        SUM(clear_cases) AS clear_cases_daily,
        SUM(blocked_cases) AS blocked_cases_daily,
        SUM(restricted_cases) AS restricted_cases_daily,
        
        SUM(clear_dollars) AS clear_dollars_daily,
        SUM(blocked_dollars) AS blocked_dollars_daily,
        SUM(restricted_dollars) AS restricted_dollars_daily
    FROM aggregated_inventory
    GROUP BY MaterialID, LocationID, DATE(Snapshotdate)
),
total_inventory_weekly AS (
    SELECT
        year,
        week,
        SUM(clear_pallets_weekly + blocked_pallets_weekly + restricted_pallets_weekly) AS total_pallets_weekly,
        SUM(clear_cases_weekly + blocked_cases_weekly + restricted_cases_weekly) AS total_cases_weekly,
        SUM(clear_dollars_weekly + blocked_dollars_weekly + restricted_dollars_weekly) AS total_dollars_weekly
    FROM weekly_inventory
    GROUP BY year, week
),
total_inventory_daily AS (
    SELECT
        day,
        SUM(clear_pallets_daily + blocked_pallets_daily + restricted_pallets_daily) AS total_pallets_daily,
        SUM(clear_cases_daily + blocked_cases_daily + restricted_cases_daily) AS total_cases_daily,
        SUM(clear_dollars_daily + blocked_dollars_daily + restricted_dollars_daily) AS total_dollars_daily
    FROM daily_inventory
    GROUP BY day
)
WITH RankedData AS (
    SELECT
        DATE,
        LocationID,
        MaterialID,
        ConsensusDemandPlan,
        SalesActualCustomerOrders,
        DependentDemand,
        SnapshotDate,
        Modified_date,
        -- Ranking logic
        ROW_NUMBER() OVER (PARTITION BY DATE, LocationID, MaterialID ORDER BY SnapshotDate DESC, Modified_date DESC) AS Rank_latest
    FROM apo_snp_35d_temp
),
FilteredRankedData AS (
    SELECT
        DATE,
        LocationID,
        MaterialID,
        ConsensusDemandPlan,
        SalesActualCustomerOrders,
        DependentDemand,
        (ConsensusDemandPlan + SalesActualCustomerOrders + DependentDemand) AS Demand_Quantity_Cases
    FROM RankedData
    WHERE Rank_latest = 1
),
otr_data AS (
    SELECT 
        GoodsIssueDate AS DATE, 
        SUBSTRING(MaterialNumber, -8) AS MaterialID,
        ActualQuantityDeliveredInSalesUnits AS Actual_Quantity_Cases
    FROM otr_var_temp_view
),
joined_data AS (
    SELECT 
        otr.DATE, 
        otr.MaterialID, 
        otr.Actual_Quantity_Cases, 
        inv.Demand_Quantity_Cases,
        inv.LocationID
    FROM otr_data otr
    LEFT JOIN FilteredRankedData inv ON otr.DATE = inv.DATE 
                                      AND otr.MaterialID = inv.MaterialID
    -- Filter the data for the last 28 days
    WHERE otr.DATE >= DATE_SUB(CURRENT_DATE(), 28)
),
forecast_bias AS (
    SELECT
        DATE,
        LocationID,
        MaterialID,
        SUM(Actual_Quantity_Cases) AS Total_Actual_Quantity_Cases,
        SUM(Demand_Quantity_Cases) AS Total_Demand_Quantity_Cases,
        -- Calculate the Bias as absolute value (positive number)
        ABS((SUM(Demand_Quantity_Cases) - SUM(Actual_Quantity_Cases)) / SUM(Actual_Quantity_Cases)) AS Bias_Perc
    FROM joined_data
    GROUP BY LocationID, MaterialID, DATE
)


SELECT 
    -- Actual weekly values
    wi.MaterialID,
    wi.LocationID,
    wi.year,
    wi.week,
    wi.clear_pallets_weekly,
    wi.blocked_pallets_weekly,
    wi.restricted_pallets_weekly,
    wi.clear_cases_weekly,
    wi.blocked_cases_weekly,
    wi.restricted_cases_weekly,
    wi.clear_dollars_weekly,
    wi.blocked_dollars_weekly,
    wi.restricted_dollars_weekly,

    -- Weekly percentage
    (wi.clear_pallets_weekly / tiw.total_pallets_weekly) * 100 AS clear_pallets_weekly_pct,
    (wi.blocked_pallets_weekly / tiw.total_pallets_weekly) * 100 AS blocked_pallets_weekly_pct,
    (wi.restricted_pallets_weekly / tiw.total_pallets_weekly) * 100 AS restricted_pallets_weekly_pct,
    
    (wi.clear_cases_weekly / tiw.total_cases_weekly) * 100 AS clear_cases_weekly_pct,
    (wi.blocked_cases_weekly / tiw.total_cases_weekly) * 100 AS blocked_cases_weekly_pct,
    (wi.restricted_cases_weekly / tiw.total_cases_weekly) * 100 AS restricted_cases_weekly_pct,
    
    (wi.clear_dollars_weekly / tiw.total_dollars_weekly) * 100 AS clear_dollars_weekly_pct,
    (wi.blocked_dollars_weekly / tiw.total_dollars_weekly) * 100 AS blocked_dollars_weekly_pct,
    (wi.restricted_dollars_weekly / tiw.total_dollars_weekly) * 100 AS restricted_dollars_weekly_pct,

    -- Actual daily values
    di.day,
    di.clear_pallets_daily,
    di.blocked_pallets_daily,
    di.restricted_pallets_daily,
    di.clear_cases_daily,
    di.blocked_cases_daily,
    di.restricted_cases_daily,
    di.clear_dollars_daily,
    di.blocked_dollars_daily,
    di.restricted_dollars_daily,

    -- Daily percentage
    (di.clear_pallets_daily / tid.total_pallets_daily) * 100 AS clear_pallets_daily_pct,
    (di.blocked_pallets_daily / tid.total_pallets_daily) * 100 AS blocked_pallets_daily_pct,
    (di.restricted_pallets_daily / tid.total_pallets_daily) * 100 AS restricted_pallets_daily_pct,
    
    (di.clear_cases_daily / tid.total_cases_daily) * 100 AS clear_cases_daily_pct,
    (di.blocked_cases_daily / tid.total_cases_daily) * 100 AS blocked_cases_daily_pct,
    (di.restricted_cases_daily / tid.total_cases_daily) * 100 AS restricted_cases_daily_pct,
    
    (di.clear_dollars_daily / tid.total_dollars_daily) * 100 AS clear_dollars_daily_pct,
    (di.blocked_dollars_daily / tid.total_dollars_daily) * 100 AS blocked_dollars_daily_pct,
    (di.restricted_dollars_daily / tid.total_dollars_daily) * 100 AS restricted_dollars_daily_pct,
    mh.Division,
    mh.Network,
    mh.ProductLevel3CategoryID,
    fb.Total_Actual_Quantity_Cases,
    fb.Total_Demand_Quantity_Cases,
    fb.Bias_Perc

FROM weekly_inventory wi
LEFT JOIN total_inventory_weekly tiw ON wi.year = tiw.year AND wi.week = tiw.week
LEFT JOIN daily_inventory di ON wi.MaterialID = di.MaterialID AND wi.LocationID = di.LocationID
LEFT JOIN total_inventory_daily tid ON di.day = tid.day
LEFT JOIN mat_hierarachy_temp mh ON wi.MaterialID = mh.MaterialID
LEFT JOIN forecast_bias fb ON wi.MaterialID = fb.MaterialID AND wi.LocationID = fb.LocationID
"""

# COMMAND ----------

# WHERE wi.MaterialID = {Product_condition} AND wi.LocationID = {Dc_condition} AND mh.Network = '{Network_condition}' AND mh.Division ='{Division_condition}' AND mh.ProductLevel3CategoryID = '{PL3_condition}' 
