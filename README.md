## Inventory and Bias Calculation Logic


## 1. Daily Matrix Calculation
<b>Input Tables:</b> batch_inventory_actual_temp, materialmaster_temp

<b>Join Condition:</b> MaterialID

### Steps:
- Select MaterialID, LocationID, and stock-related columns (e.g., Unrestricted, Blocked, and Restricted stock) from batch_inventory_actual_temp.
- Join with materialmaster_temp to bring in columns related to product dimensions like ProductPlanningUnitsPerPallet, ProductPlanningUnitsPerCase, and ProductListPrice.

### Calculate:
- Pallets: Convert each stock type into pallets by dividing stock by ProductPlanningUnitsPerPallet.
- Cases: Convert each stock type into cases by multiplying the stock by ProductPlanningUnitsPerPallet and dividing by ProductPlanningUnitsPerCase.
- Dollars: Convert cases into dollars by multiplying cases by either ProductListPrice or ProductStandardCost.

## 2. Weekly Metric Conversion
- Input Tables: Today's inventory DataFrame, Last week's inventory DataFrame.
- Join Condition: MaterialID

#### <b>Steps:</b>
- Calculate the inventory data for today using the logic from the daily calculation.
- Fetch last week's inventory data by adjusting the date back by 7 days.
- Perform a left join between today's and last week's inventory based on MaterialID.
- For each stock type (Clear, Blocked, Restricted), calculate:
  
#### Last Week Inventory
- Percentage Change: Compute the percentage difference between today's inventory and last week's inventory.


## 3. Days on Hand (DOH) Calculation
- DOH Today:
- Input Tables: Inventory data, Demand forecast table.
  
#### <b>Steps:</b>
- Calculate today's inventory as shown in the daily matrix.
- Calculate DOH Today as the ratio of inventory to daily demand.
  
#### <b>DOH Yesterday:</b>
#### <b>Input Tables:</b> Yesterday's inventory data.
#### <b>Steps:</b>
- Fetch yesterday's inventory by setting the date to one day prior.
- Calculate DOH Yesterday as the ratio of yesterday's inventory to daily demand.


## 4. Bias Calculation
#### <b>Input Tables:</b> otr_var, inventory_planning_final
#### * Join Condition: MaterialID
#### <b>Steps:</b>
- Extract the last 8 digits from MaterialNumber in otr_var to create MaterialID.
- Perform a left join between otr_var and inventory_planning_final based on MaterialID.
  
#### * Calculate Bias using the formula:
## Bias = (sum(Demand_Quantity(cases)-sum(Actual_Demand_Quantity(cases)) / sum(Actual_demand(cases))

###### I've attached the output data frame result with the Scheduled job ss.

![1. Daily Metrics Calculation Inventory](https://github.com/Akansha9821/Inventory_Holding_Databrick_project/blob/main/daily_weekly_output.png?raw=true)
![2. Bias Metrics Calculation Inventory](https://github.com/Akansha9821/Inventory_Holding_Databrick_project/blob/main/Bias_Calculation_%25_output.png?raw=true)
![3. Cluster Info](https://github.com/Akansha9821/Inventory_Holding_Databrick_project/blob/main/db_cluster_info.png?raw=true)
![4. Inventory JOB Schedule](https://github.com/Akansha9821/Inventory_Holding_Databrick_project/blob/main/fill_databrick_ss.png?raw=true)
![5. Inventory JOB Schedule 2](https://github.com/Akansha9821/Inventory_Holding_Databrick_project/blob/main/job_ss_1.png?raw=true)

