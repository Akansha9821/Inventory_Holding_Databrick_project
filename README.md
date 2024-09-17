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

<img width="649" alt="Bias_Calculation_%_output" src="https://github.com/user-attachments/assets/423183dd-cfdd-4c11-a1d5-8a14ccc3ecf3">

<img width="649" alt="daily_weekly_output" src="https://github.com/user-attachments/assets/6208acc2-60ee-4d75-8bdc-046ce733bcca">

<img width="649" alt="db_cluster_info" src="https://github.com/user-attachments/assets/951d494e-830a-4a6a-ae68-3a59fafafba0">

<img width="649" alt="fill_databrick_ss" src="https://github.com/user-attachments/assets/e61386f0-3414-4590-8a0a-c9da2433befb">

<img width="649" alt="job_ss_1" src="https://github.com/user-attachments/assets/bb8b29ce-e0db-47a0-b62a-2868d183c290">

