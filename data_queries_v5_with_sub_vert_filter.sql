-- This script is used to pull data for all **valid**, **inactive** experiments **without conditions in 2021 and beyond** so that we can calculate the significance on the treatment scope and experiment levels

/* 
V2 has several changes
1. "vertical_parent" filters changed
2. Amended the DPS logs code section

V3 has one change:
1. Removed the DPS logs code because it was taking too long to execute

V4 has one change:
1. Stopped using `dh-logistics-product-ops.pricing.experiments_quality_check` to get an initial pool of tests to work with and created my own logic

V5:
1. Added two conditions to step one to remove tests with no matching vendor IDs or parent vertical
*/
-- The only hard-coded value in this query is start_date = "2021-01-01"

-- Step 1: Pull the valid experiment names
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.valid_exp_names_mab_sig_analysis` AS 
SELECT DISTINCT
  entity_id,
  country_code,
  test_id,
  test_name
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE
  AND DATE(test_start_date) >= DATE('2021-01-01') -- Filter for tests that started on Jan 1st, 2021 or after that date
  AND DATE_DIFF(test_end_date, test_start_date, DAY) >= 7 -- The length of the experiment has to be 7 days or more
  AND is_active = FALSE -- Only look for setups of tests that ended
  AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
  AND schedule.id IS NULL -- Filter out tests with a time condition to simplify the analysis
  AND customer_condition.id IS NULL -- Filter out tests with a customer condition to simplify the analysis 
  AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
  AND parent_vertical IS NOT NULL; -- Filter out tests where there is no parent_vertical

###----------------------------------------------------------END OF VALID EXP NAMES PART----------------------------------------------------------###

-- Step 2: Extract the vendor IDs per target group along with their associated parent vertical and vertical type
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis` AS
WITH vendor_tg_vertical_mapping_with_dup AS (
  SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    vendor_group_id,
    vendor_id AS vendor_code,
    parent_vertical, -- The parent vertical can only assume 7 values 'Restaurant', 'Shop', 'darkstores', 'restaurant', 'restaurants', 'shop', or NULL. The differences are due platform configurations
    CONCAT('TG', DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS tg_name,
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
  CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
  WHERE TRUE 
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_mab_sig_analysis`)
    AND DATE(test_start_date) >= DATE('2021-01-01') -- Filter for tests that started on Jan 1st, 2021 or after that date
    AND DATE_DIFF(test_end_date, test_start_date, DAY) >= 7 -- The length of the experiment has to be 7 days or more
    AND is_active = FALSE -- Only look for setups of tests that ended
    AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
    AND schedule.id IS NULL -- Filter out tests with a time condition to simplify the analysis
    AND customer_condition.id IS NULL -- Filter out tests with a customer condition to simplify the analysis 
    AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
    AND parent_vertical IS NOT NULL -- Filter out tests where there is no parent_vertical
  QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, test_name, vendor_id ORDER BY vendor_group_id) = 1 -- If by mistake a vendor is included in two or more target groups, take the highest priority one
),

vendor_tg_vertical_mapping_agg AS (
  SELECT 
    * EXCEPT (parent_vertical),
    ARRAY_TO_STRING(ARRAY_AGG(parent_vertical RESPECT NULLS ORDER BY parent_vertical), ', ') AS parent_vertical_concat -- We do this step because some tests have two parent verticals. If we do not aggregate, we will get duplicates 
  FROM vendor_tg_vertical_mapping_with_dup 
  GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT
  a.*,
  CASE 
    WHEN parent_vertical_concat = '' THEN NULL -- Case 1
    WHEN parent_vertical_concat LIKE '%,%' THEN -- Case 2 (tests where multiple parent verticals were chosen during configuration)
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') = 'shop' THEN 'shop'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') = 'darkstores' THEN 'darkstores'
      END
    -- Case 3 (tests where a single parent vertical was chosen during configuration)
    WHEN LOWER(parent_vertical_concat) IN ('restaurant', 'restaurants') THEN 'restaurant'
    WHEN LOWER(parent_vertical_concat) = 'shop' THEN 'shop'
    WHEN LOWER(parent_vertical_concat) = 'darkstores' THEN 'darkstores'
  ELSE REGEXP_SUBSTR(parent_vertical_concat, r'(.*),\s') END AS first_parent_vertical,
  
  CASE
    WHEN parent_vertical_concat = '' THEN NULL
    WHEN parent_vertical_concat LIKE '%,%' THEN
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') = 'shop' THEN 'shop'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') = 'darkstores' THEN 'darkstores'
      END
  END AS second_parent_vertical,
  b.vertical_type -- Vertical type of the vendor (NOT parent vertical)
FROM vendor_tg_vertical_mapping_agg a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` b ON a.entity_id = b.global_entity_id AND a.vendor_code = b.vendor_id
ORDER BY 1,2,3,4,5;

-- Step 3: Extract the zones that are part of the experiment
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_zone_ids_mab_sig_analysis` AS
SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    zone_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.zone_ids) AS zone_id
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_mab_sig_analysis`)
  AND DATE(test_start_date) >= DATE('2021-01-01') -- Filter for tests that started on Jan 1st, 2021 or after that date
  AND DATE_DIFF(test_end_date, test_start_date, DAY) >= 7 -- The length of the experiment has to be 7 days or more
  AND is_active = FALSE -- Only look for setups of tests that ended
  AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
  AND schedule.id IS NULL -- Filter out tests with a time condition to simplify the analysis
  AND customer_condition.id IS NULL -- Filter out tests with a customer condition to simplify the analysis 
  AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
  AND parent_vertical IS NOT NULL -- Filter out tests where there is no parent_vertical
ORDER BY 1,2;

-- Step 4.1: Extract the target groups, variants, and price schemes of the tests
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_mab_sig_analysis` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    CONCAT('TG', priority) AS target_group,
    variation_group AS variant,
    price_scheme_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE 
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_mab_sig_analysis`)
  AND DATE(test_start_date) >= DATE('2021-01-01') -- Filter for tests that started on Jan 1st, 2021 or after that date
  AND DATE_DIFF(test_end_date, test_start_date, DAY) >= 7 -- The length of the experiment has to be 7 days or more
  AND is_active = FALSE -- Only look for setups of tests that ended
  AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
  AND schedule.id IS NULL -- Filter out tests with a time condition to simplify the analysis
  AND customer_condition.id IS NULL -- Filter out tests with a customer condition to simplify the analysis
  AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
  AND parent_vertical IS NOT NULL -- Filter out tests where there is no parent_vertical
ORDER BY 1,2;

-- Step 4.2: Find the distinct combinations of target groups, variants, and price schemes per test
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_mab_sig_analysis` AS
SELECT 
  entity_id,
  country_code,
  test_name,
  test_id,
  ARRAY_TO_STRING(ARRAY_AGG(CONCAT(target_group, ' | ', variant, ' | ', price_scheme_id)), ', ') AS tg_var_scheme_concat
FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_mab_sig_analysis`
GROUP BY 1,2,3,4;

-- Step 5: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_geo_data_mab_sig_analysis` AS
SELECT 
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id,
    tgt.test_name,
    tgt.test_id,
    tgt.test_start_date,
    tgt.test_end_date,
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_mab_sig_analysis` tgt ON p.entity_id = tgt.entity_id AND co.country_code = tgt.country_code AND zo.id = tgt.zone_id 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

###----------------------------------------------------------END OF EXP SETUPS PART----------------------------------------------------------###

-- Step 6: Pull the business KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_mab_sig_analysis` AS
WITH delivery_costs AS ( -- Get the delivery costs of orders
    SELECT
        p.entity_id,
        p.country_code,
        p.order_id, 
        o.platform_order_code,
        SUM(p.costs) AS delivery_costs_local,
        SUM(p.costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date >= DATE('2021-01-01') -- For partitioning elimination and speeding up the query
        AND o.created_date >= DATE('2021-01-01') -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3,4
),

test_start_and_end_dates AS ( -- Get the start and end dates per test
  SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id
  FROM `dh-logistics-product-ops.pricing.ab_test_zone_ids_mab_sig_analysis`
),

entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id AS test_id,
    dat.test_start_date,
    dat.test_end_date,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    COALESCE(tg.tg_name, 'Non_TG') AS target_group,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
      WHEN a.vendor_vertical_parent IS NULL THEN NULL 
      WHEN LOWER(a.vendor_vertical_parent) IN ('restaurant', 'restaurants') THEN 'restaurant'
      WHEN LOWER(a.vendor_vertical_parent) = 'shop' THEN 'shop'
      WHEN LOWER(a.vendor_vertical_parent) = 'darkstores' THEN 'darkstores'
    END AS vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    a.dps_delivery_fee_local,
    a.delivery_fee_local,
    a.commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee, 0) AS service_fee_local,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    cst.delivery_costs_local,
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END)
    END AS actual_df_paid_by_customer,

    -- Special fields
    a.is_delivery_fee_covered_by_discount, -- Needed in the profit formula
    a.is_delivery_fee_covered_by_voucher, -- Needed in the profit formula
    tg.parent_vertical_concat,
    -- This filter is used to clean the data. It removes all orders that did not belong to the correct target_group, variant, scheme_id combination as dictated by the experiment's setup
    CASE WHEN COALESCE(tg.tg_name, 'Non_TG') = 'Non_TG' OR vs.tg_var_scheme_concat LIKE CONCAT('%', COALESCE(tg.tg_name, 'Non_TG'), ' | ', a.variant, ' | ', a.scheme_id, '%') THEN 'Keep' ELSE 'Drop' END AS keep_drop_flag
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id AND a.country_code = cst.country_code -- The table that stores the CPO
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_mab_sig_analysis` zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id 
    AND a.experiment_id = zn.test_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis` tg 
  ON TRUE
    AND a.entity_id = tg.entity_id
    AND a.vendor_id = tg.vendor_code 
    AND a.experiment_id = tg.test_id -- Tag the vendors with their target group association
LEFT JOIN test_start_and_end_dates dat ON a.entity_id = dat.entity_id AND a.country_code = dat.country_code AND a.experiment_id = dat.test_id
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_mab_sig_analysis` vs ON a.entity_id = vs.entity_id AND a.country_code = vs.country_code AND a.experiment_id = vs.test_id
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date >= DATE('2021-01-01') -- Filter for all tests starting from the 1st of Jan 2021
    
    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id, ' | ', a.variant) IN ( -- Filter for the right variants belonging to the experiment
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) 
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_mab_sig_analysis`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) IS NOT NULL
    )
    -- AND a.is_own_delivery -- OD or MP (Comment out to include MP vendors in the non-TG)
    
    AND a.delivery_status = 'completed' -- Successful orders
    
    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      -- The "ab_test_target_groups_mab_sig_analysis" table was specifically chosen from the tables in steps 2-4 because it automatically eliminates tests where there are no matching vendors
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id) IS NOT NULL
    )
    
    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id, ' | ', a.vertical_type) IN ( -- Filter for orders from the right vertical (restuarants, groceries, darkstores, etc.)
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_type)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_type) IS NOT NULL
    )
    
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones

###----------------------------------------------------------SEPARATOR----------------------------------------------------------###

-- Step 7: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_mab_sig_analysis` AS
SELECT
  *,
  -- Revenue and profit formulas
  COALESCE(
      actual_df_paid_by_customer, 
      IF(is_delivery_fee_covered_by_discount = TRUE OR is_delivery_fee_covered_by_voucher = TRUE, 0, delivery_fee_local)
  ) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) AS revenue_local,

  COALESCE(
      actual_df_paid_by_customer, 
      IF(is_delivery_fee_covered_by_discount = TRUE OR is_delivery_fee_covered_by_voucher = TRUE, 0, delivery_fee_local)
  ) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local_cdwh) - delivery_costs_local AS gross_profit_local,
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_mab_sig_analysis`
WHERE TRUE -- Filter for orders from the right parent vertical (restuarants, shop, darkstores, etc.) per experiment
    AND (
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical) IS NOT NULL
      )
    );

###----------------------------------------------------------END OF INDIVIDUAL ORDERS PART----------------------------------------------------------###

-- Step 8: Clean the orders data by filtering for records where keep_drop_flag = 'Keep' (refer to the code above to see how this field was constructed)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_mab_sig_analysis` AS
SELECT
    *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_mab_sig_analysis`
WHERE TRUE
    AND keep_drop_flag = 'Keep'; -- Filter for the orders that have the correct target_group, variant, and scheme ID based on the configuration of the experiment

###----------------------------------------------------------END OF BUSINESS KPIS PART--------------------------------------------------------------###

-- Step 9: Pull front-end events data from dps_sessions_mapped_to_ga_sessions
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_stg_mab_sig_analysis` AS
SELECT DISTINCT
  x.created_date, -- Date of the ga session
  x.entity_id, -- Entity ID
  x.country_code, -- Country code
  x.platform, -- Operating system (iOS, Android, Web, etc.)
  x.brand, -- Talabat, foodpanda, Foodora, etc.
  x.events_ga_session_id, -- GA session ID
  x.fullvisitor_id, -- The visit_id defined by Google Analytics
  x.visit_id, -- 	The visit_id defined by Google Analytics
  x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
  x.total_transactions, -- The total number of transactions in the GA session
  x.ga_dps_session_id, -- DPS session ID

  x.sessions.dps_session_timestamp, -- The timestamp of the DPS logs
  x.sessions.endpoint, -- The endpoint from where the DPS request is coming
  x.sessions.perseus_client_id, -- A unique customer identifier based on the device
  x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
  x.sessions.experiment_id AS test_id, -- Experiment ID
  CASE 
    WHEN x.sessions.vertical_parent IS NULL THEN NULL 
    WHEN LOWER(x.sessions.vertical_parent) IN ('restaurant', 'restaurants') THEN 'restaurant'
    WHEN LOWER(x.sessions.vertical_parent) = 'shop' THEN 'shop'
    WHEN LOWER(x.sessions.vertical_parent) = 'darkstores' THEN 'darkstores'
  END AS vertical_parent, -- Parent vertical
  tg.parent_vertical_concat,
  x.sessions.customer_status, -- The customer.tag, indicating whether the customer is new or not
  x.sessions.location, -- The customer.location
  x.sessions.variant_concat, -- The concatenation of all the existing variants for the dps session id. There might be multiple variants due to location changes or session timeout
  x.sessions.location_concat, -- The concatenation of all the existing locations for the dps session id
  x.sessions.customer_status_concat, -- 	The concatenation of all the existing customer.tag for the dps session id

  e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
  e.vendor_code, -- Vendor ID
  -- Records where the event_type = home_screen.loaded OR shop_list.loaded will have vendor_code = NULL, so this field will take the value of 'Non_TG' for such records
  CASE WHEN e.vendor_code IS NULL THEN 'Unknown' ELSE COALESCE(tg.tg_name, 'Non_TG') END AS target_group,
  e.vertical_type, -- This field is NULL for event types home_screen.loaded and shop_list.loaded 
  e.event_time, -- The timestamp of the event's creation
  e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
  e.expedition_type, -- The delivery type of the session, pickup or delivery

  dps.city_id, -- City ID based on the DPS session
  dps.city_name, -- City name based on the DPS session
  dps.id AS zone_id, -- Zone ID based on the DPS session
  dps.name AS zone_name, -- Zone name based on the DPS session
  dps.timezone, -- Time zone of the city based on the DPS session

  ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` x
LEFT JOIN UNNEST(events) e
LEFT JOIN UNNEST(dps_zone) dps
-- IMPORTANT NOTE: WE INNER JOIN ON THE TARGET ZONES, THEN LEFT JOIN ON THE TARGET GROUPS TABLE
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_mab_sig_analysis` zn -- Filter for sessions in the target zones
  ON TRUE 
    AND zn.entity_id = x.entity_id 
    AND zn.country_code = x.country_code
    AND zn.zone_id = dps.id 
    AND zn.test_id = x.sessions.experiment_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis` tg -- Tag the vendors with their target group association
  ON TRUE
    AND tg.entity_id = x.entity_id
    AND tg.vendor_code = e.vendor_code 
    AND tg.test_id = x.sessions.experiment_id
WHERE TRUE
    AND x.created_date >= DATE('2021-01-01') -- Filter for all tests starting from the 1st of Jan 2021
    
    AND CONCAT(x.entity_id, ' | ', x.country_code, ' | ', x.sessions.experiment_id, ' | ', x.sessions.variant) IN ( -- Filter for the right variants belonging to the experiment
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant)
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_mab_sig_analysis`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) IS NOT NULL
    )
        
    AND CONCAT(x.entity_id, ' | ', x.country_code, ' | ', x.sessions.experiment_id) IN ( -- Filter for the right entity | experiment_id combos
      -- The "ab_test_target_groups_mab_sig_analysis" table was specifically chosen from the tables in steps 2-4 because it automatically eliminates tests where there are no matching vendors
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id) IS NOT NULL
    )
    
    AND (
      e.vertical_type IS NULL -- This condition is important because any events upstream of RDP don't have a vertical_type linked to them because of the unavailability of vendor IDs
      OR
      CONCAT(x.entity_id, ' | ', x.country_code, ' | ', x.sessions.experiment_id, ' | ', e.vertical_type) IN ( -- Filter for orders from the right vertical (restuarants, groceries, darkstores, etc.)
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_type)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_type) IS NOT NULL
      )
    );

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_mab_sig_analysis` AS
SELECT *
FROM `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_stg_mab_sig_analysis`
WHERE TRUE
    AND IF(
      parent_vertical_concat LIKE '%,%', -- Condition (if the experiment was configured with multiple parent verticals)
      -- Execute if the condition is TRUE
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical) IS NOT NULL
      ),
      -- Execute if the condition is FALSE
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical) IS NOT NULL
      )
    )
ORDER BY entity_id, test_id, created_date, events_ga_session_id, perseus_client_id, event_time;

###----------------------------------------------------------END OF GA SESSIONS PART--------------------------------------------------------------###

-- Step 10: Assign a flag to each session indicating if whether or not it was "in_treatment". If the target_group was "Unknown" because no RDP even existed in that session, the "in_treatment_flag" will also get a value of "Unknown"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_treatment_scope_flag_mab_sig_analysis` AS
SELECT
    created_date,
    entity_id,
    country_code,
    test_id,
    variant,
    events_ga_session_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT target_group IGNORE NULLS ORDER BY target_group), ", ") AS all_tgs_per_ga_session,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT vendor_code IGNORE NULLS ORDER BY vendor_code), ", ") AS all_vendor_codes_per_ga_session,
    -- If the session contains at least one vendor in a target group, it is considered a session in treatment
    -- If the only vendors seen from the RDP onwards were "Non_TG" vendors, then the session was **NOT** in treatment
    -- If ONLY the tag "Unknown" exists without TG(x) or Non_TG because the session did not have RDP events or any other events downstream of RDP, then we don't know the treatment status of the session
    CASE -- The order is very important here 
      WHEN REGEXP_CONTAINS(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT target_group IGNORE NULLS ORDER BY target_group), ", "), r'TG\d+') THEN 'Y' 
      WHEN REGEXP_CONTAINS(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT target_group IGNORE NULLS ORDER BY target_group), ", "), r'Non_TG') THEN 'N'
      ELSE 'Unknown' 
    END AS session_in_treatment_flag
FROM `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_mab_sig_analysis` e
GROUP BY 1,2,3,4,5,6;

-- Step 7.3: Join the previous two tables on `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_mab_sig_analysis`
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_mab_sig_analysis` AS
SELECT 
    a.*,
    b.all_tgs_per_ga_session,
    b.all_vendor_codes_per_ga_session,
    b.session_in_treatment_flag
FROM `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_mab_sig_analysis` a
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_treatment_scope_flag_mab_sig_analysis` b
    ON TRUE 
        AND a.created_date = b.created_date 
        AND a.entity_id = b.entity_id 
        AND a.country_code = b.country_code
        AND a.test_id = b.test_id
        AND a.variant = b.variant
        AND a.events_ga_session_id = b.events_ga_session_id;

###----------------------------------------------------------END OF TREATMENT FLAG PART--------------------------------------------------------------###

-- Step 11: Get a list of all entity | test_name | variant | perseus_client_id | target_group | session_in_treatment_flag combinations in "ab_test_dps_sessions_mapped_to_ga_sessions_data_stg_mab_sig_analysis"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.users_per_test_mab_sig_analysis` AS
SELECT DISTINCT
    s.entity_id,
    s.country_code,
    s.test_id,
    tst.test_name,
    tst.test_start_date,
    tst.test_end_date,
    tst.test_length,
    tst.first_parent_vertical,
    tst.second_parent_vertical,
    s.perseus_client_id,
    variant,
    target_group,
    session_in_treatment_flag
FROM `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_mab_sig_analysis` s
LEFT JOIN (
  SELECT DISTINCT 
    entity_id, 
    country_code, 
    test_id, 
    test_name, 
    test_start_date, 
    test_end_date,
    DATE_DIFF(test_end_date, test_start_date, DAY) AS test_length,
    first_parent_vertical,
    second_parent_vertical
  FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_mab_sig_analysis`
) tst USING(entity_id, country_code, test_id)
ORDER BY 1,2,3,4,5,6,7;

###----------------------------------------------------------END OF USERS PER TEST PART--------------------------------------------------------------###

-- Step 12: Join the KPIs to the table above
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.kpis_per_user_mab_sig_analysis` AS
SELECT
  s.entity_id,
  s.country_code,
  s.test_id,
  s.test_name,
  s.test_start_date,
  s.test_end_date,
  s.test_length,
  s.first_parent_vertical,
  s.second_parent_vertical,
  s.perseus_client_id,
  s.variant,
  s.target_group,
  s.session_in_treatment_flag,
  COALESCE(COUNT(DISTINCT m.order_id), 0) AS order_count_per_user,
  COALESCE(ROUND(SUM(m.revenue_local), 2), 0) AS revenue_per_user,
  COALESCE(ROUND(SUM(m.gross_profit_local), 2), 0) AS profit_per_user,
  CURRENT_TIMESTAMP() AS data_pull_timestamp
FROM `dh-logistics-product-ops.pricing.users_per_test_mab_sig_analysis` s
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_mab_sig_analysis` m USING(entity_id, country_code, test_id, perseus_client_id, target_group, variant)
-- This condition is very important because it removes all combinations of the grouping variables which there will SURELY be any matches for in the orders table. This also removes all records with session_in_treatment_flag = 'Unknown' 
-- If you keep it, you'd just unnecessarily introduce lots of zeroes to the dataset. We only want records where we know the target_group and treatment_flag
WHERE target_group != 'Unknown'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
QUALIFY DENSE_RANK() OVER (PARTITION BY entity_id, country_code, test_id, perseus_client_id ORDER BY variant) = 1 -- Eliminate users with more than one variant (< 0.1% of cases - This is simply data cleaning)
ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12;