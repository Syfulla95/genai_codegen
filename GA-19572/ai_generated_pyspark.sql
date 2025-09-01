-- 1. Date calculation including 2-, 4-, 5-week windows
WITH date_calculation AS (
  SELECT
    CURRENT_DATE()                                             AS today,
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)                   AS yesterday,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 7 DAY) AS prior_week_start,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 1 DAY) AS prior_week_end,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) AS p1m_start,
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)                   AS p1m_end,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 MONTH) AS p2m_start,
    DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS p2m_end,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH) AS p3m_start,
    DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 MONTH), INTERVAL 1 DAY) AS p3m_end,
    -- Additional sliding windows
    DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AS start_2wk,
    DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  AS end_2wk,
    DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY) AS start_4wk,
    DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY) AS end_4wk,
    DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AS start_5wk,
    DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY) AS end_5wk
),

-- 2. Sales data with full aggregations (null-to-zero applied)
sales_data AS (
  SELECT
    so_audat                                          AS SO_Date,
    fkdat                                             AS BILL_DATE,
    werks                                             AS Whs,
    CAST(vtweg AS STRING)                             AS DIST_CHNL_ID,
    zzfinclass                                        AS FNC_ID,
    bezek                                             AS FNC_DESC,
    soldto_kunnr                                      AS SOLDTO,
    shipto_kunnr                                      AS SHIPTO,
    vgbel                                             AS RFRNC_DOC_NUM,
    COUNT(DISTINCT VGBEL)                             AS Invoices,
    SUM(COALESCE(bill_itm_count,0))                   AS Invoice_Lines,
    SUM(COALESCE(so_netwr,0))                         AS SO_NetValue_Amt,
    SUM(COALESCE(so_netpr,0))                         AS SO_NetPrice_Amt,
    SUM(COALESCE(fkimg,0))                            AS SELL_QTY,
    SUM(COALESCE(fklmg,0))                            AS BASE_QTY,
    SUM(COALESCE(vbrp_brgew,0))                       AS WGT,
    SUM(COALESCE(vbrp_volum,0))                       AS VOL,
    SUM(COALESCE(extnd_land_cst,0))                   AS LANDED_COST,
    SUM(COALESCE(extnd_fnl_price,0))                  AS EXT_FINAL_PRICE,
    -- Override Invoice_Sales per Sum_EXTND_FNL_PRICE1
    SUM(COALESCE(extnd_fnl_price,0))                  AS Invoice_Sales,
    -- Custom fees
    SUM(COALESCE(addtn_trans_fee_ovrride_zsro,0))     AS Rush_Order_Fee,
    SUM(COALESCE(rf_trnsct_absorb_charge_amt_ztr2,0)) AS Trans_Absorb_Amt,
    SUM(COALESCE(rf_trnsct_charge_amt_ztrm,0))        AS Trans_Charge_Amt,
    SUM(COALESCE(vndr_hndlng_amt_zthm,0))             AS Vendor_Hndl_Amt,
    SUM(COALESCE(rf_min_order_charge_amt_zsmo,0))    AS MOC_Amt,
    SUM(COALESCE(fuel_surcharge_zsdf,0))              AS Fuel_Surcharge,
    SUM(COALESCE(markup_vendor_trans_fee_amt_zmt1,0)) AS Markup_Vendor_Trans
  FROM `velvety-tangent-342017.Altreryx.table_updated_new_1000`
  CROSS JOIN date_calculation
  WHERE so_audat BETWEEN date_calculation.p2m_end
                     AND date_calculation.today
  GROUP BY so_audat, fkdat, werks, CAST(vtweg AS STRING), zzfinclass, bezek, soldto_kunnr, shipto_kunnr, vgbel
),

-- 3. Channel data
channel_data AS (
  SELECT
    CAST(dist_chnl_id AS STRING) AS dist_chnl_id,
    dist_chnl_desc               AS dist_chnl_desc,
    direct_std_cost              AS direct_std_cost,
    net_rev_amt                  AS net_rev_amt,
    rev_cost                     AS rev_cost
  FROM `velvety-tangent-342017.Altreryx.tdmedpod_new`
  WHERE bill_dte = '2019-07-01' AND whs = 'D0CG'
),

-- 4. Final joined data
final_data AS (
  SELECT
    sd.SO_Date,
    sd.BILL_DATE,
    sd.Whs,
    sd.DIST_CHNL_ID,
    cd.dist_chnl_desc,
    sd.FNC_ID,
    sd.FNC_DESC,
    sd.SOLDTO,
    sd.SHIPTO,
    sd.RFRNC_DOC_NUM,
    sd.Invoices,
    sd.Invoice_Lines,
    sd.Invoice_Sales,
    sd.EXT_FINAL_PRICE    AS ext_sales,
    sd.Rush_Order_Fee,
    -- Formula-based repackaging
    (sd.Trans_Charge_Amt + sd.Vendor_Hndl_Amt + sd.MOC_Amt + sd.Fuel_Surcharge) AS BIA_Ship_Hndl_Amt,
    (sd.Trans_Absorb_Amt + sd.Markup_Vendor_Trans)                           AS COE_Ship_Hndl_Amt,
  FROM sales_data sd
  LEFT JOIN channel_data cd
    ON sd.DIST_CHNL_ID = cd.dist_chnl_id
)

-- 5. Persist to BigQuery table
SELECT * FROM final_data;