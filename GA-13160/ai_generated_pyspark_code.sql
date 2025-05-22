-- BigQuery SQL Script for Migrating Alteryx Workflow

WITH date_transforms AS (
  SELECT 
    FORMAT_TIMESTAMP('%Y-%m-%d', CURRENT_TIMESTAMP()) AS current_date,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS yesterday,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), WEEK)) AS prior_week_start,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), WEEK), INTERVAL 1 DAY)) AS prior_week_end,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)) AS p1m_start,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH), INTERVAL 1 DAY)) AS p1m_end,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MONTH), MONTH)) AS p2m_start,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SUB(TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)) AS p2m_end,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MONTH), MONTH)) AS p3m_start,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SUB(TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MONTH), MONTH), INTERVAL 1 DAY)) AS p3m_end
)

SELECT 
  -- Extracted and transformed fields
  FORMAT_TIMESTAMP('%Y-%m-%d', CURRENT_TIMESTAMP()) AS run_date,
  so_audat AS so_date,
  fkdat AS bill_date,
  werks AS whs,
  vtweg AS dist_chnl_id,
  bezek AS dist_chnl_desc,
  zzfinclass AS fnc_id,
  COALESCE(zzfinclass, 'OTH') AS fnc_id,
  COALESCE(bezek, 'OTHER') AS fnc_desc,
  soldto_kunnr AS soldto,
  shipto_kunnr AS shipto,
  vgbel AS rfrnc_doc_num,
  COUNT(DISTINCT vbeln) AS invoice_lines,
  SUM(unit_land_cost) AS unit_land_cost,
  SUM(extnd_land_cst) AS landed_cost,
  SUM(so_netwr) AS so_netvalue_amt,
  SUM(so_netpr) AS so_netprice_amt,
  SUM(so_netwr) AS invoice_sales,
  SUM(so_netwr) AS extended_sales,
  SUM(so_netpr) AS extended_final_price,
  SUM(kzwi1) AS service_fee,
  SUM(kzwi4) AS ext_ship_hndl,
  SUM(kzwi5) AS ext_state_tax,
  SUM(kzwi6) AS ext_local_tax,
  SUM(fklmg) AS base_qty,
  SUM(fkimg) AS sell_qty,
  SUM(bmat_brgew) AS wgt,
  SUM(volum) AS vol,
  SUM(rf_trnsct_absrb_chrge_amt_ztv2) AS vendor_trans_absorb,
  SUM(vndr_drp_shp_absrb_amt_zss2_m2) AS vendor_drop_ship_absorb,
  SUM(ex_hdnl_drp_shp_val_zssh) AS ext_hndl_drop_absorb,
  SUM(rf_vendor_moc_absrb_amt_zsm2) AS vendor_moc_absorb,
  SUM(rf_trnsct_absorb_charge_amt_ztr2) AS trans_absorb_amt,
  SUM(trans_chrgs_frt_ztr1) AS trans_charge_amt,
  SUM(restocking_fee_zsrf) AS restock_fee,
  SUM(restock_fee_manual_zsrm) AS restock_fee_man,
  SUM(spcl_hndlng_chrg_fx_zh01) AS special_hndl_amt,
  SUM(vndr_hndlng_amt_zthm) AS vendor_hndl_amt,
  SUM(min_order_charge_usd_zsmo) AS moc_amt,
  SUM(moc_drop_shp_val_zssm) AS moc_drop_amt,
  SUM(fuel_surcharge_zsdf) AS fuel_surcharge,
  SUM(fuel_surcharge_override_zsdo) AS fuel_override_amt,
  SUM(rf_vndr_drp_shp_fee_amt_zssm_f_zthm) AS vendor_drop_ship_fee,
  SUM(markup_vendor_trans_fee_amt_zmt1) AS markup_vendor_trans,
  SUM(mark_up_dnd_zmvt) AS markup_hndl_fee,
  SUM(bulk_dist_fee_dlr_zmgb) AS bulk_dist_fee,
  SUM(bcf_rf_extnd_vlink_svc_fee) AS vl_srvc_fee,
  SUM(xtnd_n_vlnk_svc_fee_zvc12m1m3nm) AS ext_vl_svc_fee,
  SUM(low_uom_dist_fee_dlr_zmgl) AS lum_dist_fee,
  SUM(onsite_rep_fee_dlr_zmgo) AS onsite_rep_fee,
  SUM(hldy_dlvr_fee_dlr_zmgd) AS hldy_dlvr_fee,
  SUM(vbrp_brgew) AS wgt,
  SUM(vbrp_volum) AS vol,
  SUM(extnd_servc_fee) AS service_fee,
  SUM(extnd_shpng_hndlng) AS ext_ship_hndl,
  SUM(xtnd_state_tx) AS ext_state_tax,
  SUM(extnd_lcl_tx) AS ext_local_tax,
  SUM(vendr_trans_chrg_frt_ztv1) AS vendor_trans_charge,
  SUM(addtn_trans_fee_zsrh) AS addl_trns_fee,
  SUM(addtn_trans_fee_override_zsro) AS addl_trns_fee_ovrd,
  SUM(dropship_fee_value_zssf) AS dropship_fee,
  SUM(gov_dist_fee_dlr_zmgn) AS gov_fee,
  -- Calculated fields
  SUM(trans_charge_amt) + SUM(restock_fee) + SUM(special_hndl_amt) + SUM(vendor_hndl_amt) + SUM(moc_amt) + SUM(fuel_surcharge) AS bia_ship_hndl_amt,
  SUM(trans_charge_amt) + SUM(restock_fee) + SUM(special_hndl_amt) + SUM(vendor_hndl_amt) + SUM(moc_amt) + SUM(fuel_surcharge) + SUM(rush_order_fee) + SUM(vendr_trans_chrg_frt_ztv1) + SUM(markup_vendor_trans_fee_amt_zmt1) AS coe_ship_hndl_amt
FROM 
  `project.dataset.table`
WHERE 
  NOT (fnc_id IS NULL OR whs IS NULL OR dist_chnl_id IS NULL OR soldto IS NULL OR shipto IS NULL)
GROUP BY 
  so_audat, fkdat, werks, vtweg, bezek, zzfinclass, soldto_kunnr, shipto_kunnr, vgbel