/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.exp;

import java.util.List;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TPCDSSchema {
    public static final Schema CALL_CENTER =
            new Schema(
                    Types.StructType.of(
                                    required(1, "cc_call_center_sk", Types.IntegerType.get()),
                                    required(2, "cc_call_center_id", Types.StringType.get()),
                                    required(3, "cc_rec_start_date", Types.StringType.get()),
                                    required(4, "cc_rec_end_date", Types.StringType.get()),
                                    required(5, "cc_closed_date_sk", Types.IntegerType.get()),
                                    required(6, "cc_open_date_sk", Types.IntegerType.get()),
                                    required(7, "cc_name", Types.StringType.get()),
                                    required(8, "cc_class", Types.StringType.get()),
                                    required(9, "cc_employees", Types.IntegerType.get()),
                                    required(10, "cc_sq_ft", Types.IntegerType.get()),
                                    required(11, "cc_hours", Types.StringType.get()),
                                    required(12, "cc_manager", Types.StringType.get()),
                                    required(13, "cc_mkt_id", Types.IntegerType.get()),
                                    required(14, "cc_mkt_class", Types.StringType.get()),
                                    required(15, "cc_mkt_desc", Types.StringType.get()),
                                    required(16, "cc_market_manager", Types.StringType.get()),
                                    required(17, "cc_division", Types.IntegerType.get()),
                                    required(18, "cc_division_name", Types.StringType.get()),
                                    required(19, "cc_company", Types.IntegerType.get()),
                                    required(20, "cc_company_name", Types.StringType.get()),
                                    required(21, "cc_street_number", Types.StringType.get()),
                                    required(22, "cc_street_name", Types.StringType.get()),
                                    required(23, "cc_street_type", Types.StringType.get()),
                                    required(24, "cc_suite_number", Types.StringType.get()),
                                    required(25, "cc_city", Types.StringType.get()),
                                    required(26, "cc_county", Types.StringType.get()),
                                    required(27, "cc_state", Types.StringType.get()),
                                    required(28, "cc_zip", Types.StringType.get()),
                                    required(29, "cc_country", Types.StringType.get()),
                                    required(30, "cc_gmt_offset", Types.IntegerType.get()),
                                    required(31, "cc_tax_percentage", Types.IntegerType.get()))
                            .fields());

    public static final Schema CATALOG_PAGE =
            new Schema(
                    Types.StructType.of(
                                    required(1, "cp_catalog_page_sk", Types.IntegerType.get()),
                                    required(2, "cp_catalog_page_id", Types.StringType.get()),
                                    required(3, "cp_start_date_sk", Types.IntegerType.get()),
                                    required(4, "cp_end_date_sk", Types.IntegerType.get()),
                                    required(5, "cp_department", Types.StringType.get()),
                                    required(6, "cp_catalog_number", Types.IntegerType.get()),
                                    required(7, "cp_catalog_page_number", Types.IntegerType.get()),
                                    required(8, "cp_description", Types.StringType.get()),
                                    required(9, "cp_type", Types.StringType.get()))
                            .fields());

    public static final Schema CATALOG_RETURNS =
            new Schema(
                    Types.StructType.of(
                                    required(1, "cr_returned_date_sk", Types.IntegerType.get()),
                                    required(2, "cr_returned_time_sk", Types.IntegerType.get()),
                                    required(3, "cr_item_sk", Types.IntegerType.get()),
                                    required(4, "cr_refunded_customer_sk", Types.IntegerType.get()),
                                    required(5, "cr_refunded_cdemo_sk", Types.IntegerType.get()),
                                    required(6, "cr_refunded_hdemo_sk", Types.IntegerType.get()),
                                    required(7, "cr_refunded_addr_sk", Types.IntegerType.get()),
                                    required(8, "cr_returning_customer_sk", Types.IntegerType.get()),
                                    required(9, "cr_returning_cdemo_sk", Types.IntegerType.get()),
                                    required(10, "cr_returning_hdemo_sk", Types.IntegerType.get()),
                                    required(11, "cr_returning_addr_sk", Types.IntegerType.get()),
                                    required(12, "cr_call_center_sk", Types.IntegerType.get()),
                                    required(13, "cr_catalog_page_sk", Types.IntegerType.get()),
                                    required(14, "cr_ship_mode_sk", Types.IntegerType.get()),
                                    required(15, "cr_warehouse_sk", Types.IntegerType.get()),
                                    required(16, "cr_reason_sk", Types.IntegerType.get()),
                                    required(17, "cr_order_number", Types.IntegerType.get()),
                                    required(18, "cr_return_quantity", Types.IntegerType.get()),
                                    required(19, "cr_return_amount", Types.IntegerType.get()),
                                    required(20, "cr_return_tax", Types.IntegerType.get()),
                                    required(21, "cr_return_amt_inc_tax", Types.IntegerType.get()),
                                    required(22, "cr_fee", Types.IntegerType.get()),
                                    required(23, "cr_return_ship_cost", Types.IntegerType.get()),
                                    required(24, "cr_refunded_cash", Types.IntegerType.get()),
                                    required(25, "cr_reversed_charge", Types.IntegerType.get()),
                                    required(26, "cr_store_credit", Types.IntegerType.get()),
                                    required(27, "cr_net_loss", Types.IntegerType.get()))
                            .fields());

    public static final Schema CATALOG_SALES =
            new Schema(
                    Types.StructType.of(
                                    required(1, "cs_sold_date_sk", Types.IntegerType.get()),
                                    required(2, "cs_sold_time_sk", Types.IntegerType.get()),
                                    required(3, "cs_ship_date_sk", Types.IntegerType.get()),
                                    required(4, "cs_bill_customer_sk", Types.IntegerType.get()),
                                    required(5, "cs_bill_cdemo_sk", Types.IntegerType.get()),
                                    required(6, "cs_bill_hdemo_sk", Types.IntegerType.get()),
                                    required(7, "cs_bill_addr_sk", Types.IntegerType.get()),
                                    required(8, "cs_ship_customer_sk", Types.IntegerType.get()),
                                    required(9, "cs_ship_cdemo_sk", Types.IntegerType.get()),
                                    required(10, "cs_ship_hdemo_sk", Types.IntegerType.get()),
                                    required(11, "cs_ship_addr_sk", Types.IntegerType.get()),
                                    required(12, "cs_call_center_sk", Types.IntegerType.get()),
                                    required(13, "cs_catalog_page_sk", Types.IntegerType.get()),
                                    required(14, "cs_ship_mode_sk", Types.IntegerType.get()),
                                    required(15, "cs_warehouse_sk", Types.IntegerType.get()),
                                    required(16, "cs_item_sk", Types.IntegerType.get()),
                                    required(17, "cs_promo_sk", Types.IntegerType.get()),
                                    required(18, "cs_order_number", Types.IntegerType.get()),
                                    required(19, "cs_quantity", Types.IntegerType.get()),
                                    required(20, "cs_wholesale_cost", Types.IntegerType.get()),
                                    required(21, "cs_list_price", Types.IntegerType.get()),
                                    required(22, "cs_sales_price", Types.IntegerType.get()),
                                    required(23, "cs_ext_discount_amt", Types.IntegerType.get()),
                                    required(24, "cs_ext_sales_price", Types.IntegerType.get()),
                                    required(25, "cs_ext_wholesale_cost", Types.IntegerType.get()),
                                    required(26, "cs_ext_list_price", Types.IntegerType.get()),
                                    required(27, "cs_ext_tax", Types.IntegerType.get()),
                                    required(28, "cs_coupon_amt", Types.IntegerType.get()),
                                    required(29, "cs_ext_ship_cost", Types.IntegerType.get()),
                                    required(30, "cs_net_paid", Types.IntegerType.get()),
                                    required(31, "cs_net_paid_inc_tax", Types.IntegerType.get()),
                                    required(32, "cs_net_paid_inc_ship", Types.IntegerType.get()),
                                    required(33, "cs_net_paid_inc_ship_tax", Types.IntegerType.get()),
                                    required(34, "cs_net_profit", Types.IntegerType.get()))
                            .fields());

    public static final Schema CUSTOMER_ADDRESS =
            new Schema(
                    Types.StructType.of(
                                    required(1, "ca_address_sk", Types.IntegerType.get()),
                                    required(2, "ca_address_id", Types.StringType.get()),
                                    required(3, "ca_street_number", Types.StringType.get()),
                                    required(4, "ca_street_name", Types.StringType.get()),
                                    required(5, "ca_street_type", Types.StringType.get()),
                                    required(6, "ca_suite_number", Types.StringType.get()),
                                    required(7, "ca_city", Types.StringType.get()),
                                    required(8, "ca_county", Types.StringType.get()),
                                    required(9, "ca_state", Types.StringType.get()),
                                    required(10, "ca_zip", Types.StringType.get()),
                                    required(11, "ca_country", Types.StringType.get()),
                                    required(12, "ca_gmt_offset", Types.IntegerType.get()),
                                    required(13, "ca_location_type", Types.StringType.get()))
                            .fields());

    public static final Schema CUSTOMER_DEMOGRAPHICS =
            new Schema(
                    Types.StructType.of(
                                    required(1, "cd_demo_sk", Types.IntegerType.get()),
                                    required(2, "cd_gender", Types.StringType.get()),
                                    required(3, "cd_marital_status", Types.StringType.get()),
                                    required(4, "cd_education_status", Types.StringType.get()),
                                    required(5, "cd_purchase_estimate", Types.IntegerType.get()),
                                    required(6, "cd_credit_rating", Types.StringType.get()),
                                    required(7, "cd_dep_count", Types.IntegerType.get()),
                                    required(8, "cd_dep_employed_count", Types.IntegerType.get()),
                                    required(9, "cd_dep_college_count", Types.IntegerType.get()))
                            .fields());

    public static final Schema CUSTOMER =
            new Schema(
                    Types.StructType.of(
                                    required(1, "c_customer_sk", Types.IntegerType.get()),
                                    required(2, "c_customer_id", Types.StringType.get()),
                                    required(3, "c_current_cdemo_sk", Types.IntegerType.get()),
                                    required(4, "c_current_hdemo_sk", Types.IntegerType.get()),
                                    required(5, "c_current_addr_sk", Types.IntegerType.get()),
                                    required(6, "c_first_shipto_date_sk", Types.IntegerType.get()),
                                    required(7, "c_first_sales_date_sk", Types.IntegerType.get()),
                                    required(8, "c_salutation", Types.StringType.get()),
                                    required(9, "c_first_name", Types.StringType.get()),
                                    required(10, "c_last_name", Types.StringType.get()),
                                    required(11, "c_preferred_cust_flag", Types.StringType.get()),
                                    required(12, "c_birth_day", Types.IntegerType.get()),
                                    required(13, "c_birth_month", Types.IntegerType.get()),
                                    required(14, "c_birth_year", Types.IntegerType.get()),
                                    required(15, "c_birth_country", Types.StringType.get()),
                                    required(16, "c_login", Types.StringType.get()),
                                    required(17, "c_email_address", Types.StringType.get()),
                                    required(18, "c_last_review_date_sk", Types.StringType.get()))
                            .fields());

    public static final Schema DATE_DIM =
            new Schema(
                    Types.StructType.of(
                                    required(1, "d_date_sk", Types.IntegerType.get()),
                                    required(2, "d_date_id", Types.StringType.get()),
                                    required(3, "d_date", Types.StringType.get()),
                                    required(4, "d_month_seq", Types.IntegerType.get()),
                                    required(5, "d_week_seq", Types.IntegerType.get()),
                                    required(6, "d_quarter_seq", Types.IntegerType.get()),
                                    required(7, "d_year", Types.IntegerType.get()),
                                    required(8, "d_dow", Types.IntegerType.get()),
                                    required(9, "d_moy", Types.IntegerType.get()),
                                    required(10, "d_dom", Types.IntegerType.get()),
                                    required(11, "d_qoy", Types.IntegerType.get()),
                                    required(12, "d_fy_year", Types.IntegerType.get()),
                                    required(13, "d_fy_quarter_seq", Types.IntegerType.get()),
                                    required(14, "d_fy_week_seq", Types.IntegerType.get()),
                                    required(15, "d_day_name", Types.StringType.get()),
                                    required(16, "d_quarter_name", Types.StringType.get()),
                                    required(17, "d_holiday", Types.StringType.get()),
                                    required(18, "d_weekend", Types.StringType.get()),
                                    required(19, "d_following_holiday", Types.StringType.get()),
                                    required(20, "d_first_dom", Types.IntegerType.get()),
                                    required(21, "d_last_dom", Types.IntegerType.get()),
                                    required(22, "d_same_day_ly", Types.IntegerType.get()),
                                    required(23, "d_same_day_lq", Types.IntegerType.get()),
                                    required(24, "d_current_day", Types.StringType.get()),
                                    required(25, "d_current_week", Types.StringType.get()),
                                    required(26, "d_current_month", Types.StringType.get()),
                                    required(27, "d_current_quarter", Types.StringType.get()),
                                    required(28, "d_current_year", Types.StringType.get()))
                            .fields());

    public static final Schema HOUSEHOLD_DEMOGRAPHICS =
            new Schema(
                    Types.StructType.of(
                                    required(1, "hd_demo_sk", Types.IntegerType.get()),
                                    required(2, "hd_income_band_sk", Types.IntegerType.get()),
                                    required(3, "hd_buy_potential", Types.StringType.get()),
                                    required(4, "hd_dep_count", Types.IntegerType.get()),
                                    required(5, "hd_vehicle_count", Types.IntegerType.get()))
                            .fields());

    public static final Schema INCOME_BAND =
            new Schema(
                    Types.StructType.of(
                                    required(1, "ib_income_band_sk", Types.IntegerType.get()),
                                    required(2, "ib_lower_bound", Types.IntegerType.get()),
                                    required(3, "ib_upper_bound", Types.IntegerType.get()))
                            .fields());

    public static final Schema INVENTORY =
            new Schema(
                    Types.StructType.of(
                                    required(1, "inv_item_sk", Types.IntegerType.get()),
                                    required(2, "inv_warehouse_sk", Types.IntegerType.get()),
                                    required(3, "inv_quantity_on_hand", Types.IntegerType.get()),
                                    required(4, "inv_date_sk", Types.IntegerType.get()))
                            .fields());

    public static final Schema ITEM =
            new Schema(
                    Types.StructType.of(
                                    required(1, "i_item_sk", Types.IntegerType.get()),
                                    required(2, "i_item_id", Types.StringType.get()),
                                    required(3, "i_rec_start_date", Types.StringType.get()),
                                    required(4, "i_rec_end_date", Types.StringType.get()),
                                    required(5, "i_item_desc", Types.StringType.get()),
                                    required(6, "i_current_price", Types.IntegerType.get()),
                                    required(7, "i_wholesale_cost", Types.IntegerType.get()),
                                    required(8, "i_brand_id", Types.IntegerType.get()),
                                    required(9, "i_brand", Types.StringType.get()),
                                    required(10, "i_class_id", Types.IntegerType.get()),
                                    required(11, "i_class", Types.StringType.get()),
                                    required(12, "i_category_id", Types.IntegerType.get()),
                                    required(13, "i_category", Types.StringType.get()),
                                    required(14, "i_manufact_id", Types.IntegerType.get()),
                                    required(15, "i_manufact", Types.StringType.get()),
                                    required(16, "i_size", Types.StringType.get()),
                                    required(17, "i_formulation", Types.StringType.get()),
                                    required(18, "i_color", Types.StringType.get()),
                                    required(19, "i_units", Types.StringType.get()),
                                    required(20, "i_container", Types.StringType.get()),
                                    required(21, "i_manager_id", Types.IntegerType.get()),
                                    required(22, "i_product_name", Types.StringType.get()))
                            .fields());

    public static final Schema PROMOTION =
            new Schema(
                    Types.StructType.of(
                                    required(1, "p_promo_sk", Types.IntegerType.get()),
                                    required(2, "p_promo_id", Types.StringType.get()),
                                    required(3, "p_start_date_sk", Types.IntegerType.get()),
                                    required(4, "p_end_date_sk", Types.IntegerType.get()),
                                    required(5, "p_item_sk", Types.IntegerType.get()),
                                    required(6, "p_cost", Types.IntegerType.get()),
                                    required(7, "p_response_target", Types.IntegerType.get()),
                                    required(8, "p_promo_name", Types.StringType.get()),
                                    required(9, "p_channel_dmail", Types.StringType.get()),
                                    required(10, "p_channel_email", Types.IntegerType.get()),
                                    required(11, "p_channel_catalog", Types.StringType.get()),
                                    required(12, "p_channel_tv", Types.StringType.get()),
                                    required(13, "p_channel_radio", Types.StringType.get()),
                                    required(14, "p_channel_press", Types.StringType.get()),
                                    required(15, "p_channel_event", Types.StringType.get()),
                                    required(16, "p_channel_demo", Types.StringType.get()),
                                    required(17, "p_channel_details", Types.StringType.get()),
                                    required(18, "p_purpose", Types.StringType.get()),
                                    required(19, "p_discount_active", Types.StringType.get()))
                            .fields());

    public static final Schema REASON =
            new Schema(
                    Types.StructType.of(
                                    required(1, "r_reason_sk", Types.IntegerType.get()),
                                    required(2, "r_reason_id", Types.StringType.get()),
                                    required(3, "r_reason_desc", Types.StringType.get()))
                            .fields());

    public static final Schema SHIP_MODE =
            new Schema(
                    Types.StructType.of(
                                    required(1, "sm_ship_mode_sk", Types.IntegerType.get()),
                                    required(2, "sm_ship_mode_id", Types.StringType.get()),
                                    required(3, "sm_type", Types.StringType.get()),
                                    required(4, "sm_code", Types.StringType.get()),
                                    required(5, "sm_carrier", Types.StringType.get()),
                                    required(6, "sm_contract", Types.StringType.get()))
                            .fields());

    public static final Schema STORE_RETURNS =
            new Schema(
                    Types.StructType.of(
                                    required(1, "sr_returned_date_sk", Types.IntegerType.get()),
                                    required(2, "sr_return_time_sk", Types.IntegerType.get()),
                                    required(3, "sr_item_sk", Types.IntegerType.get()),
                                    required(4, "sr_customer_sk", Types.IntegerType.get()),
                                    required(5, "sr_cdemo_sk", Types.IntegerType.get()),
                                    required(6, "sr_hdemo_sk", Types.IntegerType.get()),
                                    required(7, "sr_addr_sk", Types.IntegerType.get()),
                                    required(8, "sr_store_sk", Types.IntegerType.get()),
                                    required(9, "sr_reason_sk", Types.IntegerType.get()),
                                    required(10, "sr_ticket_number", Types.IntegerType.get()),
                                    required(11, "sr_return_quantity", Types.IntegerType.get()),
                                    required(12, "sr_return_amt", Types.IntegerType.get()),
                                    required(13, "sr_return_tax", Types.IntegerType.get()),
                                    required(14, "sr_return_amt_inc_tax", Types.IntegerType.get()),
                                    required(15, "sr_fee", Types.IntegerType.get()),
                                    required(16, "sr_return_ship_cost", Types.IntegerType.get()),
                                    required(17, "sr_refunded_cash", Types.IntegerType.get()),
                                    required(18, "sr_reversed_charge", Types.IntegerType.get()),
                                    required(19, "sr_store_credit", Types.IntegerType.get()),
                                    required(20, "sr_net_loss", Types.IntegerType.get()))
                            .fields());

    public static final Schema STORE_SALES =
            new Schema(
                    Types.StructType.of(
                                    required(1, "ss_sold_date_sk", Types.IntegerType.get()),
                                    required(2, "ss_sold_time_sk", Types.IntegerType.get()),
                                    required(3, "ss_item_sk", Types.IntegerType.get()),
                                    required(4, "ss_customer_sk", Types.IntegerType.get()),
                                    required(5, "ss_cdemo_sk", Types.IntegerType.get()),
                                    required(6, "ss_hdemo_sk", Types.IntegerType.get()),
                                    required(7, "ss_addr_sk", Types.IntegerType.get()),
                                    required(8, "ss_store_sk", Types.IntegerType.get()),
                                    required(9, "ss_promo_sk", Types.IntegerType.get()),
                                    required(10, "ss_ticket_number", Types.IntegerType.get()),
                                    required(11, "ss_quantity", Types.IntegerType.get()),
                                    required(12, "ss_wholesale_cost", Types.IntegerType.get()),
                                    required(13, "ss_list_price", Types.IntegerType.get()),
                                    required(14, "ss_sales_price", Types.IntegerType.get()),
                                    required(15, "ss_ext_discount_amt", Types.IntegerType.get()),
                                    required(16, "ss_ext_sales_price", Types.IntegerType.get()),
                                    required(17, "ss_ext_wholesale_cost", Types.IntegerType.get()),
                                    required(18, "ss_ext_list_price", Types.IntegerType.get()),
                                    required(19, "ss_ext_tax", Types.IntegerType.get()),
                                    required(20, "ss_coupon_amt", Types.IntegerType.get()),
                                    required(21, "ss_net_paid", Types.IntegerType.get()),
                                    required(22, "ss_net_paid_inc_tax", Types.IntegerType.get()),
                                    required(23, "ss_net_profit", Types.IntegerType.get()))
                            .fields());

    public static final Schema STORE =
            new Schema(
                    Types.StructType.of(
                                    required(1, "s_store_sk", Types.IntegerType.get()),
                                    required(2, "s_store_id", Types.StringType.get()),
                                    required(3, "s_rec_start_date", Types.StringType.get()),
                                    required(4, "s_rec_end_date", Types.StringType.get()),
                                    required(5, "s_closed_date_sk", Types.IntegerType.get()),
                                    required(6, "s_store_name", Types.StringType.get()),
                                    required(7, "s_number_employees", Types.IntegerType.get()),
                                    required(8, "s_floor_space", Types.IntegerType.get()),
                                    required(9, "s_hours", Types.StringType.get()),
                                    required(10, "s_manager", Types.StringType.get()),
                                    required(11, "s_market_id", Types.IntegerType.get()),
                                    required(12, "s_geography_class", Types.StringType.get()),
                                    required(13, "s_market_desc", Types.StringType.get()),
                                    required(14, "s_market_manager", Types.StringType.get()),
                                    required(15, "s_division_id", Types.IntegerType.get()),
                                    required(16, "s_division_name", Types.StringType.get()),
                                    required(17, "s_company_id", Types.IntegerType.get()),
                                    required(18, "s_company_name", Types.StringType.get()),
                                    required(19, "s_street_number", Types.StringType.get()),
                                    required(20, "s_street_name", Types.StringType.get()),
                                    required(21, "s_street_type", Types.StringType.get()),
                                    required(22, "s_suite_number", Types.StringType.get()),
                                    required(23, "s_city", Types.StringType.get()),
                                    required(24, "s_county", Types.StringType.get()),
                                    required(25, "s_state", Types.StringType.get()),
                                    required(26, "s_zip", Types.StringType.get()),
                                    required(27, "s_country", Types.StringType.get()),
                                    required(28, "s_gmt_offset", Types.IntegerType.get()),
                                    required(29, "s_tax_precentage", Types.IntegerType.get()))
                            .fields());

    public static final Schema TIME_DIM =
            new Schema(
                    Types.StructType.of(
                                    required(1, "t_time_sk", Types.IntegerType.get()),
                                    required(2, "t_time_id", Types.StringType.get()),
                                    required(3, "t_time", Types.IntegerType.get()),
                                    required(4, "t_hour", Types.IntegerType.get()),
                                    required(5, "t_minute", Types.IntegerType.get()),
                                    required(6, "t_second", Types.IntegerType.get()),
                                    required(7, "t_am_pm", Types.StringType.get()),
                                    required(8, "t_shift", Types.StringType.get()),
                                    required(9, "t_sub_shift", Types.StringType.get()),
                                    required(10, "t_meal_time", Types.StringType.get()))
                            .fields());

    public static final Schema WAREHOUSE =
            new Schema(
                    Types.StructType.of(
                                    required(1, "w_warehouse_sk", Types.IntegerType.get()),
                                    required(2, "w_warehouse_id", Types.StringType.get()),
                                    required(3, "w_warehouse_name", Types.StringType.get()),
                                    required(4, "w_warehouse_sq_ft", Types.IntegerType.get()),
                                    required(5, "w_street_number", Types.StringType.get()),
                                    required(6, "w_street_name", Types.StringType.get()),
                                    required(7, "w_street_type", Types.StringType.get()),
                                    required(8, "w_suite_number", Types.StringType.get()),
                                    required(9, "w_city", Types.StringType.get()),
                                    required(10, "w_county", Types.StringType.get()),
                                    required(11, "w_state", Types.StringType.get()),
                                    required(12, "w_zip", Types.StringType.get()),
                                    required(13, "w_country", Types.StringType.get()),
                                    required(14, "w_gmt_offset", Types.IntegerType.get()))
                            .fields());

    public static final Schema WEB_PAGE =
            new Schema(
                    Types.StructType.of(
                                    required(1, "wp_web_page_sk", Types.IntegerType.get()),
                                    required(2, "wp_web_page_id", Types.StringType.get()),
                                    required(3, "wp_rec_start_date", Types.StringType.get()),
                                    required(4, "wp_rec_end_date", Types.StringType.get()),
                                    required(5, "wp_creation_date_sk", Types.IntegerType.get()),
                                    required(6, "wp_access_date_sk", Types.IntegerType.get()),
                                    required(7, "wp_autogen_flag", Types.StringType.get()),
                                    required(8, "wp_customer_sk", Types.IntegerType.get()),
                                    required(9, "wp_url", Types.StringType.get()),
                                    required(10, "wp_type", Types.StringType.get()),
                                    required(11, "wp_char_count", Types.IntegerType.get()),
                                    required(12, "wp_link_count", Types.IntegerType.get()),
                                    required(13, "wp_image_count", Types.IntegerType.get()),
                                    required(14, "wp_max_ad_count", Types.IntegerType.get()))
                            .fields());

    public static final Schema WEB_RETURNS =
            new Schema(
                    Types.StructType.of(
                                    required(1, "wr_returned_date_sk", Types.IntegerType.get()),
                                    required(2, "wr_returned_time_sk", Types.IntegerType.get()),
                                    required(3, "wr_item_sk", Types.IntegerType.get()),
                                    required(4, "wr_refunded_customer_sk", Types.IntegerType.get()),
                                    required(5, "wr_refunded_cdemo_sk", Types.IntegerType.get()),
                                    required(6, "wr_refunded_hdemo_sk", Types.IntegerType.get()),
                                    required(7, "wr_refunded_addr_sk", Types.IntegerType.get()),
                                    required(8, "wr_returning_customer_sk", Types.IntegerType.get()),
                                    required(9, "wr_returning_cdemo_sk", Types.IntegerType.get()),
                                    required(10, "wr_returning_hdemo_sk", Types.IntegerType.get()),
                                    required(11, "wr_returning_addr_sk", Types.IntegerType.get()),
                                    required(12, "wr_web_page_sk", Types.IntegerType.get()),
                                    required(13, "wr_reason_sk", Types.IntegerType.get()),
                                    required(14, "wr_order_number", Types.IntegerType.get()),
                                    required(15, "wr_return_quantity", Types.IntegerType.get()),
                                    required(16, "wr_return_amt", Types.IntegerType.get()),
                                    required(17, "wr_return_tax", Types.IntegerType.get()),
                                    required(18, "wr_return_amt_inc_tax", Types.IntegerType.get()),
                                    required(19, "wr_fee", Types.IntegerType.get()),
                                    required(20, "wr_return_ship_cost", Types.IntegerType.get()),
                                    required(21, "wr_refunded_cash", Types.IntegerType.get()),
                                    required(22, "wr_reversed_charge", Types.IntegerType.get()),
                                    required(23, "wr_account_credit", Types.IntegerType.get()),
                                    required(24, "wr_net_loss", Types.IntegerType.get()))
                            .fields());

    public static final Schema WEB_SALES =
            new Schema(
                    Types.StructType.of(
                                    required(1, "ws_sold_date_sk", Types.IntegerType.get()),
                                    required(2, "ws_sold_time_sk", Types.IntegerType.get()),
                                    required(3, "ws_ship_date_sk", Types.IntegerType.get()),
                                    required(4, "ws_item_sk", Types.IntegerType.get()),
                                    required(5, "ws_bill_customer_sk", Types.IntegerType.get()),
                                    required(6, "ws_bill_cdemo_sk", Types.IntegerType.get()),
                                    required(7, "ws_bill_hdemo_sk", Types.IntegerType.get()),
                                    required(8, "ws_bill_addr_sk", Types.IntegerType.get()),
                                    required(9, "ws_ship_customer_sk", Types.IntegerType.get()),
                                    required(10, "ws_ship_cdemo_sk", Types.IntegerType.get()),
                                    required(11, "ws_ship_hdemo_sk", Types.IntegerType.get()),
                                    required(12, "ws_ship_addr_sk", Types.IntegerType.get()),
                                    required(13, "ws_web_page_sk", Types.IntegerType.get()),
                                    required(14, "ws_web_site_sk", Types.IntegerType.get()),
                                    required(15, "ws_ship_mode_sk", Types.IntegerType.get()),
                                    required(16, "ws_warehouse_sk", Types.IntegerType.get()),
                                    required(17, "ws_promo_sk", Types.IntegerType.get()),
                                    required(18, "ws_order_number", Types.IntegerType.get()),
                                    required(19, "ws_quantity", Types.IntegerType.get()),
                                    required(20, "ws_wholesale_cost", Types.IntegerType.get()),
                                    required(21, "ws_list_price", Types.IntegerType.get()),
                                    required(22, "ws_sales_price", Types.IntegerType.get()),
                                    required(23, "ws_ext_discount_amt", Types.IntegerType.get()),
                                    required(24, "ws_ext_sales_price", Types.IntegerType.get()),
                                    required(25, "ws_ext_wholesale_cost", Types.IntegerType.get()),
                                    required(26, "ws_ext_list_price", Types.IntegerType.get()),
                                    required(27, "ws_ext_tax", Types.IntegerType.get()),
                                    required(28, "ws_coupon_amt", Types.IntegerType.get()),
                                    required(29, "ws_ext_ship_cost", Types.IntegerType.get()),
                                    required(30, "ws_net_paid", Types.IntegerType.get()),
                                    required(31, "ws_net_paid_inc_tax", Types.IntegerType.get()),
                                    required(32, "ws_net_paid_inc_ship", Types.IntegerType.get()),
                                    required(33, "ws_net_paid_inc_ship_tax", Types.IntegerType.get()),
                                    required(34, "ws_net_profit", Types.IntegerType.get()))
                            .fields());

    public static final Schema WEB_SITE =
            new Schema(
                    Types.StructType.of(
                                    required(1, "web_site_sk", Types.IntegerType.get()),
                                    required(2, "web_site_id", Types.StringType.get()),
                                    required(3, "web_rec_start_date", Types.StringType.get()),
                                    required(4, "web_rec_end_date", Types.StringType.get()),
                                    required(5, "web_name", Types.StringType.get()),
                                    required(6, "web_open_date_sk", Types.IntegerType.get()),
                                    required(7, "web_close_date_sk", Types.IntegerType.get()),
                                    required(8, "web_class", Types.StringType.get()),
                                    required(9, "web_manager", Types.StringType.get()),
                                    required(10, "web_mkt_id", Types.IntegerType.get()),
                                    required(11, "web_mkt_class", Types.StringType.get()),
                                    required(12, "web_mkt_desc", Types.StringType.get()),
                                    required(13, "web_market_manager", Types.StringType.get()),
                                    required(14, "web_company_id", Types.IntegerType.get()),
                                    required(15, "web_company_name", Types.StringType.get()),
                                    required(16, "web_street_number", Types.StringType.get()),
                                    required(17, "web_street_name", Types.StringType.get()),
                                    required(18, "web_street_type", Types.StringType.get()),
                                    required(19, "web_suite_number", Types.StringType.get()),
                                    required(20, "web_city", Types.StringType.get()),
                                    required(21, "web_county", Types.StringType.get()),
                                    required(22, "web_state", Types.StringType.get()),
                                    required(23, "web_zip", Types.StringType.get()),
                                    required(24, "web_country", Types.StringType.get()),
                                    required(25, "web_gmt_offset", Types.IntegerType.get()),
                                    required(26, "web_tax_percentage", Types.IntegerType.get()))
                            .fields());

    public static final List<Schema> SCHEMA_LIST = Lists.newArrayList(CALL_CENTER, CATALOG_PAGE, CATALOG_RETURNS,
            CATALOG_SALES, CUSTOMER_ADDRESS, CUSTOMER_DEMOGRAPHICS, CUSTOMER, DATE_DIM, HOUSEHOLD_DEMOGRAPHICS,
            INCOME_BAND, INVENTORY, ITEM, PROMOTION, REASON, SHIP_MODE, STORE_RETURNS, STORE_SALES, STORE, TIME_DIM,
            WAREHOUSE, WEB_PAGE, WEB_RETURNS, WEB_SALES, WEB_SITE);

    public static final List<String> SCHEMA_NAMES = Lists.newArrayList(
            "call_center", "catalog_page", "catalog_returns", "catalog_sales",
            "customer_address", "customer_demographics", "customer", "date_dim",
            "household_demographics", "income_band", "inventory", "item",
            "promotion", "reason", "ship_mode", "store_returns", "store_sales",
            "store", "time_dim", "warehouse", "web_page", "web_returns",
            "web_sales", "web_site"
    );
}
