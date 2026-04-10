create or alter procedure silver.load_silver AS
begin
    declare @batch_start_time datetime, @batch_end_time datetime,@start_time datetime,@end_time datetime;
   BEGIN TRY
        set @batch_start_time = getdate();
        set @start_time = getdate();
        print'truncate taple silver.crm_cust_info';
        truncate table silver.crm_cust_info
        print'insert taple silver.crm_cust_info';
        insert into silver.crm_cust_info(
    
               cst_id
              ,cst_key
              ,cst_firstname
              ,cst_lastname
              ,cst_marital_status
              ,cst_gndr
              ,cst_create_date
        )
        select
               cst_id
              ,cst_key
              ,trim(cst_firstname) as cst_firstname
              ,trim(cst_lastname) as cst_lastname
              ,case
                 when upper(trim(cst_marital_status)) = 'M' then 'Married'
                 when upper(trim(cst_marital_status)) = 'S' then 'Single'
                 else 'n/a'
               end as cst_marital_status     
              ,case 
                    when upper(trim(cst_gndr)) = 'F' then 'Female'
                    when upper(trim(cst_gndr)) = 'M' then 'Male'
                    else 'n/a'
               end as cst_gndr

              ,cst_create_date
        from(
        select
        *,
        row_number() over(partition by cst_id order by cst_create_date desc) flag_last
        from bronze.crm_cust_info
        where cst_id is not null
        )t where flag_last = 1
        set @end_time = getdate();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        print'====================================';
        print'====================================';

        ------------------------------------------------------------------
        set @start_time = getdate();
        print'truncate taple silver.crm_prd_info';
        truncate table silver.crm_prd_info
        print'insert taple silver.crm_prd_info';

        insert into silver.crm_prd_info
        (

               prd_id
              ,cat_id
              ,prd_key
              ,prd_nm
              ,prd_cost
              ,prd_line
              ,prd_start_dt
              ,prd_end_dt

        )
        SELECT prd_id
              ,replace(substring(prd_key,1,5),'-','_') as cat_id
              ,substring(prd_key,7,len(prd_key))as prd_key

              ,prd_nm
              ,coalesce(prd_cost,0) as prd_cost
              ,case
		            when upper(trim(prd_line)) = 'M' then 'Mountain'
		            when upper(trim(prd_line)) = 'R' then 'Road'
		            when upper(trim(prd_line)) = 'S' then 'Other Sales'
		            when upper(trim(prd_line)) = 'T' then 'Touring'
		            else 'n/a'
               end as prd_line
              ,CASt(prd_start_dt as date) as prd_start_dt
              ,cast(LEAD( prd_start_dt) OVER(PARTITION BY PRD_KEY ORDER BY PRD_START_DT) - 1 as date) AS prd_end_dt

        FROM bronze.crm_prd_info

        set @end_time = getdate();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        print'====================================';
        print'====================================';
        -----------------------------------
        set @start_time = getdate();
        print'truncate taple silver.crm_sales_details';
        truncate table silver.crm_sales_details
        print'insert taple silver.crm_sales_details';
        insert into silver.crm_sales_details(

               [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,[sls_order_dt]
              ,[sls_ship_dt]
              ,[sls_due_dt]
              ,[sls_sales]
              ,[sls_quantity]
              ,[sls_price]



        )
        select 
               sls_ord_num
              ,sls_prd_key
              ,sls_cust_id
              ,    
                  case
                      when sls_order_dt <=0 or len(sls_order_dt) != 8 then null
                      else cast(cast(sls_order_dt as varchar) as date)
                  end as  sls_order_dt 
              ,
      
                  case
                      when sls_ship_dt <=0 or len(sls_ship_dt) != 8 then null
                      else cast(cast(sls_ship_dt as varchar) as date)
                  end as  sls_ship_dt 

              ,   case
                      when sls_due_dt <=0 or len(sls_due_dt) != 8 then null
                      else cast(cast(sls_due_dt as varchar) as date)
                  end as  sls_due_dt 



              ,       case
                            when sls_sales is null or sls_sales<=0 or sls_sales != abs(sls_price) * sls_quantity
                             then abs(sls_price) * sls_quantity
                            else sls_sales
                      end as sls_sales
              ,sls_quantity
              ,case
                  when sls_price is null or sls_price <= 0 
                   then sls_sales/ nullif(sls_quantity,0)
                  else sls_price
               end as sls_price
        from bronze.crm_sales_details


        set @end_time = getdate();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        print'====================================';
        print'====================================';
        ------------------------------


        set @start_time=getdate()
        print'truncate taple silver.erp_cust_az12';
        truncate table silver.erp_cust_az12
        print'insert taple silver.erp_cust_az12';
        insert into silver.erp_cust_az12(

        cid,
        bdate,
        gen

        )
        SELECT  
              case 
                    when cid like 'NAS%' then substring(cid,4,len(cid))
                    else cid
              end as cid,
              case
                    when BDATE> getdate() then null
                    else bdate
              end as bdate,
              case 
	            when gen is null or gen = '' then 'n/a'
	            when upper(trim(gen)) = 'F' then 'Female'
	            when upper(trim(gen)) = 'M' then 'Male'
	          else gen
        end gen
        FROM bronze.erp_CUST_AZ12
        set @end_time = getdate();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print'====================================';
        print'====================================';

        -------------------------------
        set @start_time = getdate();
        print'truncate taple silver.erp_loc_a101';
        truncate table silver.erp_loc_a101
        print'insert taple silver.erp_loc_a101';
        insert into silver.erp_loc_a101(cid,cntry)
        SELECT 
              replace(CID,'-','') as cid,
              case
                   when upper(trim(CNTRY)) in('DE','GERMANY') then 'Germany'
                   when upper(trim(CNTRY)) in ('US','USA','UNIED STATES') THEN 'United States'
                   when upper(trim(CNTRY)) IS NULL OR upper(trim(CNTRY))='' THEN 'n/a'
                   else cntry
              end cntry
        FROM bronze.erp_LOC_A101
        set @end_time = getdate();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print'====================================';
        print'====================================';
        -------------------------------


        set @start_time = getdate();
        print'truncate taple silver.erp_px_cat_g1v2';
        truncate table silver.erp_px_cat_g1v2
        print'insert taple silver.erp_px_cat_g1v2';
        insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
        SELECT 
              ID,
              CAT,
              SUBCAT,
              MAINTENANCE
        FROM bronze.erp_PX_CAT_G1V2
        set @end_time = getdate();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print'========================================='




        set @batch_end_time = getdate();
        print'=======================';
        print'Loading silver Layer is Completed';
        print'total load duration:'+ cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar) + 'seconds';
        print'=======================';



   END TRY
       BEGIN CATCH
       PRINT'=====================';
       PRINT'An error occurred while loading the Silver layer.';
       PRINT'ERRORE MESSAGE' + error_message() ;
       PRINT'ERRORE MESSAGE' + cast(error_number()as nvarchar) ;
       PRINT'ERRORE MESSAGE' + cast(error_state()as nvarchar) ;
       PRINT'=====================';
   END CATCH
end


