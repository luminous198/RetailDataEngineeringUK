###simple match

with morrissons_data as (
select lower(rsi.itemname) as itemnamelower, * from in_use.retail_store_items rsi
where rsi.storename = 'MORRISSONS'
),
asda_data as (
select lower(rsi.itemname) as itemnamelower,* from in_use.retail_store_items rsi
where rsi.storename = 'ASDA'
),
aldi_data as (
select lower(rsi.itemname) as itemnamelower,* from in_use.retail_store_items rsi
where rsi.storename = 'ALDI'
)

select md.*, ald.price from morrissons_data md
join aldi_data ald on ald.itemnamelower = md.itemnamelower
limit 100

-------------------------------------------------------------------

select * from in_use.retail_store_items limit 10;

select storename, item_category , count(*) from in_use.retail_store_items
where item_category  ilike '%fruit%'
group by storename,item_category
;

truncate table in_use.retail_store_items ;



select storename, itemname,price from in_use.retail_store_items
where item_category  in ('fruit', 'fruit-veg-176738')
and itemname ilike '%apple%';