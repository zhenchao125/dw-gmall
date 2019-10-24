#!/bin/bash



if [[ -n $2 ]]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

#---------------------

sqoop=/opt/module/sqoop-1.4.6/bin/sqoop

function import_data() {
    table=$1
    sql=$2

    ${sqoop} import \
    --connect jdbc:mysql://hadoop102:3306/gmall \
    --username root \
    --password aaaaaa \
    --query "$sql and \$CONDITIONS" \
    --num-mappers 2 \
    --split-by id \
    --target-dir /origin_data/gmall/db/${table}/${do_date} \
    --delete-target-dir \
    --fields-terminated-by \t
}

# 导入订单表
function import_order_info() {

    import_data order_info "select
                                id,
                                total_amount,
                                order_status,
                                user_id,
                                payment_way,
                                out_trade_no,
                                create_time,
                                operate_time
                             from order_info
                             where (date_format(create_time, '%y-%m-%d')='$do_date' or date_format(create_time, '%y-%m-%d')='$do_date')"
}

# order_detail 表
function import_order_detail() {

    import_data order_detail "select
                                od.id,
                                order_id,
                                user_id,
                                sku_id,
                                sku_name,
                                order_price,
                                sku_num,
                                oi.create_time
                             from order_detail od join order_info oi \
                             on od.order_id=oi.id
                             where date_format(oi.create_time, '%y-%m-%d')='$do_date'"
}

# user_info
function import_user_info() {

    import_data user_info "select
                                id,
                                name,
                                birthday,
                                gender,
                                email,
                                user_level,
                                create_time
                           from user_info
                           where 1=1"
}

import_sku_info(){
  import_data "sku_info" "select
id, spu_id, price, sku_name, sku_desc, weight, tm_id,
category3_id, create_time
  from sku_info where 1=1"
}


import_base_category1(){
  import_data "base_category1" "select
id, name from base_category1 where 1=1"
}

import_base_category2(){
  import_data "base_category2" "select
id, name, category1_id from base_category2 where 1=1"
}

import_base_category3(){
  import_data "base_category3" "select id, name, category2_id from base_category3 where 1=1"
}
import_payment_info(){
  import_data "payment_info"   "select
    id,
    out_trade_no,
    order_id,
    user_id,
    alipay_trade_no,
    total_amount,
    subject,
    payment_type,
    payment_time
  from payment_info
  where DATE_FORMAT(payment_time,'%Y-%m-%d')='$do_date'"
}

table=$1
case ${table} in
    order_info)
        import_order_info
    ;;

    order_detail)
        import_order_detail
    ;;

    user_info)
        import_user_info
    ;;

    sku_info)
        import_sku_info
    ;;

    base_category1)
        import_base_category1
    ;;

    base_category2)
        import_base_category2
    ;;

    base_category3)
        import_base_category3
    ;;

    payment_info)
        import_payment_info
    ;;
    all)
        import_order_info
        import_order_detail
        import_user_info
        import_sku_info
        import_base_category1
        import_base_category2
        import_base_category3
        import_payment_info
    ;;
esac
