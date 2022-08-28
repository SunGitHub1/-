DROP TABLE if EXISTS yp_dwd.fact_shop_order;
CREATE TABLE yp_dwd.fact_shop_order
(
    id               string COMMENT '根据一定规则生成的订单编号',
    order_num        string COMMENT '订单序号',
    buyer_id         string COMMENT '买家的userId',
    store_id         string COMMENT '店铺的id',
    order_from       string COMMENT '此字段可以转换 1.安卓\; 2.ios\; 3.小程序H5 \; 4.PC',
    order_state      int COMMENT '订单状态:1.已下单\; 2.已付款, 3. 已确认 \;4.配送\; 5.已完成\; 6.退款\;7.已取消',
    create_date      string COMMENT '下单时间',
    finnshed_time    timestamp COMMENT '订单完成时间,当配送员点击确认送达时,进行更新订单完成时间,后期需要根据订单完成时间,进行自动收货以及自动评价',
    is_settlement    tinyint COMMENT '是否结算\;0.待结算订单\; 1.已结算订单\;',
    is_delete        tinyint COMMENT '订单评价的状态:0.未删除\;  1.已删除\;(默认0)',
    evaluation_state tinyint COMMENT '订单评价的状态:0.未评价\;  1.已评价\;(默认0)',
    way              string COMMENT '取货方式:SELF自提\;SHOP店铺负责配送',
    is_stock_up      int COMMENT '是否需要备货 0：不需要    1：需要    2:平台确认备货  3:已完成备货 4平台已经将货物送至店铺 ',
    create_user      string,
    create_time      string,
    update_user      string,
    update_time      string,
    is_valid         tinyint COMMENT '是否有效  0: false\; 1: true\;   订单是否有效的标志',
    end_date         string COMMENT '拉链结束日期'
)
    COMMENT '订单表'
    partitioned by (start_date string) --拉链起始时间 也是表分区字段
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'SNAPPY');
INSERT overwrite TABLE yp_dwd.fact_shop_order PARTITION (start_date)
SELECT id,
       order_num,
       buyer_id,
       store_id,
       case order_from
           when 1
               then 'android'
           when 2
               then 'ios'
           when 3
               then 'miniapp'
           when 4
               then 'pcweb'
           else 'other'
           end
           as       order_from,
       order_state,
       create_date,
       finnshed_time,
       is_settlement,
       is_delete,
       evaluation_state,
       way,
       is_stock_up,
       create_user,
       create_time,
       update_user,
       update_time,
       is_valid,
       '9999-99-99' end_date,
       dt  as       start_date
FROM yp_ods.t_shop_order;
DROP TABLE if EXISTS yp_dwd.fact_shop_order_tmp;
CREATE TABLE yp_dwd.fact_shop_order_tmp
(
    id               string COMMENT '根据一定规则生成的订单编号',
    order_num        string COMMENT '订单序号',
    buyer_id         string COMMENT '买家的userId',
    store_id         string COMMENT '店铺的id',
    order_from       string COMMENT '此字段可以转换 1.安卓\; 2.ios\; 3.小程序H5 \; 4.PC',
    order_state      int COMMENT '订单状态:1.已下单\; 2.已付款, 3. 已确认 \;4.配送\; 5.已完成\; 6.退款\;7.已取消',
    create_date      string COMMENT '下单时间',
    finnshed_time    timestamp COMMENT '订单完成时间,当配送员点击确认送达时,进行更新订单完成时间,后期需要根据订单完成时间,进行自动收货以及自动评价',
    is_settlement    tinyint COMMENT '是否结算\;0.待结算订单\; 1.已结算订单\;',
    is_delete        tinyint COMMENT '订单评价的状态:0.未删除\;  1.已删除\;(默认0)',
    evaluation_state tinyint COMMENT '订单评价的状态:0.未评价\;  1.已评价\;(默认0)',
    way              string COMMENT '取货方式:SELF自提\;SHOP店铺负责配送',
    is_stock_up      int COMMENT '是否需要备货 0：不需要    1：需要    2:平台确认备货  3:已完成备货 4平台已经将货物送至店铺 ',
    create_user      string,
    create_time      string,
    update_user      string,
    update_time      string,
    is_valid         tinyint COMMENT '是否有效  0: false\; 1: true\;   订单是否有效的标志',
    end_date         string COMMENT '拉链结束日期'
)
    COMMENT '订单表'
    partitioned by (start_date string)
    row format delimited fields terminated by '\t'
    stored as orc
    tblproperties ('orc.compress' = 'SNAPPY');
insert overwrite table yp_dwd.fact_shop_order_tmp partition (start_date)
select *
from (
         --1、ods表的新分区数据(有新增和更新的数据)
         select id,
                order_num,
                buyer_id,
                store_id,
                case order_from
                    when 1
                        then 'android'
                    when 2
                        then 'ios'
                    when 3
                        then 'miniapp'
                    when 4
                        then 'pcweb'
                    else 'other'
                    end
                             as order_from,
                order_state,
                create_date,
                finnshed_time,
                is_settlement,
                is_delete,
                evaluation_state,
                way,
                is_stock_up,
                create_user,
                create_time,
                update_user,
                update_time,
                is_valid,
                '9999-99-99'    end_date,
                '2021-11-30' as start_date
         from yp_ods.t_shop_order
         where dt = '2021-11-30'

         union all

         -- 2、历史拉链表数据，并根据up_id判断更新end_time有效期
         select fso.id,
                fso.order_num,
                fso.buyer_id,
                fso.store_id,
                fso.order_from,
                fso.order_state,
                fso.create_date,
                fso.finnshed_time,
                fso.is_settlement,
                fso.is_delete,
                fso.evaluation_state,
                fso.way,
                fso.is_stock_up,
                fso.create_user,
                fso.create_time,
                fso.update_user,
                fso.update_time,
                fso.is_valid,
                --3、更新end_time：如果没有匹配到变更数据，或者当前已经是无效的历史数据，则保留原始end_time过期时间；否则变更end_time时间为前天（昨天之前有效）
                if(tso.id is null or fso.end_date < '9999-99-99', fso.end_date, date_add(tso.dt, -1)) end_time,
                fso.start_date
         from yp_dwd.fact_shop_order fso
                  left join (select * from yp_ods.t_shop_order where dt = '2021-11-30') tso
                            on fso.id = tso.id
     ) his
order by his.id, start_date;
select *
from yp_dwd.fact_shop_order_tmp
where id = 'dd1910223851672f32';