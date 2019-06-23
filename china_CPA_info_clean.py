# -*-coding: utf-8 -*-
import logging
import sys
from pymongo import MongoClient
import pymysql
import re
import time



# 连接mongo数据库
def connect_mongo_db():
    try:
        # 建立连接
        mongo_clinet = MongoClient('127.0.0.1', 25000)
        # 连接数据库
        db = mongo_clinet.raw
        # 用户验证
        db.authenticate('raw', 'raw_bigdata963qaz')
        # 连接集合，也就是mongodb数据库表
        collection = db.reg_acc_office_detail_info
        return mongo_clinet, collection
    except Exception as e:
        logging.error(e)

# 连接mysql数据库
def connect_mysql_db():
    try:
        pymysql.install_as_MySQLdb()
        conn = pymysql.connect(host='118.25.117.117', port=13306, database='qixin', user='etl_worker',
                               password='bigdata@2018')
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        logging.error(e)

# 插入数据库表
def insert(data, tablename):
    print(data)
    keys = list(data.keys())
    length = len(data)
    itemsx = ' (' + keys[0]
    for x in range(1, length):
        itemsx += ','
        itemsx += keys[x]
    itemsx +=') '
    valuetypex = '(' + '%s,' * (length-1) + '%s)'
    sqlx = 'INSERT INTO ' + tablename + itemsx + 'VALUES' + valuetypex
    cur.execute(sqlx, tuple(data.values()))
    conn.commit()

# 会计师信息清洗
def cpa_info_clean(data):
    try:
        data['id'] = str(data['_id'])
        del data['_id']     # 剔除mongodb自带的id
        data['office_address'] = data['office_adress']
        del data['office_adress']
        branch_office_num = data['branch_office_num']   # 分所数量
        branch_office_num = re.findall(r'\d+\.?\d*', branch_office_num)[0]
        data['branch_office_num'] = branch_office_num
        partner_num = data['partner_num']    # 合伙人或股东数量
        partner_num = re.findall(r'\d+\.?\d*', partner_num)[0]
        data['partner_num'] = partner_num
        reg_acc_num = data['reg_acc_num']    #  注册会计师人数
        reg_acc_num = re.findall(r'\d+\.?\d*', reg_acc_num)[0]
        data['reg_acc_num'] = reg_acc_num
        employ_num = data['employ_num']     # 从业人员人数
        employ_num = re.findall(r'\d+\.?\d*', employ_num)[0]
        data['employ_num'] = employ_num
        if 'is_get_bus_license' not in data:
            data['is_get_bus_license'] = None
        if 'is_get_bus_license_date' not in data:
            data['is_get_bus_license_date'] = None
        if 'qua_cer_no' not in data:
            data['qua_cer_no'] = None
        del data['join_inter_net']
        del data['overseas_branches']
        del data['punishment_info']
        del data['check_info']
        del data['welfare_act']
        del data['crawl_time']
        data['clean_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 插入数据表
        cur.execute(
            "select accounting_firm_name from CPA_detail_info where accounting_firm_name = '" + data['accounting_firm_name']+"'"
        )
        acc_firm_name = cur.fetchone()
        if acc_firm_name == None:
            insert(data, 'CPA_detail_info')
    except Exception as e:
        logging.error(e)

if __name__ == '__main__':
    # 设置打印日志信息级别
    logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d]-%(levelname)s:%(message)s',
                        level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    # 连接MongoDB数据库
    clinet, coll = connect_mongo_db()
    # 连接mysql数据库
    conn, cur = connect_mysql_db()

    # 取mongodb数据实例
    # 设置 no_cursor_timeout= True, 永不超时，游标连接不会主动关闭，需要手动关闭
    try:
        dict_data = coll.find({}, no_cursor_timeout=True)
        for data in dict_data:
            cpa_info_clean(data)    # 注册会计师数据清洗
    except Exception as e:
        logging.error(e)

    # 数据处理完成，关闭连接
    cur.close()
    conn.close()
    clinet.close()


