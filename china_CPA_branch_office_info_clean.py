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
        collection = db.reg_acc_branch_office_info
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

# 分所信息清洗
def cpa_branch_office_info_clean(data):
    try:
        data['id'] = str(data['_id'])
        del data['_id']     # 剔除mongodb自带的id
        data['postal_code'] = data['branch_office_postal_code']
        del data['branch_office_postal_code']
        data['email'] = data['branch_office_email']
        del data['branch_office_email']
        data['fax'] = data['branch_office_fax']
        del data['branch_office_fax']
        data['phone'] = data['branch_office_phone']
        del data['branch_office_phone']
        reg_acc_sum = data['reg_acc_sum']   # 注册会计师总数
        reg_acc_sum = re.findall(r'\d+\.?\d*', reg_acc_sum)[0]
        data['reg_acc_sum'] = reg_acc_sum
        emp_sum = data['emp_sum']    # 从业人员总数
        emp_sum = re.findall(r'\d+\.?\d*', emp_sum)[0]
        data['emp_sum'] = emp_sum
        del data['punish_info']
        del data['crawl_time']
        data['clean_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 插入数据表
        cur.execute(
            "select branch_office_name from CPA_branch_office_info where branch_office_name = '" + data['branch_office_name']+"'"
        )
        branch_office_name = cur.fetchone()
        if branch_office_name == None:
            insert(data, 'CPA_branch_office_info')
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
            cpa_branch_office_info_clean(data)    # 注册会计师数据清洗
    except Exception as e:
        logging.error(e)

    # 数据处理完成，关闭连接
    cur.close()
    conn.close()
    clinet.close()


