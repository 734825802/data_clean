# -*- coding: utf-8 -*-
import sys
from pymongo import MongoClient
import pymysql
import json
import uuid
from aip import AipNlp   #导入百度自然语言处理的Python SDK客户端
import logging



"""
插入mysql(TIDB)数据库
datadic : 要插入的数据字典
tablename : 表名
"""
def insert(datadic,tablename) :
    keys = list(datadic.keys())
    length = len(datadic)
    itemsx = ' (' + keys[0]
    for x in range(1,length):
        itemsx += ','
        itemsx += keys[x]
    itemsx += ') '
    valuetypex = '(' + '%s,' * (length-1) + '%s)'
    sqlx = 'INSERT INTO ' + tablename + itemsx + 'VALUES ' + valuetypex
    cur.execute (sqlx,tuple(datadic.values()))
    conn.commit()





"""
数据清洗函数
dict  数据字典
"""
def legaldata_clean(data):
    try:
        illegal_data = {}
        print(data['url'])
        if data['state'] == '个人':

            # 姓名
            illegal_data['name'] = data['name']

            # 链接地址
            illegal_data['url'] = data['url']

            # 证件名称及号码
            # illegal_data['idCard'] = data['idCard']

            # 主要违法事实
            illegal_data['illegal_facts'] = data['IllegalFacts']

            illegal_data['state'] = data['state']

            # 负有直接责任的中介机构信息及其从业人员信息
            illegal_data['gency_info'] = data['gencyInfo']

            # 案件性质
            illegal_data['cases_nature'] = data['casesNature']

            #负有直接责任的财务负责人姓名、性别、证件名称及号码
            illegal_data['direct'] = ''

            # 纳税人识别号
            illegal_data['id_tax'] = ''

            # 组织机构代码
            illegal_data['org_code'] = ''

            # 法定代表人或者负责人姓名、性别、证件名称及号码
            illegal_data['legal_person'] = ''

            # 注册地址
            illegal_data['reg_address'] = ''

            insert(illegal_data,'legal_illegal')

        else:
            # (纳税人名称)
            illegal_data['name'] = data['companyName']

            # 纳税人识别号
            illegal_data['id_tax'] = data['idTax']

            # 组织机构代码
            illegal_data['org_code'] = data['orgCode']

            #详情链接
            illegal_data['url'] = data['url']

            # 负有直接责任的财务负责人姓名、性别、证件名称及号码
            illegal_data['direct'] = data['direct'][:data['direct'].find(',')]


            #法定代表人或者负责人姓名、性别、证件名称及号码
            illegal_data['legal_person'] = data['legalPerson'][:data['legalPerson'].find(',')]

            #主要违法事实
            illegal_data['illegal_facts'] = data['IllegalFacts']

            illegal_data['state'] = data['state']

            #负有直接责任的中介机构信息及其从业人员信息
            illegal_data['gency_info'] = data['gencyInfo']

            #注册地址
            illegal_data['reg_address'] = data['regAddress']

            #案件性质
            illegal_data['cases_nature'] = data['casesNature']
            #插入数据库
            insert(illegal_data, 'legal_illegal')
    except Exception as e :
        logging.error(e)


if __name__ =="__main__":

    # 设置打印日志信息级别
    logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                        level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    """
    APP_ID
    API_KEY
    SECRET_KEY   
    """
    APP_ID = '11116414'
    API_KEY = '27tRThGvjvPlvGICu0GtliLH'
    SECRET_KEY = 'nisTIAdMVBp9prlNwHORTlmOG7XZr8DY'
    client = AipNlp(APP_ID, API_KEY, SECRET_KEY)

    """
    建立MongoDB数据库连接
    """
    # mongo_client = MongoClient('192.168.111.214', 27017)
    # db = mongo_client.raw
    # db.authenticate("raw", "raw_bigdata963qaz")
    # collection = db.legeldata_2017_10_12

    mongo_client = MongoClient('192.168.111.210', 27017)
    db = mongo_client.legeldata
    collection = db.legaldata_2018_3_11
    """
    建立Mysql(TIDB)数据库连接
    """
    pymysql.install_as_MySQLdb()
    conn = pymysql.connect(host='118.25.117.117', port=13306, database='qixin', user='etl_worker',
                           password='bigdata@2018')
    cur = conn.cursor()
    # engine =  create_engine('mysql+mysqldb://wx_crawler:bigdata_wx_crawler@2018@118.25.117.117:13306/wx_article?charset=utf8')

    """
    命令传入参数
    """
   # num = int(sys.argv[1])

    """
    取数据实例
    设置  no_cursor_timeout = True，永不超时，游标连接不会主动关闭，需要手动关闭
    """
    dict_data = collection.find({}, {'_id': 0}, no_cursor_timeout=True)
    for data in dict_data:
        try:
            legaldata_clean(data)
        except Exception as e:
            logging.error(e)

    cur.close()
    conn.close()







