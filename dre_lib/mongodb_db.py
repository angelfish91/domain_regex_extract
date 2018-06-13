#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
mongodb中接口封装
包括：
添加，删除，更新，单结果查询，多结果查询，游标查询

"""
from pymongo import MongoClient
from mongodb_dao import MongoDB
from mongodb_setting import MONGO_RS_URL, TABLE_COLUMNS, ENABLE_MODULES

# # 断线会自动重连，若执行操作时处于断线状态，会抛出ServerSelectionTimeoutError异常
# # 正式地址为192.168.3.15
# user = "root"
# password = quote_plus("sangfor_soapa_mongodb_~!@#$%")
# #socket_path = "192.168.3.15"
# socket_path = "mongodb-primary-1.novalocal"
# uri = "mongodb://" + user + ":" + password + "@" + socket_path
# mongo_client = MongoClient(uri, port=27017, serverSelectionTimeoutMS=15000, connect=False)
# mongo_scproxydb = mongo_client.secproxydb
mongo_client = MongoClient(MONGO_RS_URL, connect=False)
mongo_scproxydb = mongo_client.secproxydb


def init_mongodb_db(collection, module_name, row_key='_id'):
    """
    初始化数据库对象
    :return: 数据库对象，如果初始化失败，返回None
    """
    if module_name not in ENABLE_MODULES:
        raise ValueError('module is not arrow to use this db')

    columns_list = TABLE_COLUMNS[collection]
    if row_key not in columns_list:
        raise ValueError('row_key is not in columns')

    table = MongoDB(mongo_scproxydb, collection, columns_list, row_key)
    return table


if __name__ == '__main__':
    try:
        db = init_mongodb_db(
            "MaliceLinkDomainRegex", "domain_regex_extract", "_id")
        print 'mongodb_db is ready'
    except Exception as err:
        print 'mongodb_db is not ready'
        print err
