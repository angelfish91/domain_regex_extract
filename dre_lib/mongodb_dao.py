#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
mongodb中接口封装
包括：
添加，删除，更新，单结果查询，多结果查询，游标查询
"""

import logging
from pymongo.errors import ServerSelectionTimeoutError

DUP_ERROR = "-1"


class MongoDisconncect(Exception):
    pass


class MongoDB(object):
    """
    数据库数据集合类
    """
    def __init__(self, db, collection, columns_list, row_key):
        self.collection = db[collection]
        self.collection_name = collection
        self.cols = columns_list
        self.end_scan = True
        self.skip_count = 0
        self.row_key = row_key
        pass

    # 添加数据
    def add_data(self, field_value_dict):
        """
        添加数据，插入相应的记录
        :param field_value_dict: 字典形式，待添加的数据
        :return:None
        """
        if not (isinstance(field_value_dict, dict)):
            print "para is not dict"
            raise Exception

        for column in field_value_dict:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception
        try:
            self.collection.insert_one(document=field_value_dict)
        # except DuplicateKeyError:
        #    return DUP_ERROR
        except ServerSelectionTimeoutError:
            return False
        return True

    def delete(self, condition):
        """
        删除数据库中的指定数据
        :param condition: 要删除的指定数据，如{"row_key": "1.1.1.1"}
        :return:None
        """
        if not (isinstance(condition, dict)):
            print "para is not dict"
            raise Exception

        for column in condition:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception

        try:
            self.collection.delete_one(filter=condition)
        except Exception as err:
            logging.error(
                'can not delete %s with error %s' %
                (str(condition), str(err)))
            raise RuntimeError('can not del data from mongodb')

    def write(self, field_value_dict):
        """
        更新记录，若不存在，则插入新的一行
        :param field_value_dict: 字典，要更新的域，如{"type": "1"}
        :return:True or False
        """
        if not (isinstance(field_value_dict, dict)):
            print "para is not dict"
            raise Exception

        if self.row_key not in field_value_dict:
            print "row_key is not in field_value_dict"
            raise Exception

        for column in field_value_dict:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception

        value = field_value_dict[self.row_key]
        update_filter = {self.row_key: value}
        try:
            self.collection.update_one(
                filter=update_filter, update={
                    "$set": field_value_dict}, upsert=True)

        except ServerSelectionTimeoutError:
            return False
        return True

    def update_data(self, update_filter, field_value_dict, up_sert):
        """
        更新记录，若不存在，则可根据参数up_sert决定是否插入新的一行
        :param update_filter:字典，指定的主键，如{"row_key": "1.1.1.1"}
        :param field_value_dict: 字典，更新的域，如{"type": "1"}
        :param up_sert: 值为True or False，为True表示不存在时，则插入一条数据
        :return:True or False
        """
        if not ((isinstance(update_filter, dict)) and (isinstance(field_value_dict, dict))):
            print "para is not dict"
            raise Exception

        for column in update_filter:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception

        for column in field_value_dict:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception

        try:
            self.collection.update_one(filter=update_filter,
                                       update={"$set": field_value_dict},
                                       upsert=up_sert)

        except ServerSelectionTimeoutError:
            return False
        return True

    def update_many(self, update_filter, field_value_dict, up_sert):
        """
        批量更新MongoDBdb中数据，符合某一条件的数据
        根据参数up_sert决定是否插入新的一行
        :param update_filter: 字典，指定的主键，如{"is_preloaded": True}
        :param field_value_dict: 字典，更新的域，如{"is_preloaded": False}
        :param up_sert: 值为True or False，为True表示不存在时，则插入一条数据
        :return: 错误返回异常
        """
        if not ((isinstance(update_filter, dict)) and (isinstance(field_value_dict, dict))):
            raise ValueError("update many fields in mongodb params is not a dict")

        for column in update_filter:
            if column not in self.cols:
                raise ValueError("filter column %s not in mongodb setting columns" %
                                 column)

        for column in field_value_dict:
            if column not in self.cols:
                raise ValueError("update column %s not in mongodb setting columns" %
                                 column)
        # 不用捕获异常，直接让上层进行捕获就可以了
        self.collection.update_many(filter=update_filter,
                                    update={"$set": field_value_dict},
                                    upsert=up_sert)

    def get(self, spec_fields_dict):
        """
        查询指定一行数据
        :param spec_fields_dict: 字典，要查询的域字典
        :return: ( {"_id":"id", "exist_field1":"value"} )
        """
        if not (isinstance(spec_fields_dict, dict)):
            print "para is not dict"
            raise Exception

        for column in spec_fields_dict:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception

        return self.collection.find_one(filter=spec_fields_dict)

    def get_data_list(self, filter_dict, sort_field, limit=1000, descending_order=True):
        """
        获取列表，只获取关注的字段，且根据特定字段过滤和排序（对于指定的键无依赖）
        :param filter_dict: 用于过滤的字典
        :param sort_field: 用于排序的字段
        :param limit: 要取记录的条数
        :param descending_order: 是否降序，True为降序
        :return: ( [{"_id":"id", "exist_field1":"value"},], total_count_of_index )
        """
        if not (isinstance(filter_dict, dict)):
            print "para is not dict"
            raise Exception

        for column in filter_dict:
            if column not in self.cols:
                print "column %s not in columns" % column
                raise Exception

        if sort_field not in self.cols:
            print "sort_field %s not in columns" % sort_field
            raise Exception
        # 设置过滤选项

        total_count = self.collection.find(filter=filter_dict).count()

        sort_order = -1 if descending_order else 1
        index_cursor = self.collection.find(filter=filter_dict,
                                            sort=[(sort_field, sort_order)],
                                            limit=limit)
        if index_cursor.count() == 0:
            return [], 0
        try:
            # 测试数据库是否连接
            index_cursor[0]
        except ServerSelectionTimeoutError:
            raise MongoDisconncect
        else:
            result_list = [index for index in index_cursor]
            return result_list, total_count

    def scan(self, count):
        """
        游标查询，一次性获取count条记录
        :param count: 一次性获取count条记录
        :return: ( [{"_id":"id", "exist_field1":"value"},], total_count_of_index )
        """
        # 设置过滤选项
        skip_count = self.skip_count
        total_count = self.collection.find().count()
        if skip_count >= total_count:
            print skip_count, total_count
            return [], 0

        index_cursor = self.collection.find(skip=skip_count, limit=count)
        result_count = index_cursor.count()
        print result_count

        if result_count < count:
            self.end_scan = True
            self.skip_count = 0
            total_count = 0
        else:
            self.skip_count = skip_count + count

        if index_cursor.count() == 0:
            return [], 0
        try:
            # 测试数据库是否连接
            index_cursor[0]
        except ServerSelectionTimeoutError:
            raise MongoDisconncect
        else:
            result_list = [index for index in index_cursor]
            return result_list, total_count

    def scan_by_condition(self, filter_dict, count):
        """
        游标查询，一次性获取count条记录
        :param filter_dict: 字典，要过滤的域字典
        :param count: 一次性获取count条记录
        :return: ( [{"_id":"id", "exist_field1":"value"},], total_count_of_index )
        """
        # 设置过滤选项
        skip_count = self.skip_count
        total_count = self.collection.find().count()
        if skip_count >= total_count:
            self.skip_count = 0
            print skip_count, total_count
            return [], 0

        index_cursor = self.collection.find(
            filter=filter_dict, skip=skip_count, limit=count)
        # result_count = index_cursor.count()

        if (count + skip_count) >= total_count:
            self.end_scan = True

        self.skip_count = skip_count + count

        print skip_count, count
        print index_cursor.count()
        result_list = [index for index in index_cursor]
        return result_list, total_count

    def scan_init(self):
        self.end_scan = False
        self.skip_count = 0

    def close(self):
        """
        关闭接口，释放资源
        :return: True
        """
        pass
        return True

    def count_by_condition(self, filter_dict=None):
        if filter_dict is None:
            total_count = self.collection.find().count()
        else:
            total_count = self.collection.find(filter=filter_dict).count()
        return total_count
