#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-
"""
发布训练完成的正则表达式

"""
import sys
import hashlib
import datetime

from file_io import load_regex_list
from mongodb_db import init_mongodb_db
from source_conf.config import logger, module_name, PUBLISH_TABLE, malicious_type_malware


class PublishDomainRegex(object):
    """
    发布恶意链接
    """
    def __init__(self):
        self.table = None

    def std_md5(self, data):
        """
         获取标准MD5
        :param data: 要进行MD5运算的数据，字符串类型
        :return: data的md5值
        """
        m = hashlib.md5(data)
        return m.hexdigest()

    def connect_db(self):
        """
        连接数据库
        :return:
        """
        try:
            self.table = init_mongodb_db(PUBLISH_TABLE, module_name, "_id")
        except Exception as err:
            raise ValueError("mongodb connection error %s" % (str(err)))

    def do_publish_domain_regex(self, input_path):
        """
        对数据库中的正则表达式进行跟新
        :param input_path: 正则表达式文件路径
        :return:
        """
        regex_list = load_regex_list(input_path)
        try:
            self.connect_db()
        except Exception as err:
            logger.error("%s: publish_domain_regex: do_publish_domain_regex: %s" % (module_name, err))
            sys.exit(1)

        for regex in regex_list:
            data_dict = dict()
            data_dict["_id"] = self.std_md5(regex)
            if not self.table.get(data_dict):
                data_dict["regex"] = regex
                data_dict["add_time"] = str(datetime.datetime.now())
                data_dict["update_time"] = data_dict["add_time"]
                data_dict["source_type"] = malicious_type_malware
                response = self.table.add_data(data_dict)
            else:
                update_dict = dict()
                update_dict["update_time"] = str(datetime.datetime.now())
                response = self.table.update_data(data_dict, update_dict, False)

            if not response:
                logger.error("%s: publish_domain_regex: do_publish_domain_regex: \
                mongodb write error when try to write data %s" % (module_name, str(data_dict)))

        logger.info("domain regex data has been update")


if __name__ == "__main__":
    publish_obj = PublishDomainRegex()
    publish_obj.do_publish_domain_regex("/home/soapa_cloud/daemon-task/url_source/domain_regex_extract/test.txt")
