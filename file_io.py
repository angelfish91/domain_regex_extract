#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
IO模块
数据加载与保存
"""
import os
import sys
import json

import pandas as pd
from source_conf.config import logger, module_name

SYSTME_ERROR_CODE = 1

def dump_urls(urls, file_path):
    """
    保存URL数据，csv格式
    :param urls: 待保存URL列表
    :param file_path: 保存路径
    :return:
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
    try:
        df = pd.DataFrame({"url": urls})
        df.to_csv(file_path, index=False)
        logger.info("%s: urls has been dump\t%s" % (module_name , file_path))
    except Exception as err:
        logger.error("%s: urls dump error %s %s" % (module_name, file_path, str(err)))
        sys.exit(SYSTME_ERROR_CODE)


def load_urls(file_path):
    """
    读取URL数据，csv格式
    :param file_path: 读取路径
    :return:
    """
    try:
        df = pd.read_csv(file_path)
        urls = list(df.url)
        logger.info("%s: urls has been load\t%s" % (module_name, file_path))
        return urls
    except Exception as err:
        logger.error("%s: urls load error %s %s" % (module_name, file_path, str(err)))
        sys.exit(SYSTME_ERROR_CODE)


def dump_cluster_data(file_path, cluster_list):
    """
    保存聚类结果数据,json格式
    :param file_path: 保存路径
    :param cluster_list: 聚类结果，列表
    :return:
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
    try:
        with open(file_path, "w") as fd:
            for index, cluster in enumerate(cluster_list):
                fd.write(json.dumps({index: cluster}) + '\n')
        logger.info("%s: cluster data has been dump\t%s" % (module_name, file_path))
    except Exception as err:
        logger.error("%s: cluster data dump error %s %s" % (module_name, file_path, str(err)))
        sys.exit(SYSTME_ERROR_CODE)


def load_cluster_data(file_path):
    """
    读取聚类结果数据,json格式
    :param file_path: 读取路径
    :return: 聚类结果，列表
    """
    cluster = []
    try:
        with open(file_path, 'r') as fd:
            for line in fd:
                cluster.append(json.loads(line.strip()))
        logger.info("%s: cluster data has been load\t%s" % (module_name, file_path))
    except Exception as err:
        logger.error("%s: cluster data load error %s %s" % (module_name, file_path, str(err)))
        sys.exit(SYSTME_ERROR_CODE)
    cluster = [_[_.keys()[0]] for _ in cluster]
    return cluster


def load_regex_list(file_path):
    """
    读取正则表达式列表
    :param file_path: 读取路径
    :return: 正则表达式列表
    """
    regex = []
    try:
        with open(file_path, 'r') as fd:
            for line in fd:
                regex.append(line.strip())
        logger.info("%s: regex data has been load\t%s" % (module_name, file_path))
    except Exception as err:
        logger.error("%s: regex load error %s %s" % (module_name, file_path, str(err)))
        sys.exit(SYSTME_ERROR_CODE)
    return regex


def dump_regex_list(regex_list, file_path):
    """
    保存正则表达式列表
    :param file_path:  保存路径
    :param regex_list: 正则表达式列表
    :return:
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
    try:
        with open(file_path, 'w') as fd:
            for regex in regex_list:
                fd.write(regex + '\n')
        logger.info("%s: regex data has been dump\t%s" % (module_name, file_path))
    except Exception as err:
        logger.error("%s: regex dump error %s %s" % (module_name, file_path, str(err)))
        sys.exit(SYSTME_ERROR_CODE)
