#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-
"""
域名正则表达式提取主程序入口

"""
import os

from file_io import load_urls
from preprocess import UrlPreprocess
from cluster_domain import DomainCluster
from regex import DomainRegexExtract
#from dre_lib.get_malicious_link import GetMaliceLink
#from dre_lib.publish_domain_regex import PublishDomainRegex
from source_conf.config import logger, module_name, WORK_PATH


def domain_regex_extract():
    # 检查路径
    train_urls_file = os.path.join(WORK_PATH, "train_urls.csv")
    cluster_file = os.path.join(WORK_PATH, "cluster_distance.json")
    domain_regex_file = os.path.join(WORK_PATH, "domain_regex.txt")

    if not os.path.isdir(WORK_PATH):
        os.makedirs(WORK_PATH)
        logger.info("mkdir %s" % WORK_PATH)

    # 获取恶意链接
#    get_data_obj = GetMaliceLink()
#    get_data_obj.do_get_malice_link(train_urls_file)
#    malice_link_list = load_urls(train_urls_file)
#    logger.info("%s: main: malice_link get" % (module_name))
    malice_link_list = load_urls("../std_data/mal.csv")

    # 进行预处理
    process_obj = UrlPreprocess()
    malice_domain_list = process_obj.do_url_preprocess(malice_link_list)
    logger.info("%s: main: preprocess complete" % (module_name))

    # 域名聚类
    cluster_obj = DomainCluster()
    cluster_obj.do_make_domain_clustering(malice_domain_list , cluster_file )
    logger.info("%s: main: clustering complete" % (module_name))

    # 正则表达式抽取
    extract_obj = DomainRegexExtract()
    extract_obj.do_domain_regex_extract(cluster_file, domain_regex_file)
    logger.info("%s: main: regex extract complete" % (module_name))

    #上传正则表达式
#    publish_obj = PublishDomainRegex()
#    publish_obj.do_publish_domain_regex(domain_regex_file)
#    logger.info("%s: main: regex publish complete" % (module_name))


if __name__ == "__main__":
    domain_regex_extract()
    
    
