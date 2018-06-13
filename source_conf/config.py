#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
域名正则匹配训练配置文件

"""
import os
#from lib import cloud_logger

#cloud_logger.init()
import logging
logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger()

# 模块名称
module_name = "domain_regex_extract"

# 保存文件路径
DATA_PATH = "./data"
WORK_PATH = os.path.join(DATA_PATH, module_name)

# 数据源及结果表
SOURCE_TABLE = "SynTrMaliceLinkBlackLink"
PUBLISH_TABLE = "MaliceLinkDomainRegex"

# 数据标签宏定义
malicious_type_malware = "1"

# 从数据获取恶意链接上限
MAX_TRAIN_URL_COUNT = 30000

# 向量化模块抽取向量维数
ASCII_SIZE = 128

# 聚类模块并行进程数
N_JOBS_CLUSTERING = 8

# k-means 聚类簇大小极限
KMEANS_SIZE_LIMIT = 50000

# 每次分裂子簇的数量
KMEANS_SUB_SPLIT_SIZE = 3

# 长短URL字符数，用于改变编辑聚类系数
SHORT_URL_THRESH = 10
LONG_URL_THRESH = 40

# 编辑距离系数
EDIT_DISTANCE_THRESH_LONG = 0.3
EDIT_DISTANCE_THRESH_SHORT = 0.2

# 打印日志，聚类进度粒度
CLUSTER_REPORT_FQ = 100

# 聚类簇大小过滤
DOMAIN_CLUSTER_SIZE_THRESH = 6

# 域名token频率统计阈值
DOMAIN_TOKEN_FREQUENCY_THRESH = 0.2

# 域名token抽取公共子串，子串最小字符阈值
MAX_SUBSTRING_SIZE_LIMIT = 2

# 域名token采样配置
DOMAIN_TOKEN_SAMPLE_ROUND = 10
DOMAIN_TOKEN_SAMPLE_RATIO = 0.4
DOMAIN_TOKEN_SAMPLE_UPBOUND = 50
DOMAIN_TOKEN_SAMPLE_LOWBOUND = 10

# 大小簇尺寸，用于统计分析聚类结果
SMALL_CLUSTER_SIZE = 2
BIG_CLUSTER_SIZE = 6

# 统计子域名词频时，写入正则表达式的TLD, SLD数量上写
TLD_SLD_COUNT_UPBOUND = 10
