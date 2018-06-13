#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
mongodb 数据库的配置

"""
import urllib
from source_conf.config import module_name

HOST = "192.168.7.5"
USERNAME = "root"
PASSWORD = 'sangfor_soapa_mongodb_~!@#$%'
ENABLE_MODULES = {module_name}


MONGO_RS_URL = "mongodb://%s:%s@%s" %(USERNAME, urllib.quote_plus(PASSWORD), HOST)

TR_MALICELINK_BLACK_LINK = ["_id", "hash_index", "row_key", "first_found_time", "update_time", "source_type",
                            "url", "is_domain", "danger_level", "malicious_type", "class_type", "mask",
                            "file_md5_128", "add_time", "add_method", "is_deleted", "count"]
TR_DOMAIN_REGEX = ["_id", "regex", "add_time", "update_time", "source_type", "match_count", "false_alarm_count"]

TABLE_COLUMNS = {
    'SynTrMaliceLinkBlackLink': TR_MALICELINK_BLACK_LINK,
    'MaliceLinkDomainRegex': TR_DOMAIN_REGEX
}