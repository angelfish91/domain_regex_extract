#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-
"""
从恶意链接库中获取链接

"""
import sys

from file_io import dump_urls
from mongodb_db import init_mongodb_db
from source_conf.config import logger, module_name, MAX_TRAIN_URL_COUNT, SOURCE_TABLE, malicious_type_malware


class GetMaliceLink(object):
    """
    从mongdb中获取恶意链接
    """
    def __init__(self):
        self.table = None
        self.data = None

    def connect_db(self):
        try:
            self.table = init_mongodb_db(SOURCE_TABLE, module_name, "row_key")
        except Exception as err:
            raise ValueError("mongodb connection error %s" % (str(err)))

    def get_malice_link(self):
        filter_dict = dict()
        filter_dict['malicious_type'] = malicious_type_malware
        try:
            malware_url_count = self.table.count_by_condition(filter_dict)
        except Exception as err:
            raise ValueError("mongodb can not get malware cnt %s" % (str(err)))

        if malware_url_count < MAX_TRAIN_URL_COUNT:
            sellect_count = malware_url_count
        else:
            sellect_count = MAX_TRAIN_URL_COUNT

        try:
            self.table.scan_init()
            self.data = self.table.scan_by_condition(filter_dict, sellect_count)
        except Exception as err:
            raise ValueError("mongodb can not get malware data %s" % (str(err)))

    def do_get_malice_link(self, output_path):
        try:
            self.connect_db()
            self.get_malice_link()
        except Exception as err:
            logger.error("%s: publish_domain_regex: do_publish_domain_regex: %s" % (module_name, err))
            sys.exit(1)
        malice_link_list = list()
        for data_dict in self.data[0]:
            try:
                malice_link_list.append(data_dict[u"url"])
            except Exception as err:
                logger.error("%s: publish_domain_regex: do_publish_domain_regex: data format error %s %s"
                             % (module_name, str(data_dict), err))
        dump_urls(malice_link_list, output_path)


if __name__ == "__main__":
    get_data_obj = GetMaliceLink()
    get_data_obj.do_get_malice_link("./malice_link.csv")
