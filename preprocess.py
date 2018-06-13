#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
URL数据统计与预处理模块
"""
import re
import time

from dre_lib import tldextract
from urlnormalize import UrlNormalize
from source_conf.config import logger, module_name


class UrlPreprocess(object):
    """
    对将要训练的URL抽取域名
    """
    def __init__(self):
        pass

    @staticmethod
    def check_domain(domain):
        """
        检测一个字符串是否是域名
        :param domain: 待检测的字符串
        :return: bool，如果该字符串是域名返回True，否则返回False
        """
        regex_domain = re.compile('(?i)^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$')
        if regex_domain.match(domain) is None:
            return False
        return True

    @staticmethod
    def check_ip(check_str):
        """
        检测一个字符串是否是IP
        :param check_str: 待检测的字符串
        :return: bool，如果该字符串是IP返回True，否则返回False
        """
        regex_ip = re.compile(
            '^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
        if regex_ip.match(check_str) is None:
            return False
        return True

    def _core_preprocess(self, url):
        res = ""
        if not isinstance(url, str):
            logger.error("%s: vectorize: input url is not string" %(module_name))
            return res
        url_obj = UrlNormalize(url)
        hostname = url_obj.get_hostname()
        if UrlPreprocess.check_domain(hostname) and not UrlPreprocess.check_ip(hostname):
            tld_obj = tldextract.extract(hostname)
            primary_domain = tld_obj.domain
            sub_domain = tld_obj.subdomain
            if primary_domain and sub_domain and sub_domain != "www":
                res = hostname
        return res


    def do_url_preprocess(self, url_list):
        """
        URL训练预处理
        :param url_list:
        :return:
        """
        domain_list = list()
        st_time = time.time()
        for url in url_list:
            try:
                domain_list.append(self._core_preprocess(url))
            except Exception as err:
                logger.error("%s: preprocess: preprocess url %s error %s" %(module_name, url, str(err)))
        domain_list = list(set([domain for domain in domain_list if domain]))
        end_time = time.time()
        logger.info("%s: [statistic] url preprocess time cost:%f\tdomain count:%d" %
                    (module_name, (end_time-st_time), len(domain_list)))
        return domain_list


if __name__ == "__main__":
    url_list = ["http://sre.baidu.com", "http://www.xxx.com", 1, "xx.cd", "ftp://fe.we.com.cn"]
    process_obj = UrlPreprocess()
    domain_list = process_obj.do_url_preprocess(url_list)
    print domain_list