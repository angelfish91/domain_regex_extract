#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
字符串聚类模块
"""
import Levenshtein as ls

from source_conf.config import logger, module_name, SHORT_URL_THRESH, LONG_URL_THRESH, EDIT_DISTANCE_THRESH_LONG, \
    EDIT_DISTANCE_THRESH_SHORT, CLUSTER_REPORT_FQ


class StringClustering(object):
    """
    对Domain进行聚类分析
    """
    def __init__(self):
        pass

    def _core_distance_check(self, domain_0, domain_1):
        """
        判断两个字符串是否相似
        :param domain_0: base url string to compare
        :param domain_1: second url string to compare with base url string
        :return: true of false whether the comp_url is similar with base url
        """
        if not isinstance(domain_0, str) or not isinstance(domain_1, str):
            logger.error("%s:_core_distance_chec: domain_0/domain_1 should be str str not %s %s"
                         % (module_name, str(type(domain_0)), str(type(domain_1))))

        distance_thresh = 0
        url_length = len(domain_0)
        if url_length < SHORT_URL_THRESH:
            return False
        if SHORT_URL_THRESH <= url_length < LONG_URL_THRESH:
            distance_thresh = int(url_length * EDIT_DISTANCE_THRESH_SHORT)
        if LONG_URL_THRESH <= url_length:
            distance_thresh = int(url_length * EDIT_DISTANCE_THRESH_LONG)
        if ls.distance(domain_0, domain_1) <= distance_thresh:
            return True
        return False

    def do_string_distance_cluster(self, domain_list, job_index):
        """
        对粗粒度聚类结果的每个簇进行进一步的字符串相似度聚类
        :param domain_list: 输入的domain列表
        :param job_index: 子进程编号:
        :return:
        """
        if not isinstance(domain_list, list):
            raise ValueError("do_string_distance_cluster: domain_list should be list not %s" % str(type(domain_list)))

        cluster_list = list()
        cluster_done = set()
        # for each sub cluster of k-mean results
        for index, domain_0 in enumerate(domain_list):
            if domain_0 in cluster_done:
                continue
            cluster = [domain_0]
            cluster_done.add(domain_0)
            for domain_1 in domain_list[index + 1:]:
                if domain_1 not in cluster_done and self._core_distance_check(domain_0, domain_1):
                    cluster.append(domain_1)
                    cluster_done.add(domain_1)
            cluster_list.append(cluster)
            if index % CLUSTER_REPORT_FQ == 0:
                logger.info("batch:%d %d/%d" % (job_index, index, len(domain_list)))

        return cluster_list


def string_cluster_subpro(domain_list, job_index):
    """
    字符串聚类子进程
    :param domain_list 域名簇:
    :param job_index: 子进程编号
    :return:
    """
    str_cluster_obj = StringClustering()
    cluster_list = list()
    try:
        cluster_list = str_cluster_obj.do_string_distance_cluster(domain_list, job_index)
    except Exception as err:
        logger.error("%s: string_cluster_subpro: %s" % (module_name, str(err)))
    return cluster_list


if __name__ == "__main__":
    domains = ['asdfasdf', 'asdfsadfe', '123', 'sdaf', 'aaa', 'asdfsadf2']
    # domain_list = 1
    print string_cluster_subpro(domains, 0)
