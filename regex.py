#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-
"""
域名正则表达式提取模块

"""
import re
import time
import random
from collections import Counter, defaultdict

from dre_lib import tldextract
from dre_lib.maxsubstring import maxsubstring
from file_io import load_cluster_data, dump_regex_list
from source_conf.config import logger, module_name, DOMAIN_CLUSTER_SIZE_THRESH, DOMAIN_TOKEN_FREQUENCY_THRESH, \
    MAX_SUBSTRING_SIZE_LIMIT, DOMAIN_TOKEN_SAMPLE_ROUND, DOMAIN_TOKEN_SAMPLE_RATIO, DOMAIN_TOKEN_SAMPLE_UPBOUND, \
    SMALL_CLUSTER_SIZE, BIG_CLUSTER_SIZE, DOMAIN_TOKEN_SAMPLE_LOWBOUND, TLD_SLD_COUNT_UPBOUND


class DomainRegexExtract(object):
    """
    域名正则表达式抽取模块

    """
    def __init__(self):
        pass

    def __domain_token_regex_match(self, regex, domain):
        """
        对域名token进行匹配
        :param regex:
        :param domain:
        :return:
        """
        regex = "".join(["^", regex, "$"])
        pattern = re.compile(regex)
        if pattern.match(domain):
            return True
        return False

    def __domain_regex_match(self, regex, domain):
        """
        域名正则表达匹配
        :param regex:
        :param domain:
        :return:
        """
        pattern = re.compile(regex)
        if pattern.match(domain):
            return True
        return False

    def __filter_domain_list(self, domain_list):
        return [domain for domain in domain_list if domain.count(".") == domain_list[0].count(".")]

    def domain_regex_deduplicate(self, regex_list):
        """
        正则表达式去重
        :param regex_list:
        :return:
        """
        pattern0 = re.compile("\{(\d{1,})\,(\d{1,})\}")
        pattern1 = re.compile("\{(\d{1,})\}")

        index_done, regex_done = list(), list()
        for index0, regex0 in enumerate(regex_list):
            if index0 in index_done:
                continue
            p0_res0 = pattern0.search(regex0)
            p1_res0 = pattern1.search(regex0)
            if p0_res0 is not None:
                rex0_str = "\t".join([regex0[:p0_res0.span()[0]], regex0[p0_res0.span()[1]:]])
                rex0_lo = int(p0_res0.groups()[0])
                rex0_hi = int(p0_res0.groups()[1])
            elif p1_res0 is not None:
                rex0_str = "\t".join([regex0[:p1_res0.span()[0]], regex0[p1_res0.span()[1]:]])
                rex0_lo = rex0_hi = int(p1_res0.groups()[0])
            else:
                regex_done.append(regex0)
                continue

            for index1, regex1 in enumerate(regex_list[index0 + 1:]):
                p0_res1 = pattern0.search(regex1)
                p1_res1 = pattern1.search(regex1)
                if p0_res1 is not None:
                    if rex0_str == "\t".join([regex1[:p0_res1.span()[0]], regex1[p0_res1.span()[1]:]]):
                        index_done.append(index0 + index1 + 1)
                        rex0_lo = min(int(p0_res1.groups()[0]), rex0_lo)
                        rex0_hi = max(int(p0_res1.groups()[1]), rex0_hi)
                elif p1_res1 is not None:
                    if rex0_str == "\t".join([regex1[:p1_res1.span()[0]], regex1[p1_res1.span()[1]:]]):
                        index_done.append(index0 + index1 + 1)
                        rex0_lo = min(int(p1_res1.groups()[0]), rex0_lo)
                        rex0_hi = max(int(p1_res1.groups()[0]), rex0_hi)
            if rex0_lo != rex0_hi:
                regex_done.append("%s{%d,%d}%s" %
                                  (rex0_str.split("\t", 1)[0], rex0_lo, rex0_hi, rex0_str.split("\t", 1)[1]))
            else:
                regex_done.append("%s{%d}%s" %
                                  (rex0_str.split("\t", 1)[0], rex0_lo, rex0_str.split("\t", 1)[1]))
        return regex_done

    def domain_token_analyze(self, domain_list):
        """
        对域名各部分token进行统计分析
        :param domain_list:
        :return:
        """
        domain_size = len(domain_list)
        # 拆分各级别的token
        each_level_dict = defaultdict(list)
        for domain in domain_list:
            tld_obj = tldextract.extract(domain)
            each_level_dict[0].append(tld_obj.suffix)
            each_level_dict[1].append(tld_obj.domain)
            if tld_obj.subdomain:
                for level, token in enumerate(tld_obj.subdomain.split(".")[::-1]):
                    each_level_dict[level + 2].append(token)
        # 统计各级别的token
        each_level_tree = defaultdict(list)
        for level, token_list in each_level_dict.iteritems():
            sorted_token_list = sorted(Counter(token_list).items(), key=lambda itme: itme[1], reverse=True)
            for counter, (token, token_count) in enumerate(sorted_token_list):
                if float(token_count) / float(domain_size) > DOMAIN_TOKEN_FREQUENCY_THRESH:
                    each_level_tree[level].append(token)
                elif (level == 0 or level == 1) and token not in each_level_tree[level] and \
                        counter < TLD_SLD_COUNT_UPBOUND:
                    each_level_tree[level].append(token)
        return each_level_dict, each_level_tree

    def __string_regex_extract(self, string_list):
        """
        无公共子串字符串正则表达式抽取
        :param string_list:
        :return:
        """
        pattern = ""
        string_len_list = [len(string) for string in string_list]
        size_max = max(string_len_list)
        size_min = min(string_len_list)
        if size_max == 0:
            pass
        elif sum([string.isdigit() or not string for string in string_list]) == len(string_list):
            if size_max == size_min:
                pattern = "\d{%d}" % size_max
            else:
                pattern = "\d{%d,%d}" % (size_min, size_max)
        elif sum([string.isalpha() or not string for string in string_list]) == len(string_list):
            if size_max == size_min:
                pattern = "[A-Za-z]{%d}" % size_max
            else:
                pattern = "[A-Za-z]{%d,%d}" % (size_min, size_max)
        elif sum([string.isalnum() or not string for string in string_list]) == len(string_list):
            if size_max == size_min:
                pattern = "\w{%d}" % size_max
            else:
                pattern = "\w{%d,%d}" % (size_min, size_max)
        else:
            if size_max == size_min:
                pattern = "[^\.]{%d}" % size_max
            else:
                pattern = "[^\.]{%d,%d}" % (size_min, size_max)
        return pattern

    def string_regex_extract(self, string_list):
        """
        对域名中各个子域名token进行进行正则表达式抽取
        :param string_list:
        :return:
        """
        # 首先抽取字符串集合的公共子串
        max_sub_string_list = maxsubstring(string_list, MAX_SUBSTRING_SIZE_LIMIT)
        max_sub_string_size = len(max_sub_string_list)
        # 判断有无公共子串, 若不存在公共子串
        pattern = ""
        if max_sub_string_size == 0:
            pattern = self.__string_regex_extract(string_list)
            return pattern
        # 判断有无公共子串, 若存在公共子串
        for index in range(max_sub_string_size):
            left_strings, right_strings = list(), list()
            max_sub_string = max_sub_string_list[index]
            for string in string_list:
                left_strings.append(string.split(max_sub_string, 1)[0])
                right_strings.append(string.split(max_sub_string, 1)[1])
            pattern = "".join([pattern, self.__string_regex_extract(left_strings), max_sub_string])
            string_list = right_strings
        pattern += self.__string_regex_extract(string_list)
        return pattern

    def build_domain_token_tree(self, domain_list):
        """
        构建域名token树
        :param domain_list:
        :return:
        """
        # 过滤域名列表
        domain_list = self.__filter_domain_list(domain_list)
        # 进行token统计分析
        token_dict, token_tree = self.domain_token_analyze(domain_list)
        for level in token_dict:
            # 对无法抽出高频词的token列表进行正则表达式抽取
            if level not in token_tree.keys():
                token_list = token_dict[level]
                score_list, regex_list = list(), list()
                for sample_round in range(DOMAIN_TOKEN_SAMPLE_ROUND):
                    sample_num = int(len(token_list) * DOMAIN_TOKEN_SAMPLE_RATIO)
                    if sample_num > DOMAIN_TOKEN_SAMPLE_UPBOUND:
                        sample_num = DOMAIN_TOKEN_SAMPLE_UPBOUND
                    if sample_num <= DOMAIN_TOKEN_SAMPLE_LOWBOUND:
                        sample_num = len(token_list)
                    token_sample = random.sample(token_list, sample_num)

                    try:
                        regex = self.string_regex_extract(token_sample)
                    except Exception as err:
                        regex = "[^\.]{%d,%d}" \
                            % (min([len(token) for token in token_sample]), max([len(token) for token in token_sample]))
                        logger.error("%s: regex: build_domain_token_tree: %s" % (module_name, str(err)))

                    regex_list.append(regex)
                    score_list.append(sum([self.__domain_token_regex_match(regex, token) for token in token_list]))
                max_score_index = score_list.index(max(score_list))
                regex = regex_list[max_score_index]
                token_tree[level].append(regex)
        return token_tree

    def build_domain_regex(self, token_tree):
        """
        将toke tree拼装为正则表达式
        :param token_tree:
        :return:
        """
        token_regex_list = []
        for level in range(len(token_tree) - 1, -1, -1):
            token_regex = "|".join(token_tree[level])
            if len(token_tree[level]) == 1:
                token_regex_list.append(token_regex)
            else:
                token_regex_list.append("".join(["(:?", token_regex, ")"]))
        domain_regex = "\.".join(token_regex_list)
        domain_regex = "".join(["^", domain_regex, "$"])
        logger.info("%s: raw regex %s" % (module_name, domain_regex))
        return domain_regex

    def do_domain_regex_extract(self, input_path, output_path):
        """
        域名正则表达抽取主函数
        :param input_path:
        :param output_path:
        :return:
        """
        st_time = time.time()
        # 对聚类结果进行统计并写入日志
        cluster_list = load_cluster_data(input_path)
        size_list = [len(cluster) for cluster in cluster_list]
        logger.info("%s: total cluster num:\t%d" % (module_name, len(cluster_list)))
        logger.info("%s: single one:\t%d" % (module_name, len([1 for size in size_list if size == 1])))
        logger.info("%s: small cluster:\t%d" %
                    (module_name, len([1 for size in size_list if SMALL_CLUSTER_SIZE <= size < BIG_CLUSTER_SIZE])))
        logger.info("%s: big cluster:\t%d" %
                    (module_name, len([1 for size in size_list if size >= BIG_CLUSTER_SIZE])))

        # 过滤过小的簇
        cluster_list = [cluster for cluster in cluster_list if len(cluster) >= DOMAIN_CLUSTER_SIZE_THRESH]

        # 抽取正则表达式
        domain_token_tree_list = [self.build_domain_token_tree(cluster) for cluster in cluster_list]
        domain_regex_list = list(set([self.build_domain_regex(token_tree) for token_tree in domain_token_tree_list]))
        domain_regex_list = self.domain_regex_deduplicate(domain_regex_list)
        end_time = time.time()

        # 将结果写入日志
        logger.info("%s: extract regex count:\t%d" % (module_name, len(domain_regex_list)))
        logger.info("%s: [statistic] domain regex extract time cost:%f" % (module_name, (end_time - st_time)))
        dump_regex_list(domain_regex_list, output_path)
