#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
向量化URL
"""
import numpy as np
import pandas as pd

from source_conf.config import logger, module_name, ASCII_SIZE


class DomainVectorize(object):
    """
    对输入的URL进行向量化
    """
    def __init__(self):
        pass

    def _core_vectorize(self, domain):
        res = np.zeros((1, ASCII_SIZE), dtype=np.int32)
        if not isinstance(domain, str):
            logger.error("%s: vectorize: input url is not string" %(module_name))
            return res
        for char in domain:
            if ord(char) < ASCII_SIZE:
                res[0][ord(char)] += 1
        return res

    def do_vectorize(self, domain_list):
        """
        :param domain_list:
        :return:
        """
        if not isinstance(domain_list, list):
            raise ValueError("input must list not %s" %str(type(domain_list)))

        res = list()
        for index, domain in enumerate(domain_list):
            res.append(self._core_vectorize(domain))
        res = np.concatenate(res, axis=0)

        df = pd.DataFrame(res)
        df['domain'] = domain_list
        df = df.set_index('domain')

        logger.info("%s: vectorize: vectorization complete! data shape:\t%s" %(module_name, str(df.values.shape)))
        return df

if __name__ == "__main__":
    domain_list = ["www.taobao.com", "www.baidu.com", 1]
    vec_obj = DomainVectorize()
    df = vec_obj.do_vectorize(domain_list)
    print df