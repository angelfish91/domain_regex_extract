#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
域名聚类模块

"""
import time

from sklearn.cluster import KMeans
from sklearn.externals.joblib import Parallel, delayed

from file_io import dump_cluster_data
from vectorize import DomainVectorize
from cluster_string import string_cluster_subpro
from source_conf.config import logger, module_name, KMEANS_SIZE_LIMIT, KMEANS_SUB_SPLIT_SIZE, N_JOBS_CLUSTERING


class DomainCluster(object):
    """
    进行域名字符串聚类
    """
    def __init__(self):
        pass

    def make_string_distance_cluster(self, cluster_list, output_path):
        """
        :param cluster_list:
        :param output_path:
        :return:
        """
        # normalize input
        if not isinstance(cluster_list, list):
            raise ValueError("input must url list in list")

        # sorting the list to optimize multiprocess
        cluster_list = sorted(cluster_list, reverse=True, key=lambda lst: len(lst))

        # url distance cluster
        cluster_list = Parallel(n_jobs=N_JOBS_CLUSTERING)(delayed(string_cluster_subpro)(domain_list, index)
                                                          for index, domain_list in enumerate(cluster_list))
        cluster_dump = list()

        for cluster in cluster_list:
            cluster_dump.extend(cluster)
        dump_cluster_data(output_path, cluster_dump)

    def make_kmeans_cluster(self, domain_list, n_cluster):
        """
        :param domain_list:
        :param n_cluster:
        :return:
        """
        # normalize input
        if not isinstance(domain_list, list):
            raise ValueError("make_kmeans_cluster: input should be list not %s" % (str(type(domain_list))))

        if n_cluster <= 1:
            return [domain_list]

        vec_obj = DomainVectorize()
        df_vector = vec_obj.do_vectorize(domain_list)

        # make kmeans cluster
        kmeans = KMeans(n_clusters=n_cluster, verbose=0, random_state=0, n_jobs=N_JOBS_CLUSTERING)
        kmeans.fit(df_vector.values)
        df_vector["labels"] = kmeans.labels_

        # dump results
        res = list()
        for index in range(n_cluster):
            df_cluster = df_vector.loc[df_vector["labels"] == index]
            res.append(list(df_cluster.index))
        logger.info("k-means cluster done!")
        return res

    def _core_check_cluster_size(self, cluster_list):
        for index, cluster in enumerate(cluster_list):
            if len(cluster) > KMEANS_SIZE_LIMIT:
                url_list = cluster_list.pop(index)
                sub_cluster_list = self.make_kmeans_cluster(url_list, n_cluster=KMEANS_SUB_SPLIT_SIZE)
                cluster_list.extend(sub_cluster_list)
                return False
        return True

    def do_make_domain_clustering(self, domain_list, output_path):
        """
        :param domain_list:
        :param output_path:
        :return:
        """
        st_time = time.time()
        if not isinstance(domain_list, list):
            raise ValueError("do_make_domain_clustering: should be list not %s" % (str(type(domain_list))))

        preliminary_n_cluster = len(domain_list) / KMEANS_SIZE_LIMIT + 1
        cluster_list = self.make_kmeans_cluster(domain_list, n_cluster=preliminary_n_cluster)
        logger.info("Preliminary K-means clustering complete")
        while True:
            if self._core_check_cluster_size(cluster_list):
                break
        self.make_string_distance_cluster(cluster_list, output_path)
        end_time = time.time()
        logger.info("%s: [statistic] domain clustering time cost:%f" % (module_name, (end_time - st_time)))
