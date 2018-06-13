#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
抽取公共子串
"""
import copy


def sort_max_sub_string_list(string_list, sub_string_list):
    """
    将公共子串进行排序
    :param string_list:
    :param sub_string_list:
    :return:
    """
    sort_index = [[] for index in range(len(string_list))]
    sort_sub_string = list()
    for sub_string in sub_string_list:
        for string_index, string in enumerate(string_list):
            sort_index[string_index].append(string.index(sub_string))
        sort_sub_string = sorted(zip(sort_index[0], sub_string_list[:len(sort_index[0])]), key=lambda item: item[0])
        sort_sub_string = [_[1] for _ in sort_sub_string]
        for sort_index_each in sort_index[1:]:
            sort_sub_string_eva = sorted(zip(sort_index_each, sub_string_list[:len(sort_index_each)]),
                                         key=lambda item: item[0])
            sort_sub_string_eva = [_[1] for _ in sort_sub_string_eva]
            if sort_sub_string_eva != sort_sub_string:
                return sort_sub_string[:-1]
    return sort_sub_string


def maxsubstring(raw_string_list, min_char_count):
    """
    计算输入的字符串列表的公共子串
    :param raw_string_list:
    :param min_char_count:
    :return:
    """
    string_list = copy.deepcopy(raw_string_list)
    string_list_size = len(string_list)
    sub_string_list = list()
    if string_list_size == 0:
        return sub_string_list
    while True:
        ans_st, ans_len = -1, -1
        size0 = len(string_list[0])
        while ans_len == -1 and size0 > 0:
            pos0 = 0
            while ans_len == -1 and pos0 + size0 - 1 < len(string_list[0]):
                string_idx, flag0 = 1, 1
                while flag0 and string_idx < string_list_size:
                    pos1, flag1 = 0, 0
                    while flag1 == 0 and pos1 + size0 - 1 < len(string_list[string_idx]):
                        if string_list[0][pos0:pos0 + size0] == string_list[string_idx][pos1:pos1 + size0]:
                            flag1 = 1
                        pos1 += 1
                    if flag1 == 0:
                        flag0 = 0
                    string_idx += 1
                if flag0 != 0:
                    ans_len = size0
                    ans_st = pos0
                pos0 += 1
            size0 -= 1
        if ans_len == -1:
            break
        else:
            if ans_len < min_char_count:
                break
            sub_string_list.append(string_list[0][ans_st: ans_st + ans_len])
            string_list[0] = "\t".join([string_list[0][0:ans_st], string_list[0][ans_st + ans_len:]])
            for index in range(string_list_size-1):
                ans_st = string_list[index+1].index(sub_string_list[-1])
                string_list[index+1] = "\n".join([string_list[index+1][0:ans_st],
                                                  string_list[index+1][ans_st + ans_len:]])
    sub_string_list = sort_max_sub_string_list(raw_string_list, sub_string_list)
    return sub_string_list
