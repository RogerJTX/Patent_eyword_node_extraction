# -*- coding:utf-8 -*-
"""
Description:大变双向字典树
迭代次数默认最大999，可以增加但是没必要。其实能深到999层，那这个序列还是选择另外的处理方式吧。

author: jtx
date: 2020/12/18
"""
import sys,os
sys.path.append('/home/liangzhi/xjt/')
import pymongo
import re
import logging
from etl.utils.log_conf import configure_logging
import math
import datetime
import time
from tqdm import tqdm, trange

configure_logging("TRIE_TDIDF")  # 日志文件名
logger = logging.getLogger("spider")

client = pymongo.MongoClient('xxx', 0)
client.admin.authenticate("xxx", "xxx")
db = client.xxx
mongo_col_patent = db.xxx

class TrieNode(object):
    def __init__(self, value=None, count=0, parent=None):
        # 值
        self.value = value
        # 频数统计
        self.count = count
        # 父结点
        self.parent = parent
        # 子节点，{value:TrieNode}
        self.children = {}


class Trie(object):
    def __init__(self):
        # 创建空的根节点
        self.root = TrieNode()

    def insert(self, sequence):
        """
        基操，插入一个序列
        :param sequence: 列表
        :return:
        """
        cur_node = self.root
        for item in sequence:
            if item not in cur_node.children:
                # 插入结点
                child = TrieNode(value=item, count=1, parent=cur_node)
                cur_node.children[item] = child
                cur_node = child
            else:
                # 更新结点
                cur_node = cur_node.children[item]
                cur_node.count += 1

    def search(self, sequence):
        """
        基操，查询是否存在完整序列
        :param sequence: 列表
        :return:
        """
        cur_node = self.root
        mark = True
        for item in sequence:
            if item not in cur_node.children:
                mark = False
                break
            else:
                cur_node = cur_node.children[item]
        # 如果还有子结点，说明序列并非完整
        if cur_node.children:
            mark = False
        return mark

    def delete(self, sequence):
        """
        基操，删除序列，准确来说是减少计数
        :param sequence: 列表
        :return:
        """
        mark = False
        if self.search(sequence):
            mark = True
            cur_node = self.root
            for item in sequence:
                cur_node.children[item].count -= 1
                if cur_node.children[item].count == 0:
                    cur_node.children.pop(item)
                    break
                else:
                    cur_node = cur_node.children[item]
        return mark

    def search_part(self, sequence, prefix, suffix, start_node=None):
        """
        递归查找子序列，返回前缀和后缀结点
        此处简化操作，仅返回一位前后缀的内容与频数
        :param sequence: 列表
        :param prefix: 前缀字典，初始传入空字典
        :param suffix: 后缀字典，初始传入空字典
        :param start_node: 起始结点，用于子树的查询
        :return:
        """
        if start_node:
            cur_node = start_node
            prefix_node = start_node.parent
        else:
            cur_node = self.root
            prefix_node = self.root
        mark = True
        # 必须从第一个结点开始对比
        for i in range(len(sequence)):
            if i == 0:
                if sequence[i] != cur_node.value:
                    for child_node in cur_node.children.values():
                        self.search_part(sequence, prefix, suffix, child_node)
                    mark = False
                    break
            else:
                if sequence[i] not in cur_node.children:
                    for child_node in cur_node.children.values():
                        self.search_part(sequence, prefix, suffix, child_node)
                    mark = False
                    break
                else:
                    cur_node = cur_node.children[sequence[i]]
        if mark:
            if prefix_node.value:
                # 前缀数量取序列词中最后一词的频数
                if prefix_node.value in prefix:
                    prefix[prefix_node.value] += cur_node.count
                else:
                    prefix[prefix_node.value] = cur_node.count
            for suffix_node in cur_node.children.values():
                if suffix_node.value in suffix:
                    suffix[suffix_node.value] += suffix_node.count
                else:
                    suffix[suffix_node.value] = suffix_node.count
            # 即使找到一部分还需继续查找子结点
            for child_node in cur_node.children.values():
                self.search_part(sequence, prefix, suffix, child_node)

class ListCleaningProcess(object):
    def __init__(self):
        process_name = 'TF_IDF_clean'
        self.insert_num = 0

    def split_text(self, text, length):
        word_list = []
        each_word_list = list(text)
        len_each_word_list = len(each_word_list)
        for num, i in enumerate(each_word_list):
            if num+length <= len_each_word_list:
                word_list.append(''.join(each_word_list[num:num+length]))
                # print(each_word_list[num:num+length])
        return word_list
    
    def run_insert(self, trie_new, line):
        """
        数据清洗主入口
        :return:
        """
        # ============keyword加入树==============================
        for split_length in range(2, 8):
            word_list = self.split_text(line, split_length)
            # list去重 并加入树
            # word_list_set = list(set(word_list))
            word_list_set = word_list
            for each_word in word_list_set:
                if each_word:
                    # print(each_word)
                    trie_new.insert(each_word)
                    self.insert_num += 1
        return trie_new, self.insert_num

    def pp_trie(self, TrieNode_dict, record, list_name_and_count):
        # ============遍历树==============================
        if TrieNode_dict:
            for key, value in TrieNode_dict.items():
                print(key, 'parent:', value.parent.value, [y.value for x, y in value.children.items()], value.count)
                record['name'] = key
                record['count'] = value.count
                record['parent'] = value.parent.value
                list_name_and_count.append(list_name_and_count)
                if value:
                    self.pp_trie(value.children, record, list_name_and_count)
        else:
            return list_name_and_count

    def math_tf_idf(self, record, line, insert_num):
        # ============keyword计算tfidf==============================

        # name = record['name']
        count = record['count']

        # 计算IDF：log(语料库的文档总数/(包含该词的文档数+1)) + 1
        idf = math.log((insert_num + 1) / (count + 1)) + 1  # 返回x的自然对数， 默认底数为e
        # print(idf)

        # 计算TF：某个词在文章中出现的次数/文章总词数
        # frequency = line.count(name)
        # word_num_all = len(line) / len(name)
        # tf = frequency / word_num_all
        tf = count/insert_num
        tf_idf = tf * idf
        record['tf'] = tf
        record['idf'] = idf
        record['tf_idf'] = tf_idf
        return record





if __name__ == "__main__":
    logger.info('Begin Run')
    logger.info('建树开始时间：%s'% datetime.datetime.now())
    trie = Trie()
    trie_new = trie
    insert_num = 0

    # 加载树
    cleaning_process = ListCleaningProcess()
    for num, i in enumerate(mongo_col_patent.find()):
        title = i['title']
        abstract = i['abstract']
        text = (title+'。'+abstract).replace('.', '_')
        text_list = re.split('[。；？！（）：，{}【】“<>《》”’‘| \',?!()\]\[]', text)
        # print(text_list)
        for each_text in text_list:
            trie_new, insert_num = cleaning_process.run_insert(trie_new, each_text)
        if num % 100 == 0 and num != 0:
            logger.info("num:%s insert_num:%s "% (num, insert_num))
            break
    logger.info('建树完成时间：%s' % datetime.datetime.now())
    logger.info('insert_num_all:%s', insert_num)

    logger.info('计算tfidf开始时间：%s' % datetime.datetime.now())
    mongo_col_patent_count = mongo_col_patent.find().count()
    logger.info('mongo_col_patent_count: %s' % mongo_col_patent_count)
    # 计算每个文件的每个词的tfitdf
    for num, file in enumerate(mongo_col_patent.find()):
        # if num%100 == 0:
        logger.info('cleaning_num:%s'%num)
        title = file['title']
        abstract = file['abstract']
        text = (title + '。' + abstract).replace('.', '_')
        text_list = re.split('[。；？！（）：，{}【】“<>《》”’‘| \',?!()\]\[]', text)
        # print(text_list)
        list_compound = []
        for each_text in tqdm(text_list, desc='Processing'):
            for split_length in range(2, 8):
                word_list = cleaning_process.split_text(each_text, split_length)
                # print(word_list)
                prefixx = {}
                suffixx = {}


                for each_word in word_list:
                    if each_word:
                        record = {}
                        # print(list(each_word))
                        # 输入的是每个字
                        trie.search_part(list(each_word), prefixx, suffixx)
                        # print(prefixx)
                        # print(suffixx)
                        record['name'] = each_word
                        count_each_word = 0
                        for each_prefixx_key, each_prefixx_value in prefixx.items():
                            count_each_word += each_prefixx_value
                        record['count'] = count_each_word
                        record['prefixx_parent'] = prefixx
                        record['suffixx_children'] = suffixx
                        # 计算tfidf
                        record = cleaning_process.math_tf_idf(record, text, insert_num)
                        list_compound.append(record)

                # 把list排序 并更新mongo数据
                list_compound.sort(key=lambda k: (k.get('tf_idf', 0)), reverse=True)
                # print(list_compound_new)
                list_compound = list_compound[:100]
                mongo_col_patent.update_one({"_id": file['_id']}, {"$set": {"compound_new": list_compound}})

    logger.info('计算tfidf完成时间：%s' % datetime.datetime.now())
    logger.info('Finish Run')




