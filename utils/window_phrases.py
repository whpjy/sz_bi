import re

import jieba

from config.log_config import logger
from utils.key_word_rule import exist_special_character, exist_sql_word, get_sql_word_pos, contain_sql, \
    exist_sql_word_for_metric, get_time_word_by_llm
from utils.util import str_truncation_jieba


def get_window_phrases(user_object, task="other"):
    sentence = user_object.history[0]["user"]

    # user_object.metric_recognize_by_phrase 是识别的指标所对应的片段
    table_name_list = user_object.slot_dict["table"]
    aggregation_name_list = user_object.slot_dict["aggregation"]
    timeType_name_list = user_object.slot_dict["timeType"]
    group_name_list = user_object.group_recognize_by_phrase

    """
    要考虑已经字形匹配的指标要不要从句子中剔除
    例如：2023年普外科手术人数  字形识别：手术，如果不剔除，“手术人数” 向量召回推荐 手术部位数量、手术医师数量、...
    """
    # del_word_list = table_name_list + aggregation_name_list
    del_word_list = user_object.metric_recognize_by_phrase + table_name_list + aggregation_name_list + timeType_name_list + group_name_list

    logger.info(f"user_object.metric_recognize_by_phrase：{user_object.metric_recognize_by_phrase}")
    if task == "group":
        logger.info(f"--原始句子：{sentence}")
        logger.info(f"--需剔除词：{del_word_list}")
    # del_word_list +=
    if task == "where":
        # 用模型识别出时间片段，剔除掉
        time_word_list = get_time_word_by_llm(sentence)
        for word in time_word_list:
            if word in sentence:
                sentence = sentence.replace(word, ',')

    for del_word in del_word_list:
        if del_word in sentence:
            sentence = sentence.replace(del_word, ",")

    sentence = re.sub(r'\d+(-\d+)*年|\d+(-\d+)*月', '', sentence)

    user_object.rewrite_input = sentence
    if task == "group":
        logger.info(f"--新的句子：{sentence}")

    all_window_phrases = {}

    # 有些长的编号（特殊字符、数字、字母）可能被分为多个片段，这里先用正则匹配到放到词库
    pattern = '(?:[^\w\s、，,。！？]|[0-9]+|[a-zA-Z]+)+'
    matches = re.findall(pattern, sentence)
    if len(matches) > 0:
        for match in matches:
            if match in sentence:
                start = sentence.find(match)
                end = start + len(match)
                position_list = [i for i in range(start, end)]
                all_window_phrases[match] = position_list

    default_result = jieba.tokenize(sentence, mode='default')
    default_word_position = []
    for token in default_result:
        # 根据正则匹配手动识别的来判断是否保留当前分词
        contain_flag = True
        for match, match_position_list in all_window_phrases.items():
            start = match_position_list[0]
            end = match_position_list[-1] + 1  # 因为结巴分词的最后一个位置多1个
            # 判断结巴分出来的是否是子串或者有交叉
            if (token[1] >= start and token[1] <= end) or (token[2] >= start and token[2] <= end):
                contain_flag = False
                break
        if contain_flag:
            default_word_position.append({"word": token[0], "position_list": [i for i in range(token[1], token[2])]})

    search_result = jieba.tokenize(sentence, mode='search')
    search_word_position = []
    for token in search_result:
        # 根据正则匹配手动识别的来判断是否保留当前分词
        contain_flag = True
        for match, match_position_list in all_window_phrases.items():
            start = match_position_list[0]
            end = match_position_list[-1] + 1  # 因为结巴分词的最后一个位置多1个
            # 判断结巴分出来的是否是子串或者有交叉
            if (token[1] >= start and token[1] <= end) or (token[2] >= start and token[2] <= end):
                contain_flag = False
                break
        if contain_flag:
            search_word_position.append({"word": token[0], "position_list": [i for i in range(token[1], token[2])]})

    seg_all = jieba.cut(sentence, cut_all=True)
    seg_all_word_position = []
    start = 0
    for token in seg_all:
        tk1 = sentence.find(token, start)
        tk2 = tk1 + len(token)
        # 根据正则匹配手动识别的来判断是否保留当前分词
        contain_flag = True
        for match, match_position_list in all_window_phrases.items():
            start = match_position_list[0]
            end = match_position_list[-1] + 1  # 因为结巴分词的最后一个位置多1个
            # 判断结巴分出来的是否是子串或者有交叉
            if (tk1 >= start and tk1 <= end) or (tk2 >= start and tk2 <= end):
                contain_flag = False
                break
        if contain_flag:
            seg_all_word_position.append({"word": token, "position_list": [i for i in range(tk1, tk2)]})


    window_lower_limit = 1
    window_upper_limit = 4

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(default_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, default_word_position)
            if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases.keys() or exist_special_character(phrase):
                continue
            all_window_phrases[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(search_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, search_word_position)
            if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases.keys() or exist_special_character(phrase):
                continue
            all_window_phrases[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(seg_all_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, seg_all_word_position)
            if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases.keys() or exist_special_character(phrase):
                continue
            all_window_phrases[phrase] = position_list

    if task == "group" or task == "group_label":
        logger.info(f"--{task}分词结果：{all_window_phrases}")
        sql_where = []
        if "WHERE" in user_object.test2sql_information.keys():
            sql_where = user_object.test2sql_information["WHERE"]
        sql_word_pos = get_sql_word_pos(sql_where, sentence)
        if len(sql_word_pos) > 0:
            logger.info(f"--根据sql_where再次过滤：sql_where {sql_word_pos}")
        group_window_phrases = {}
        for phrase, position_list in all_window_phrases.items():
            if not exist_sql_word(phrase, position_list, sql_word_pos):
                group_window_phrases[phrase] = position_list

        logger.info(f"--{task}再次分词结果：{list(group_window_phrases.keys())}")
        return group_window_phrases

    return all_window_phrases


def get_window_phrases_for_metric(user_object):

    sentence = user_object.history[0]["user"].strip()
    origin_sentence = sentence
    sentence = re.sub(r'\d+(-\d+)*年|\d+(-\d+)*月', '', sentence)

    sql_where = []
    sql_group = []
    if "WHERE" in user_object.test2sql_information.keys():
        sql_where = user_object.test2sql_information["WHERE"]
    if "GROUP BY" in user_object.test2sql_information.keys():
        sql_group = user_object.test2sql_information["GROUP BY"]

    metric_name_list = list(user_object.metric_type.keys())

    sql_word_pos = get_sql_word_pos(sql_group + sql_where, sentence)
    logger.info(f"指标sql（group和where词）识别：{sql_word_pos}")
    default_result = jieba.tokenize(sentence, mode='default')
    default_word_position = []
    for token in default_result:
        default_word_position.append({"word": token[0], "position_list": [i for i in range(token[1], token[2])]})

    search_result = jieba.tokenize(sentence, mode='search')
    search_word_position = []
    for token in search_result:
        search_word_position.append({"word": token[0], "position_list": [i for i in range(token[1], token[2])]})

    seg_all = jieba.cut(sentence, cut_all=True)
    seg_all_word_position = []
    start = 0
    for token in seg_all:
        start = sentence.find(token, start)
        seg_all_word_position.append({"word": token, "position_list": [i for i in range(start, start + len(token))]})

    all_window_phrases_for_metric = {}
    window_lower_limit = 1
    window_upper_limit = 6
    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(default_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, default_word_position)
            if (len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys()
                    or exist_special_character(phrase)):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(search_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, search_word_position)
            if (len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys()
                    or exist_special_character(phrase)):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(seg_all_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, seg_all_word_position)
            if (len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys()
                    or exist_special_character(phrase)):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    # 2024年主营业务毛利率  枚举不出“业务毛利率” seg_all_word_position分词：业务/毛利/毛利率/利率，用跳跃一个词匹配
    for i in range(0, len(seg_all_word_position) - 3 + 1):
        phrase = seg_all_word_position[i]["word"] + seg_all_word_position[i+2]["word"]
        position_list = seg_all_word_position[i]["position_list"] + seg_all_word_position[i+2]["position_list"]
        if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys() or exist_special_character(
                phrase):
            continue
        all_window_phrases_for_metric[phrase] = position_list


    if len(list(all_window_phrases_for_metric.keys())) == 0:
        all_window_phrases_for_metric[sentence] = [i for i in range(0, len(sentence))]
        logger.info(f"指标分词为空，直接使用全部输入：{list(all_window_phrases_for_metric.keys())}")

    return all_window_phrases_for_metric



def get_window_phrases_for_metric_recommend(user_object):
    '''
    与get_window_phrases_for_metric的分词区别： recommend分词要尽可能覆盖多
    get_window_phrases_for_metric：舍弃任何与sql_where和sql_group有交叉的词
    get_window_phrases_for_metric_recommend：仅舍弃sql_where和sql_group做为字词的情况，如果和分词结果相同则保留
    '''
    sentence = user_object.history[0]["user"].strip()
    sentence = re.sub(r'\d+(-\d+)*年|\d+(-\d+)*月', '', sentence)

    sql_where = []
    sql_group = []
    if "WHERE" in user_object.test2sql_information.keys():
        sql_where = user_object.test2sql_information["WHERE"]
    if "GROUP BY" in user_object.test2sql_information.keys():
        sql_group = user_object.test2sql_information["GROUP BY"]

    default_result = jieba.tokenize(sentence, mode='default')
    default_word_position = []
    for token in default_result:
        default_word_position.append({"word": token[0], "position_list": [i for i in range(token[1], token[2])]})

    search_result = jieba.tokenize(sentence, mode='search')
    search_word_position = []
    for token in search_result:
        search_word_position.append({"word": token[0], "position_list": [i for i in range(token[1], token[2])]})

    seg_all = jieba.cut(sentence, cut_all=True)
    seg_all_word_position = []
    start = 0
    for token in seg_all:
        start = sentence.find(token, start)
        seg_all_word_position.append({"word": token, "position_list": [i for i in range(start, start + len(token))]})

    all_window_phrases_for_metric = {}
    window_lower_limit = 1
    window_upper_limit = 6
    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(default_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, default_word_position)
            if (len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys()
                    or exist_special_character(phrase) or contain_sql(phrase, sql_group + sql_where)):
                continue

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(search_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, search_word_position)
            if (len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys()
                    or exist_special_character(phrase) or contain_sql(phrase, sql_group + sql_where)):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(seg_all_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, seg_all_word_position)
            if (len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys()
                    or exist_special_character(phrase) or contain_sql(phrase, sql_group + sql_where)):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    # 2024年主营业务毛利率  枚举不出“业务毛利率” seg_all_word_position分词：业务/毛利/毛利率/利率，用跳跃一个词匹配
    for i in range(0, len(seg_all_word_position) - 3 + 1):
        phrase = seg_all_word_position[i]["word"] + seg_all_word_position[i+2]["word"]
        position_list = seg_all_word_position[i]["position_list"] + seg_all_word_position[i+2]["position_list"]
        if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys() or exist_special_character(
                phrase):
            continue
        all_window_phrases_for_metric[phrase] = position_list

    if len(list(all_window_phrases_for_metric.keys())) == 0:
        all_window_phrases_for_metric[sentence] = [i for i in range(0, len(sentence))]
        logger.info(f"指标分词为空，直接使用全部输入：{list(all_window_phrases_for_metric.keys())}")

    return all_window_phrases_for_metric

