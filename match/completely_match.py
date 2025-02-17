from utils.util import cal_start_pos


def get_completely_match(user_input, need_match_word):
    completely_match_word_list = []
    completely_match_word_pos = []
    finally_completely_match_word_list = []

    # 执行完全匹配
    for word in need_match_word:
        if word in user_input and len(word) > 0:
            completely_match_word_list.append(word)

    # 筛选出不是其他词子集的词汇
    for word1 in completely_match_word_list:
        reserve_flag = True
        for word2 in completely_match_word_list:
            if word1 in word2 and word1 != word2:
                reserve_flag = False
        if reserve_flag:
            completely_match_word_pos.append({"word": word1, "start_pos": user_input.find(word1)})

    completely_match_word_pos = sorted(completely_match_word_pos, key=lambda x: (x["start_pos"]), reverse=False)

    for info in completely_match_word_pos:
        finally_completely_match_word_list.append(info["word"])
    return finally_completely_match_word_list


def get_completely_match_list2list(list1, list2):
    completely_match_word_list = []

    # 执行完全匹配
    for word1 in list1:
        for word2 in list2:
            if word1 == word2 and len(word1) > 0:
                completely_match_word_list.append(word1)

    return completely_match_word_list


def get_group_label_completely_match(real_input, group_label_dict):
    # 字典按照键值的字符串长度从大到小排序
    all_match_word = []
    phrase_recall_info = []
    sentence_jieba_position_use = []
    sort_group_label_dict = dict(sorted(group_label_dict.items(), key=lambda item: len(item[0]), reverse=True))
    for word, weidu_list in sort_group_label_dict.items():
        if word in real_input:
            start_pos = cal_start_pos(real_input, word)
            phrase_recall_info.append({"phrases": word, "recall_weidu_list": weidu_list, "start_pos": start_pos, "position_list": [start_pos + i for i in range(0, len(word))]})

    phrase_recall_info = sorted(phrase_recall_info, key=lambda x: len(x["position_list"]), reverse=True)
    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            all_match_word.append({"phrases": info["phrases"], "recall_weidu_list": info["recall_weidu_list"], "start_pos": info["start_pos"]})

    return all_match_word