import json
import re
import jieba

from utils.key_word_rule import exist_special_character, exist_group_prefix_for_interface
from utils.request_util import batch_send_embedding_message, get_bge_reranker
from utils.util import str_truncation_jieba, jaccard_similarity, metric_recommend


def get_window_phrases_for_metric(question):

    sentence = re.sub(r'\d+(-\d+)*年|\d+(-\d+)*月', '', question)

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
            if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys() or exist_special_character(phrase):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(search_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, search_word_position)
            if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys() or exist_special_character(phrase):
                continue
            all_window_phrases_for_metric[phrase] = position_list

    for window_size in range(window_lower_limit, window_upper_limit):
        for i in range(0, len(seg_all_word_position) - window_size + 1):
            phrase, position_list = str_truncation_jieba(i, window_size, seg_all_word_position)
            if len(phrase) <= 1 or phrase not in sentence or phrase in all_window_phrases_for_metric.keys() or exist_special_character(phrase):
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

    return all_window_phrases_for_metric



def get_metric_vector_match_for_interface(question, collection_name):
    '''
    给接口使用
    根据问题，获取最相关的一些指标
    '''

    phrases2positionList = get_window_phrases_for_metric(question)  # 剔除字形匹配的指标
    print("问题分词：", list(phrases2positionList.keys()))

    sentence_jieba_position_use = []
    threshold = 0.85
    phrase_recall_info = []
    all_match_word = []
    no_threshold_values_reranker = []


    # 获取所有窗口子串
    result_words = list(phrases2positionList.keys())
    if len(result_words) == 0:
        return [], [], []
    embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words), scores_threshold=0)

    # print(embedding_results)

    num = 0
    for item in embedding_results:
        phrases = result_words[num]
        num += 1
        if exist_group_prefix_for_interface(question, phrases):
            continue
        position_list = phrases2positionList[phrases]
        recall_list = [info['payload']['value'] for info in item if info['score'] >= threshold]
        if len(recall_list) > 0:
            if recall_list[0] != "手术":
                max_score = item[0]['score']
                new_dict = {"phrases": phrases, "position_list": position_list, "recall_list": recall_list, "max_score": max_score}
                phrase_recall_info.append(new_dict)

    # 先优先分数排序
    phrase_recall_info = sorted(phrase_recall_info, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    # 如果存在0.9以上的，则只取0.9以上的进行按照序列长度重新排序
    highly_similar = []
    for info in phrase_recall_info:
        if info["max_score"] >= 0.88:  # 此时recall_list只保留一个
            new_dict = {"phrases": info["phrases"], "position_list": info["position_list"], "recall_list": [info["recall_list"][0]], "max_score": info["max_score"]}
            highly_similar.append(new_dict)

    if len(highly_similar) > 0:
        # 按照召回的第一个词的长度排，不按照phrases长度的原因：例如 住院天数是多少 召回 住院天数 0.92， 住院天数 召回 住院天数 1.0
        highly_similar = sorted(highly_similar, key=lambda x: (len(x["recall_list"][0]), x["max_score"]), reverse=True)
        phrase_recall_info = []
        # 例如 急诊人次 召回 急诊就诊人次 0.96， 门急诊人次 召回 门急诊人次 1.0
        # 急诊人次 属于  门急诊人次， 且 max_score小，要过滤掉
        for info1 in highly_similar:
            reserve_flag = True
            for info2 in highly_similar:
                if info1["phrases"] in info2["phrases"] and info1["max_score"] <= info2["max_score"] and info1 != info2:
                    reserve_flag = False
                    break
            if reserve_flag:
                phrase_recall_info.append(info1)


    print("*****", "metric候选列表", phrase_recall_info)

    # 判断所有达到阈值是否来自相互重叠的片段 例如 [{'phrases': '主营业务毛利', 'position_list': [0, 1, 2, 3, 4, 5], 'recall_list': ['主营业务毛利'],
    # 'max_score': 1.0000001}, {'phrases': '主营业务毛利率', 'position_list': [0, 1, 2, 3, 4, 5, 6], 'recall_list': [
    # '主营业务毛利'], 'max_score': 0.94910306}, {'phrases': '业务毛利率', 'position_list': [2, 3, 4, 5, 6], 'recall_list': [
    # '业务毛利率'], 'max_score': 1.0}]

    completely_match_count = 0
    for info in phrase_recall_info:
        if info["max_score"] >= 1.0:
            completely_match_count += 1

    if completely_match_count >= 2:
        overlap_flag = True
        overlap_list = []
        for info in phrase_recall_info:
            if info["max_score"] < 1.0:
                continue
            if len(overlap_list) == 0:
                overlap_list = info["position_list"]
            intersection_list = [value for value in info["position_list"] if value in overlap_list]
            if len(intersection_list) == 0 or len(info["recall_list"]) > 1:
                overlap_flag = False
                break
            else:
                overlap_list = overlap_list + info["position_list"]

    else:
        overlap_flag = False

    if overlap_flag:
        for info in phrase_recall_info:
            if info["max_score"] < 1.0:
                continue
            recall_word = info["recall_list"][0]
            # 性别比例召回性别数量0.9，遇到数量再用字形筛一下
            # 病人数量召回患者数量0.9 但字形=0.5
            if '数量' in recall_word:
                if '数量' not in info["phrases"] and jaccard_similarity(info["phrases"], recall_word) < 0.5:
                    continue

            if recall_word not in all_match_word:
                all_match_word.append(recall_word)

    else:

        for info in phrase_recall_info:
            intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
            if len(intersection_list) == 0:
                sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
                for recall_word in info["recall_list"]:
                    # 性别比例召回性别数量0.9，遇到数量再用字形筛一下
                    # 病人数量召回患者数量0.9 但字形=0.5
                    if '数量' in recall_word:
                        if '数量' not in info["phrases"] and jaccard_similarity(info["phrases"], recall_word) < 0.5:
                            continue
                    all_match_word.append(recall_word)

        new_match_word = []
        for word1 in all_match_word:
            reserve_flag = True
            for word2 in all_match_word:
                if word1 != word2 and word1 in word2:
                    reserve_flag = False
            if reserve_flag:
                new_match_word.append(word1)

        all_match_word = new_match_word


    if len(all_match_word) >= 1:
        return all_match_word


    else:  # all_match_word 长度为0
        # 以下进行无阈值推荐
        no_threshold_reranker = []
        num = 0
        for item in embedding_results:
            phrases = result_words[num]
            num += 1
            recall_list = [info['payload']['value'] for info in item if info['score'] >= 0]
            if len(recall_list) > 0:
                rerank_result = get_bge_reranker(phrases, recall_list)
                no_threshold_reranker += [{"phrases": phrases, 'value': k, 'score': v} for k, v in rerank_result.items()]

        no_threshold_sorted_reranker = sorted(no_threshold_reranker, key=lambda x: x['score'], reverse=True)
        unique_results_reranker = metric_recommend(no_threshold_sorted_reranker)
        no_threshold_values_reranker = [result['value'] for result in unique_results_reranker[:10]]

        return no_threshold_values_reranker