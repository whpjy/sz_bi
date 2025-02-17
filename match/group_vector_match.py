import re

from config.log_config import logger
from utils.json_util import find_max_list
from utils.request_util import batch_send_embedding_message, get_bge_reranker, send_llm
from utils.util import get_best_group, metric_recommend, group_recommend
from utils.window_phrases import get_window_phrases


def get_group_vector_match(user_object, collection_name, group_name_list, main_table_id, relation_table_id, self_group_name2id_dict):

    all_table_id = main_table_id + relation_table_id
    logger.info(f"--group_vector_match获取分词")
    user_object.jieba_window_phrases2positionList = get_window_phrases(user_object, task="group")  # 剔除字形匹配的指标

    sentence_jieba_position_use = []
    threshold = 0.8
    phrase_recall_info = []
    all_match_word = []
    phrsase2groupList = {}

    # 获取所有窗口子串
    result_words = list(user_object.jieba_window_phrases2positionList.keys())
    if len(result_words) == 0:
        return []
    embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words), scores_threshold=0)

    num = 0
    for item in embedding_results:
        phrases = result_words[num]
        num += 1
        position_list = user_object.jieba_window_phrases2positionList[phrases]
        recall_list = [info for info in item if info["score"] >= threshold and info["payload"]["tableId"] in all_table_id]
        # print("-----", phrases, " recall: ", recall_list)
        if len(recall_list) > 0:
            max_score = recall_list[0]["score"]
            new_dict = {"phrases": phrases, "position_list": position_list,
                        "recall_word": recall_list[0]["payload"]["value"],
                        "columnId": recall_list[0]["payload"]["columnId"],
                        "max_score": max_score}
            phrase_recall_info.append(new_dict)
            '''
                phrase_recall_info 只对每个位置保存一个最大值，但位置如果对应多个相同值会有另外的字典记录
                因为重叠位置也肯能对应多个相同值，这样可以过滤掉重叠位置
                另外的字典记录：如果存在连续几个相同的value
                1、先看是否有main_table_id，如果有则break
                2、将phrases作为键值，多个value对应存字典
            '''
            count = 0
            recall_word = recall_list[0]["payload"]["value"]
            for info in recall_list:
                if info["payload"]["value"] == recall_word:
                    count += 1

            if count > 1:  # 说明存在连续几个相同的value
                exist_main_table_id_flag = False
                for i in range(0, count):
                    if recall_list[i]["payload"]["tableId"] in main_table_id:
                        exist_main_table_id_flag = True
                if not exist_main_table_id_flag:
                    phrsase2groupList[phrases] = []
                    for i in range(0, count):
                        phrsase2groupList[phrases].append(recall_list[i]["payload"])

            # assert 0
    if len(phrsase2groupList) > 0:
        print("*****", "关联维度存在同名：", phrsase2groupList)
    phrase_recall_info = sorted(phrase_recall_info, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in phrase_recall_info:
        logger.info(f'group 召回：phrases：{info["phrases"]}: score: {info["max_score"]} value: {info["recall_word"]}')
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            if info["phrases"] == '住院' and info["recall_word"] == '病房':
                continue
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            if info["phrases"] not in phrsase2groupList.keys():
                all_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"], "columnId": info["columnId"], "max_score": info["max_score"]})
            else:
                # 关联同名的维度识别出来的多放了columnId，因为更新到槽位时本身无法只根据同名维度确定id
                if info["recall_word"] in self_group_name2id_dict.keys():
                    all_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"],
                                           "columnId": self_group_name2id_dict[info["recall_word"]], "max_score": info["max_score"]})
                else:
                    groupList = phrsase2groupList[info["phrases"]]
                    best_group = get_best_group(user_object, info["recall_word"], groupList)
                    all_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"],
                                                    "columnId": best_group["columnId"], "max_score": info["max_score"]})

    return all_match_word



def get_group_label_vector_match(user_object, collection_name, group_name_list):
    user_object.jieba_window_phrases2positionList = get_window_phrases(user_object, task="group_label")  # 剔除字形匹配的指标

    sentence_jieba_position_use = []
    threshold = 0.8
    phrase_recall_info = []
    all_match_word = []
    match2phrase = {}

    # 获取所有窗口子串
    result_words = list(user_object.jieba_window_phrases2positionList.keys())
    if len(result_words) == 0:
        return []
    embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words),
                                                     scores_threshold=0)

    num = 0
    for item in embedding_results:
        phrases = result_words[num]
        num += 1
        position_list = user_object.jieba_window_phrases2positionList[phrases]
        for info in item:
            if info['score'] >= threshold:
                recall_weidu_list = info['payload']['weidu_list']
                contain_weidu_list = [weidu for weidu in recall_weidu_list if weidu in group_name_list]
                if len(contain_weidu_list) > 0:
                    max_score = info['score']
                    new_dict = {"phrases": phrases, "position_list": position_list, "recall_weidu_list": contain_weidu_list, "max_score": max_score}
                    phrase_recall_info.append(new_dict)
                    break

    phrase_recall_info = sorted(phrase_recall_info, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)
    logger.info(f"group label phrase_recall_info：{phrase_recall_info}")
    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            all_match_word.append({"phrases": info["phrases"], "recall_weidu_list": info["recall_weidu_list"], "max_score": info["max_score"]})

    return all_match_word


def get_group_sql_vector_match(user_object, group_name_list, label_use_phrase, collection_name):
    sentence = user_object.history[0]["user"]
    table_name_list = user_object.slot_dict["table"]
    aggregation_name_list = user_object.slot_dict["aggregation"]
    timeType_name_list = user_object.slot_dict["timeType"]
    group_recognize_list = user_object.group_recognize_by_phrase
    del_word_list = user_object.metric_recognize_by_phrase + table_name_list + aggregation_name_list + timeType_name_list + group_recognize_list + label_use_phrase
    logger.info(f"--通过sql抽取的group降低阈值分析")
    logger.info(f"--原问题：{sentence}")
    for del_word in del_word_list:
        if del_word in sentence:
            sentence = sentence.replace(del_word, ",")
    sentence = re.sub(r'\d+(-\d+)*年|\d+(-\d+)*月', '', sentence)
    logger.info(f"--删除已识别信息后的句子：{sentence}")

    sql_group = []
    if "GROUP BY" in user_object.test2sql_information.keys():
        sql_group = user_object.test2sql_information["GROUP BY"]
    logger.info(f"--sql_group：{sql_group}")

    sql_word_list = []
    for sql_group_word in sql_group:
        if sql_group_word in sentence:
            sql_word_list.append(sql_group_word)

    if len(sql_word_list) == 0:
        return []

    embedding_results = batch_send_embedding_message(sql_word_list, 1, [collection_name] * len(sql_word_list),
                                                             scores_threshold=0)
    all_match_word = []
    num = 0
    for item in embedding_results:
        phrases = sql_word_list[num]
        num += 1
        recall_list = [info['payload']['value'] for info in item if info['score'] >= 0 and info['payload']['value'] in group_name_list]
        no_threshold_reranker = []
        if len(recall_list) > 0:
            rerank_result = get_bge_reranker(phrases, recall_list)
            logger.info(f"--“{phrases}” 与维度名计算结果：{rerank_result}")
            no_threshold_reranker += [{"phrases": phrases, 'value': k, 'score': v} for k, v in rerank_result.items()]
            unique_results_reranker = group_recommend(no_threshold_reranker)
            if len(unique_results_reranker) > 0:
                recall_weidu_list = [info["value"] for info in unique_results_reranker]
                # 重排后再让模型来选一下
                recall_weidu_list = llm_choose_group(phrases, recall_weidu_list)
                if len(recall_weidu_list) > 0:
                    logger.info(f"--sql_group phrases：{phrases}，模型选择：{recall_weidu_list}")
                    all_match_word.append({"phrases": phrases, "recall_weidu_list": recall_weidu_list})
            else:
                logger.info(f"--放弃 “{phrases}”")

    return all_match_word


def llm_choose_group(phrases, group_name_list):

    prompt = f'''基于用户的输入词从全部分组名称中选择出相关的分组名称，如所有分组名称都不能明确反应用户意图，则不选择任何分组名称并返回空列表
用户的输入词为：{phrases}
全部分组名称列表为：{group_name_list}
规则：
1.你选择的分组名称可以是0个、1个或多个，但必须与用户输入直接相关。不要选择间接或模糊相关的词。
2.无法判断或没有合适的分组名称时，返回空列表[]。'''

    print(prompt)
    result = send_llm(prompt)
    result = find_max_list(result)
    final_list = [group_name for group_name in result if group_name in group_name_list]
    return final_list


def extract_keshi_bianma(user_object, collection_name, keshi_bianma_list):

    columnIdList = [info["columnId"] for info in keshi_bianma_list]
    user_object.jieba_window_phrases2positionList = get_window_phrases(user_object)

    threshold = 0.91
    # 获取所有窗口子串
    result_words = list(user_object.jieba_window_phrases2positionList.keys())
    result_words = [word + "科室编码" for word in result_words]
    if len(result_words) == 0:
        return []
    embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words),
                                                     scores_threshold=0)
    num = 0
    phrase_recall_info = []
    for item in embedding_results:
        phrases = result_words[num]
        num += 1
        recall_list = [info for info in item if
                       info["score"] >= threshold and info["payload"]["columnId"] in columnIdList]

        if len(recall_list) > 0:
            max_score = recall_list[0]["score"]
            new_dict = {"phrases": phrases,
                        "recall_word": recall_list[0]["payload"]["value"],
                        "columnId": recall_list[0]["payload"]["columnId"],
                        "max_score": max_score}
            phrase_recall_info.append(new_dict)

    if len(phrase_recall_info) == 0:
        return []
    else:
        phrase_recall_info = sorted(phrase_recall_info, key=lambda x: (x["max_score"]), reverse=True)
        return [{"columnId": phrase_recall_info[0]["columnId"], "columnName": phrase_recall_info[0]["recall_word"]}]