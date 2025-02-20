import re
import time

from config import CURRENT_SCENE
from config.log_config import logger
from utils.json_util import find_max_list
from utils.key_word_rule import exist_rank, get_sql_word_pos, rule_judge
from utils.request_util import send_embedding_message, batch_send_embedding_message, get_bge_reranker, send_llm
from utils.rule_word import noise_word
from utils.util import jaccard_similarity, fuzzy_conflict_selection, get_sorce_from_target


def get_where_word_vector_match(where_word, collection_name):
    threshold = 0.8
    # 获取所有窗口子串

    embedding_results = send_embedding_message(where_word, collection_name, scores_threshold=0)

    # 筛选和排序结果
    filtered_results = [item for item in embedding_results if item['score'] >= threshold]
    sorted_results = sorted(filtered_results, key=lambda x: x['score'], reverse=True)
    # 处理数据
    processed_results = [{'value': item['payload']['value'], 'score': item['score']} for item in sorted_results]
    # 去除重复项
    unique_results = []
    seen_values = set()
    for item in processed_results:
        if item['value'] not in seen_values:
            unique_results.append(item)
            seen_values.add(item['value'])

    # 选取前6个唯一值
    selected_values = [result['value'] for result in unique_results[:6]]

    return selected_values




def get_where_sentence_vector_match(user_object, columnId_list, all_columnId2columnName, origin_columnId2values, columnId_list_relation):  # 用之前jieba已经分好的词的不同组合：jieba_window_phrases2positionList_list
    '''
        get_where_sentence_vector_match_relation
        识别当前指标的自身表维度下的where值
        模糊匹配提前考虑关联维度、向量匹配后续考虑
    '''
    # --------提前规则匹配模糊查询的where值
    fuzzy_dict = {}
    for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
        # 过滤干扰性词语
        if phrases in noise_word or exist_rank(phrases):
            continue
        for columnId in columnId_list + columnId_list_relation:  # 自身维度和关联维度同时考虑
            contain_count = 0
            for info in origin_columnId2values[columnId]:
                targetValue = info["targetValue"]
                if phrases in targetValue:
                    contain_count += 1
            if contain_count >= 1:  # 2个及以上
                if phrases not in fuzzy_dict.keys():
                    fuzzy_dict[phrases] = {"position_list": position_list, "columnIdList": [columnId]}
                else:
                    if columnId not in fuzzy_dict[phrases]["columnIdList"]:
                        fuzzy_dict[phrases]["columnIdList"].append(columnId)

    fuzzy_dict_list = []
    for phrases, dict_info in fuzzy_dict.items():
        fuzzy_dict_list.append({"phrases": phrases, "position_list": dict_info["position_list"], "columnIdList": dict_info["columnIdList"]})
    logger.info(f"自身维度+关联维度的模糊词初始识别结果：{fuzzy_dict_list}")
    fuzzy_dict_list_pre = sorted(fuzzy_dict_list, key=lambda x: (len(x["position_list"]), -x["position_list"][0]), reverse=True)
    fuzzy_position_use = []
    final_fuzzy_dict = []
    # phrases 是纯数字字母和特殊字符的不要
    fuzzy_dict_list = []
    for fuzzy_dict in fuzzy_dict_list_pre:
        pattern = '(?:[^\w\s、，,。！？]|[0-9]+|[a-zA-Z]+)+'
        matches = re.findall(pattern, fuzzy_dict["phrases"])  # 对于编号类的要特殊考虑
        if len(matches) == 1:
            if matches[0] == fuzzy_dict["phrases"]:
                continue
            else:
                fuzzy_dict_list.append(fuzzy_dict)
        else:
            fuzzy_dict_list.append(fuzzy_dict)


    for info in fuzzy_dict_list:
        intersection_list = [value for value in info["position_list"] if value in fuzzy_position_use]
        if len(intersection_list) == 0:
            fuzzy_position_use = fuzzy_position_use + info["position_list"]
            # info['columnIdList'] 有可能存在id不同但columnName相同的情况，此时选择第一个id
            columnIdList = []
            columnNameList = []
            for columnId in info['columnIdList']:
                columnName = all_columnId2columnName[columnId]
                if columnName not in columnNameList:
                    columnIdList.append(columnId)
                    columnNameList.append(columnName)

            if len(columnIdList) == 0:
                continue

            info['columnIdList'] = columnIdList
            if len(info['columnIdList']) == 1:  # 模糊值只存在于一个列表可以直接确定
                columnId = info['columnIdList'][0]
                where_dict = {"columnId": columnId, "columnName": all_columnId2columnName[columnId],
                              "targetValue": info["phrases"], "value": info["phrases"], "CompleteMatch": False}
                user_object.slot_dict["where"].append(where_dict)
                user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrases"], "position_list": info["position_list"]})
                logger.info(f"模糊值只存在于一个列表直接确定：{where_dict}")
            else:  # 模糊值同时存在于多个列表进入多轮
                final_fuzzy_dict.append(info)
                logger.info(f"模糊值准备进入多轮：{info}")

    # --------后续基于分词做向量召回，这里根据模糊阶段使用的fuzzy_position_use做筛选
    # phrase_list = []
    # for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
    #     intersection_list = [value for value in position_list if value in fuzzy_position_use]
    #     if len(intersection_list) == 0:
    #         phrase_list.append(phrases)
    #
    # if len(phrase_list) == 0:
    #     return {}, final_fuzzy_dict

    phrase_list = list(user_object.jieba_window_phrases2positionList.keys())
    if len(phrase_list) == 0 or len(columnId_list) == 0:
        return {}, final_fuzzy_dict

    # --------开始进行向量召回
    origin_input = user_object.history[0]["user"]
    sentence_jieba_position_use = {}
    phrase_recall_info = {}
    current_phrase_recall_info = {}
    scores_threshold_first = 0.7

    collection_name_list = []
    for columnId in columnId_list:
        sentence_jieba_position_use[columnId] = []
        phrase_recall_info[columnId] = []
        current_phrase_recall_info[columnId] = []
        collection_name_list = collection_name_list + [CURRENT_SCENE + "_weidu_" + str(columnId)] * len(phrase_list)

    print("*****", "分词(维度)：", phrase_list)
    print("*****", "where->qdrant请求次数(词组合数*维度数)：", len(phrase_list * len(columnId_list)))
    logger.info(f"where vector分词：{list(phrase_list)}")

    time1 = time.time()
    recall_result_list = batch_send_embedding_message(phrase_list, len(columnId_list), collection_name_list, scores_threshold_first)
    time2 = time.time()
    print("*****", "where->qdrant批量请求耗时：", time2 - time1)

    columnId_list_index = 0
    for columnId in columnId_list:
        recall_result_index = columnId_list_index * len(phrase_list)
        columnId_list_index += 1
        for phrases in phrase_list:
            position_list = user_object.jieba_window_phrases2positionList[phrases]
            recall_list, max_score = get_where_phrase_vector_match(phrases, recall_result_list[recall_result_index], origin_input)
            recall_result_index += 1
            if len(recall_list) > 0:
                new_dict = {"phrases": phrases, "position_list": position_list, "recall_list": recall_list, "max_score": max_score}
                phrase_recall_info[columnId].append(new_dict)

    logger.info(f"自身维度初始识别结果：{phrase_recall_info}")
    # --------1、每个维度先确定一个达到阈值的最佳选择
    current_slot_dict = []
    for columnId in columnId_list:
        if len(phrase_recall_info[columnId]) > 0:
            # 维度内部的过滤：同分值的必须更长的排前面 心血管内科 内科 心血管 都能召回，此时只需要 心血管内科， 如果 心血管内科 排后面会意外召回 内科
            phrase_recall_info[columnId] = sorted(phrase_recall_info[columnId], key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)
            for info in phrase_recall_info[columnId]:
                intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use[columnId]]
                if len(intersection_list) == 0:
                    sentence_jieba_position_use[columnId] = sentence_jieba_position_use[columnId] + info["position_list"]
                    if len(info["recall_list"]) == 1:  # 只有一个可以直接确认
                        exist_flag = False
                        for where_info in user_object.slot_dict["where"]:
                            if info["recall_list"][0] == where_info["value"]:
                                exist_flag = True
                                break
                        if not exist_flag:
                            # 提前筛选出where值，但此时仍然是非确定的，因为目前是各维度下取出一个最佳，最佳之间可能召回原始片段可能是重叠的，需要二次判断
                            where_dict = {"columnId": columnId, "columnName": all_columnId2columnName[columnId], "targetValue": info["recall_list"][0],
                                          "value": get_sorce_from_target(origin_columnId2values, columnId, info["recall_list"][0]), "CompleteMatch": True}
                            current_slot_dict.append({"phrases": info["phrases"], "position_list": info["position_list"], "max_score": info["max_score"], "where_dict": where_dict})
                    else:
                        current_phrase_recall_info[columnId].append(info)

    # --------2、二次过滤最佳选择之间的重叠问题
    position_use = []
    # 维度之间的过滤：同分值的必须更长的排前面 心血管内科 内科 心血管 都能召回，此时只需要 心血管内科， 如果 心血管内科 排后面会意外召回 内科
    current_slot_dict = sorted(current_slot_dict, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)
    for info in current_slot_dict:
        intersection_list = [value for value in info["position_list"] if value in position_use]
        if len(intersection_list) == 0:
            position_use = position_use + info["position_list"]
            # 这里需要再基于已确定模糊值和模糊推荐再相互筛选
            logger.info(f"向量结果根据现有模糊识别能否放入where槽位：{info['where_dict']}")
            fuzzy_value_exist, fuzzy_recommend_exist, final_fuzzy_dict = fuzzy_conflict_selection(user_object, final_fuzzy_dict, info["phrases"])
            if not fuzzy_value_exist and not fuzzy_recommend_exist:
                user_object.slot_dict["where"].append(info["where_dict"])
                user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrases"], "position_list": info["position_list"]})
                logger.info(f"放入where槽位成功")
            else:
                logger.info(f"放入where槽位失败")

    # --------3、final_phrase_recall_info 进行推荐也要过滤一下，不能推荐的原始片段和已确定的片段重叠
    final_phrase_recall_info = {}
    for columnId in columnId_list:
        for info in current_phrase_recall_info[columnId]:
            intersection_list = [value for value in info["position_list"] if value in position_use]
            if len(intersection_list) == 0:
                # 这里需要再基于已确定模糊值和模糊推荐再相互筛选
                logger.info(f"向量结果根据现有模糊识别判断能否进入where多轮（columnId：{columnId}）：{info}")
                fuzzy_value_exist, fuzzy_recommend_exist, final_fuzzy_dict = fuzzy_conflict_selection(user_object, final_fuzzy_dict, info["phrases"])
                logger.info(f"放入where多轮成功")
                if not fuzzy_value_exist and not fuzzy_recommend_exist:
                    final_phrase_recall_info[columnId].append(info)
                else:
                    logger.info(f"放入where多轮失败")

    return final_phrase_recall_info, final_fuzzy_dict


def get_where_sentence_vector_match_relation(user_object, columnId_list, all_columnId2columnName, origin_columnId2values):  # 用之前jieba已经分好的词的不同组合：jieba_window_phrases2positionList_list
    '''
    get_where_sentence_vector_match_relation
    识别当前指标的关联表维度下的where值
    '''
    origin_input = user_object.history[0]["user"]
    sentence_jieba_position_use = {}
    phrase_recall_info = {}
    scores_threshold_first = 0.7

    # --------后续基于分词做向量召回，这里根据模糊阶段使用的fuzzy_position_use做筛选
    # phrase_list = []
    # for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
    #     intersection_list = [value for value in position_list if value in fuzzy_position_use]
    #     if len(intersection_list) == 0:
    #         phrase_list.append(phrases)

    phrase_list = list(user_object.jieba_window_phrases2positionList.keys())
    if len(phrase_list) == 0:
        return []

    collection_name_list = []
    for columnId in columnId_list:
        sentence_jieba_position_use[columnId] = []
        phrase_recall_info[columnId] = []
        collection_name_list = collection_name_list + [CURRENT_SCENE + "_weidu_" + str(columnId)] * len(phrase_list)

    print("*****", "分词(维度)--关联维度：", phrase_list)
    print("*****", "where->qdrant请求次数(词组合数*维度数)：", len(phrase_list * len(columnId_list)))
    time1 = time.time()
    recall_result_list = batch_send_embedding_message(phrase_list, len(columnId_list), collection_name_list, scores_threshold_first)
    time2 = time.time()
    print("*****", "where->qdrant批量请求耗时：", time2 - time1)

    columnId_list_index = 0
    for columnId in columnId_list:
        recall_result_index = columnId_list_index * len(phrase_list)
        columnId_list_index += 1
        for phrases in phrase_list:
            position_list = user_object.jieba_window_phrases2positionList[phrases]
            recall_list, max_score = get_where_phrase_vector_match(phrases, recall_result_list[recall_result_index], origin_input)
            recall_result_index += 1

            if len(recall_list) > 0:
                new_dict = {"phrases": phrases, "position_list": position_list, "recall_list": [recall_list[0]], "max_score": max_score}
                # recall_list 只保留一个值，不会进入单个维度下的多轮了，只考虑多维度的多轮
                phrase_recall_info[columnId].append(new_dict)

    logger.info(f"关联维度初始识别结果：{phrase_recall_info}")
    # 1、每个维度先确定一个达到阈值的最佳选择
    # print("phrase_recall_info: ", phrase_recall_info)
    current_slot_dict = []
    for columnId in columnId_list:
        if len(phrase_recall_info[columnId]) > 0:
            # 维度内部的过滤：同分值的必须更长的排前面 心血管内科 内科 心血管 都能召回，此时只需要 心血管内科， 如果 心血管内科 排后面会一外召回 内科
            phrase_recall_info[columnId] = sorted(phrase_recall_info[columnId], key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)
            for info in phrase_recall_info[columnId]:
                intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use[columnId]]
                if len(intersection_list) == 0:
                    sentence_jieba_position_use[columnId] = sentence_jieba_position_use[columnId] + info["position_list"]
                    if len(info["recall_list"]) == 1:  # 关联维度只有一个
                        exist_flag = False
                        for where_info in user_object.slot_dict["where"]:
                            if info["recall_list"][0] == where_info["value"]:
                                exist_flag = True
                                break
                        if not exist_flag:
                            # 提前筛选出where值，但此时仍然是非确定的，因为目前是各维度下取出一个最佳，最佳之间可能召回原始片段可能是重叠的，需要二次判断
                            where_dict = {"phrases": info["phrases"], "position_list": info["position_list"], "columnId": columnId, "columnName": all_columnId2columnName[columnId], "targetValue": info["recall_list"][0],
                                          "value": get_sorce_from_target(origin_columnId2values, columnId, info["recall_list"][0]), "CompleteMatch": True}
                            current_slot_dict.append({"phrases":info["phrases"], "position_list": info["position_list"], "max_score": info["max_score"], "where_dict": where_dict})
                    else:
                        pass

    # 2、二次过滤最佳选择之间的重叠问题
    # print("current_slot_dict: ", current_slot_dict)
    final_slot_dict = []
    position_use = []
    # 维度之间的过滤：同分值的必须更长的排前面 心血管内科 内科 心血管 都能召回，此时只需要 心血管内科， 如果 心血管内科 排后面会一外召回 内科
    current_slot_dict = sorted(current_slot_dict, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)
    for info in current_slot_dict:
        intersection_list = [value for value in info["position_list"] if value in position_use]
        if len(intersection_list) == 0:
            position_use = position_use + info["position_list"]
            final_slot_dict.append(info["where_dict"])

    print("关联候选结果：", final_slot_dict)
    logger.info(f"关联维度候选结果：{final_slot_dict}")

    return final_slot_dict  # 返回关联维度识别的结果，暂时不放进slot_dict槽位，等自身维度完全确定之后再进一步判断这个


def get_where_phrase_vector_match(phrases, recall_result, origin_input):
    scores_threshold_second = 0.8  # 有时threshold_first召回一批，但jaccard值很低（例如低于0.5），此时要拉高阈值, 此时len(phrases)最好 > 2， 解决 职工在职 ， 职工， 达到0.66的
    value2score = {}

    exist_value = []  # 这里都是phrases在同一纬度的召回结果，避免如果同一纬度有相同值（后端bug）进行的重复推荐
    temporary_recall_result = []

    if isinstance(recall_result, str):
        print("存在问题：json中提供新维度，但向量数据库可能未更新查不到该维度的where向量库")
        return [], 0

    for info in recall_result:
        if info['payload']['value'] not in exist_value:
            value = info['payload']['value']
            exist_value.append(value)
            score = info['score']
            if score > 1.0:
                score = 1.0
            temporary_recall_result.append({'value': value, 'score': score})

    # 例如 同一个phrase可能召回 心血管内科 和 内科 两个 score=1.0的值，也要考虑 心血管内科 这个结果放前面
    temporary_recall_result = sorted(temporary_recall_result, key=lambda x: (x["score"], len(x["value"])), reverse=True)

    if isinstance(temporary_recall_result, list):
        if len(temporary_recall_result) != 0:
            all_racall_list = []
            substring_list = []  # 当search_word是召回词子词则存储该召回词
            for info in temporary_recall_result:
                value = info['value']
                score = info['score']
                jaccard_score = jaccard_similarity(phrases, value)

                if '三级手术' == value:
                    print("-------------")

                if len(value) == 4 and value[-3:] == '级手术' and value[0] not in phrases:
                    continue
                # 说明
                # (jaccard_score >= 0.3 and len(phrases) > 2 and score >= 0.8)   普通情况
                # (phrases in value and score >= 0.75 and len(phrases) > 2)   phrases是子串可以减少阈值，但保持len(phrases)大于2
                # (jaccard_score >= 0.66 and len(phrases) == 2 and score >= 0.8) len(phrases)等于2，要提升jaccard_score, 2/3才可以考虑。 骨科->骨外科0.8280
                # (jaccard_score == 0.5 and len(phrases) == 2 and len(value) == 1 and score >= 0.85) 男性->男

                if ((jaccard_score >= 0.3 and len(phrases) > 2 and score >= 0.8)
                        or (phrases in value and score >= 0.75 and len(phrases) > 2)
                        or (jaccard_score >= 0.66 and len(phrases) == 2 and score >= 0.8)
                        or (jaccard_score == 0.5 and len(phrases) == 2 and len(value) == 1 and score >= 0.85)
                ):
                    if value not in all_racall_list:
                        all_racall_list.append(value)
                    value2score[value] = score
                    if phrases in value:
                        substring_list.append(value)
                # print(value, score)

            if len(substring_list) == 0:  # 当search_word不为任何召回词的子串
                if len(all_racall_list) == 1:  # 仅一个召回结果可以直接确定
                    return all_racall_list, value2score[all_racall_list[0]]
                if len(all_racall_list) > 1:
                    return all_racall_list[:6], value2score[all_racall_list[0]]

            elif len(substring_list) == 1:  # 含子串召回词只有一个结果可以直接确定
                return substring_list, value2score[substring_list[0]]

            else:  # 含子串召回词有多个结果返回前6个全部进行推荐
                return substring_list[:6], value2score[substring_list[0]]

    return [], 0


def get_sql_where_vector_match(user_object, CURRENT_SCENE, all_columnId2columnName):

    logger.info(f"user_object.where_recognize_by_phrase_pos: {user_object.where_recognize_by_phrase_pos}")
    all_columnId_list = []
    for columnId in user_object.where_weidu_list:
        if columnId not in all_columnId_list:
            all_columnId_list.append(columnId)

    sql_where = []
    if "WHERE" in user_object.test2sql_information.keys():
        sql_where = user_object.test2sql_information["WHERE"]

    logger.info(f"--原始sql_where: {sql_where}")
    sql_word_pos = get_sql_word_pos(sql_where, user_object.rewrite_input)
    logger.info(f"--sql_word_pos: {sql_word_pos}")

    phrase_list = []
    for phrase, position_list in sql_word_pos.items():
        overlap_flag = False
        for info in user_object.where_recognize_by_phrase_pos:
            word_pos = info["position_list"]
            intersection_list = [value for value in word_pos if value in position_list]
            if len(intersection_list) != 0:
                overlap_flag = True
                break

        if not overlap_flag:
            if rule_judge(phrase):
                phrase_list.append(phrase)

    logger.info(f"--筛选sql_where: {phrase_list}")

    sql_muti_dict = []
    if len(phrase_list) > 0:
        collection_name_list = []
        for columnId in all_columnId_list:
            collection_name_list.append(CURRENT_SCENE + "_weidu_" + str(columnId))

        all_collection_name_list = collection_name_list * len(phrase_list)
        # 注意collection_name_list里面的顺序不与where召回的写法相同

        recall_result_list = batch_send_embedding_message(phrase_list, len(all_columnId_list), all_collection_name_list, 0)

        # 对每个词的召回结果 （每个词在每个维度都有召回）进行重排
        index = 0
        for phrase in phrase_list:  # sql_where 可能有多个
            logger.info(f"--当前phrase：{phrase}")
            col_index = index * len(all_columnId_list)
            reranker_dict = {}
            maxx_score = -10
            for columnId in all_columnId_list:  # 在每一列都有召回一次
                recall_word_list = []
                for info in recall_result_list[col_index]:  # 每一列的每个值的计算结果
                    value = info['payload']['value']
                    recall_word_list.append(value)
                col_index += 1
                if len(recall_word_list) > 0:
                    rerank_result = get_bge_reranker(phrase, recall_word_list)
                    max_score = max(rerank_result.values())
                    if maxx_score < max_score:
                        maxx_score = max_score

                    # 当前phrase 在每一列的每个值的召回结果进行重排保存
                    reranker_dict[columnId] = {"max_score": max_score, "recall_list": rerank_result}
                    # print(phrase, rerank_result)

            # 如果这个phrase包含编号等特殊字符，还要单独抽取再召回匹配一下，例如：sql_where=手术编码是96.04000, 整个词无法匹配到96.04000
            pattern = '(?:[^\w\s、，,。！？]|[0-9]+|[a-zA-Z]+)+'
            matches = re.findall(pattern, phrase)
            if len(matches) == 1:
                if matches[0] != phrase:
                    special_recall_result_list = batch_send_embedding_message(matches, len(all_columnId_list), collection_name_list, 0.95)
                    special_index = 0
                    for columnId in all_columnId_list:  # 在每一列都有召回一次
                        special_recall_word = {}
                        for info in special_recall_result_list[special_index]:  # 每一列的每个值的计算结果
                            if info['score'] >= 1.0:
                                value = info['payload']['value']
                                special_recall_word = {}
                                special_recall_word[value] = 10
                                maxx_score = 10
                                break
                            elif info['score'] >= 0.95:
                                value = info['payload']['value']
                                special_recall_word[value] = 10 * info['score']
                                max_score = 10 * info['score']
                                if maxx_score < max_score:
                                    maxx_score = max_score

                        special_index += 1
                        if len(special_recall_word) > 0:
                            reranker_dict[columnId] = {"max_score": maxx_score, "recall_list": special_recall_word}

                            # 任意维度下的“值”相关性都小于0，有可能用户输入的模糊where是 用维度 指 值 ，例如 我们部门、我们科....这仍然是where
            if maxx_score <= 0:
                # 通过模糊where和所有维度名再计算一下，看是否可以定位到具体where
                logger.info(f"--任意维度下的“值”相关性都小于0，通过模糊where和所有维度名再计算一下")
                where_name = {columnId: all_columnId2columnName[columnId] for columnId in all_columnId_list}
                rerank_result = get_bge_reranker(phrase, list(where_name.values()))
                logger.info(f"--维度名：{where_name}")
                logger.info(f"--“{phrase}” 和维度名的计算结果：{rerank_result}")
                max_score = list(rerank_result.values())[0]
                max_columnName = list(rerank_result.keys())[0]
                # 通过计算phrase与维度名，存在正相关的，可以用最大值确定维度名
                if max_score > 0:
                    final_columnId = -1
                    for columnId, columnName in where_name.items():
                        if max_columnName == columnName:
                            final_columnId = columnId
                            break
                    # 先通过columnName找到columnId，再通过reranker_dict找到  phrase 与 维度下的值的计算重排结果
                    if final_columnId != -1:
                        logger.info(f"--选择维度名：“{max_columnName}”，维度id：{final_columnId}")
                        recall_list = reranker_dict[final_columnId]['recall_list']
                        logger.info(f"--“{max_columnName}” 维度重排：{recall_list}")
                        # 选择前5个进入多轮
                        sql_muti_dict.append({"phrase": phrase, "position_list": sql_word_pos[phrase], "recall_list": list(recall_list.keys())[:5], "columnId": final_columnId})

                else:
                    logger.info(f"--没有合适的维度名，放弃 “{phrase}”")

            # 某个维度下的“值”存在正相关的
            else:
                # 筛选出所有达到最大值的，可能会存在多个达到最大值的情况
                new_reranker_dict = {columnId: info for columnId, info in reranker_dict.items() if info["max_score"] >= maxx_score}

                final_columnId = -1
                if len(new_reranker_dict) == 1:
                    final_columnId = list(new_reranker_dict.keys())[0]
                elif len(new_reranker_dict) > 1:
                    where_name = {columnId: all_columnId2columnName[columnId] for columnId, info in new_reranker_dict.items()}
                    where_name_list = list(where_name.values())
                    logger.info(f"--发现多个columnName分数相同: {where_name_list}")

                    # 多个columnName分数相同，此时先看用户输入有没有输入
                    max_columnName = ''
                    user_input = user_object.history[0]["user"]
                    for name in where_name_list:
                        if name in user_input:
                            max_columnName = name
                            logger.info(f"--用户提及了 “{name}”: 选择这个维度")
                            break

                    # 用户输入有没有提及，则进行维度计算
                    if len(max_columnName) == 0:
                        rerank_result = get_bge_reranker(phrase, where_name_list)
                        logger.info(f"--重排columnName: {rerank_result}")

                        # phrase与维度名计算有存在大于0，选择最大值作为columnName
                        if list(rerank_result.values())[0] > 0:
                            max_columnName = list(rerank_result.keys())[0]
                        # 全部小于0，此时仍不能直接确定columnName
                        else:
                            # 此时只从自身维度考虑
                            logger.info(f"--phrase与维度名计算全部小于0，此时从自身维度来选择")

                            # 自身维度 要结合self_where_weidu_list 和 columnName分数相同的 where_name 进行交叉筛选
                            self_where_name = {columnId: all_columnId2columnName[columnId] for columnId in
                                               user_object.self_where_weidu_list if columnId in list(where_name.keys())}

                            if len(self_where_name) > 0:
                                rerank_result = get_bge_reranker(phrase, list(self_where_name.values()))
                                logger.info(f"--自身维度重排columnName: {rerank_result}")
                                max_columnName = list(rerank_result.keys())[0]
                            else:
                                logger.info(f"--自身维度为空，默认从全局选择一个最大值")
                                max_columnName = list(rerank_result.keys())[0]

                    for columnId, columnName in where_name.items():
                        if max_columnName == columnName:
                            final_columnId = columnId
                            break

                    logger.info(f"--选择最佳columnName: {max_columnName}，columnId：{final_columnId}")

                if final_columnId != -1:
                    # 先筛选这一维度重排大于0的 值
                    reranker = new_reranker_dict[final_columnId]["recall_list"]
                    sort_reranker = dict(sorted(reranker.items(), key=lambda item: item[1], reverse=True))
                    logger.info(f"--当前columnName结果: {sort_reranker}")
                    maxx_score = list(sort_reranker.values())[0]
                    positive_value = []
                    positive_value_fu1 = [] # 筛选大于负1的
                    for value, score in sort_reranker.items():
                        if score > 0:
                            positive_value.append(value)
                        if score > -1:
                            positive_value_fu1.append(value)

                    if len(positive_value_fu1) == 1:
                        if maxx_score >= 1.6:
                            # 可以直接确定
                            logger.info(f"--直接确定: {phrase} 为 {positive_value_fu1[0]}")
                            sql_muti_dict.append({"phrase": phrase, "position_list": sql_word_pos[phrase], "recall_list": positive_value_fu1, "columnId": final_columnId})

                    elif len(positive_value_fu1) > 1:
                        # 先让模型选一下看能不能直接确定
                        llm_result = llm_choose_where(phrase, positive_value_fu1)
                        # 需要处理进入多轮
                        if len(llm_result) == 0:
                            logger.info(f"--进入多轮： {positive_value_fu1}")
                            sql_muti_dict.append({"phrase": phrase, "position_list": sql_word_pos[phrase], "recall_list": positive_value_fu1, "columnId": final_columnId})
                        elif len(llm_result) == 1:
                            logger.info(f"--直接确定: {phrase} 为 {llm_result[0]}")
                            sql_muti_dict.append({"phrase": phrase, "position_list": sql_word_pos[phrase], "recall_list": llm_result, "columnId": final_columnId})
                        else:
                            logger.info(f"--进入多轮： {llm_result}")
                            sql_muti_dict.append({"phrase": phrase, "position_list": sql_word_pos[phrase], "recall_list": llm_result, "columnId": final_columnId})
    return sql_muti_dict


def llm_choose_where(phrases, where_list):
    prompt = f'''基于用户的输入词从全部值中选择出相关的值，如所有值都不能明确反应用户意图，则不选择任何值并返回空列表
用户的输入词为：{phrases}
全部值列表为：{where_list}
规则：
1.你选择的值可以是0个、1个或多个，但必须与用户输入直接相关。不要选择间接或模糊相关的值。
2.无法判断或没有合适的值时，返回空列表[]。'''

    # print(prompt)
    result = send_llm(prompt)
    result = find_max_list(result)
    final_list = [where_name for where_name in result if where_name in where_list]
    return final_list