import re

from config import CURRENT_SCENE
from config.log_config import logger
from knowledge_graph.get_metric_knowledge_graph import get_values_from_id
from match.completely_match import get_completely_match, get_completely_match_list2list
from match.jaccard_match import get_word_jaccard_match_max, get_where_jaccard_match, get_jaccard_match_list2list
from utils.json_util import extract_json_from_string
from utils.prompt import prompt_extract_exclude
from utils.request_util import send_llm
from utils.util import get_sorce_from_target, fuzzy_conflict_selection
from match.where_vector_match import get_where_word_vector_match, get_where_sentence_vector_match, \
    get_where_sentence_vector_match_relation, get_sql_where_vector_match


def extract_where(user_object, user_input):

    where_json = {}
    for metric, table_list in user_object.metric_table.items():
        for table_name in table_list:
            if "维度" in user_object.metric_knowledge_graph[metric][table_name].keys():
                current_where_json = user_object.metric_knowledge_graph[metric][table_name]["维度"]
                for k, v in current_where_json.items():
                    if k not in where_json.keys():
                        where_json[k] = v

    columnId2values = {info["columnId"]: info["values"] for columnName, info in where_json.items()}
    # logger.info(f"columnId2values：{columnId2values}")
    origin_where_json = user_object.all_metric_data["valueJson"]
    origin_columnId2values = {info["columnId"]: info["values"] for info in origin_where_json}
    columnId2columnName_relation = {}
    if len(user_object.relation_table_id) > 0:  # 将计算关联维度的where
        for info in origin_where_json:
            if info["tableId"] in user_object.relation_table_id:
                columnId2columnName_relation[info["columnId"]] = info["columnName"]

    all_columnId2columnName = {info["columnId"]: columnName for columnName, info in where_json.items()}
    for columnId, columnName in columnId2columnName_relation.items():
        if columnId not in all_columnId2columnName.keys():
            all_columnId2columnName[columnId] = columnName

    all_columnName2columnId = {columnName: info["columnId"] for columnName, info in where_json.items()}
    for columnId, columnName in columnId2columnName_relation.items():
        if columnName not in all_columnName2columnId.keys():
            all_columnName2columnId[columnName] = columnId

    multi_turn = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "where" in user_object.history[-2].keys():
            real_input = user_input
            multi_turn = True
        else:
            real_input = user_object.history[0]["user"]

    columnId_list = []
    if multi_turn:

        if "columnId" in user_object.history[-2].keys():  # 自身单个维度内的多个可能值多轮
            columnId = user_object.history[-2]["columnId"]
            # columnId2values 是自身的，这里没有记录关联维度的，所以不用，这里从原始数据重新获取一次
            match_values = get_values_from_id(user_object.all_metric_data, columnId)
            value_completely_match = get_completely_match(real_input, match_values)
            value_jaccard_match = get_word_jaccard_match_max(real_input, match_values)
            value_character_match = set()
            for word in value_completely_match + value_jaccard_match:
                value_character_match.add(word)

            value_character_match_list = list(value_character_match)
            if len(value_character_match_list) > 0:  # 这里取第一个
                value = value_character_match_list[0]
                exist_flag = False
                for info in user_object.slot_dict["where"]:
                    if value == info["value"]:
                        exist_flag = True
                        break

                if not exist_flag:
                    user_object.slot_dict["where"].append({"columnId": columnId, "columnName": all_columnId2columnName[columnId], "targetValue": value, "value": get_sorce_from_target(origin_columnId2values, columnId, value), "CompleteMatch": True})

            else:
                where_vector_match = get_where_word_vector_match(real_input, CURRENT_SCENE + '_weidu_' + str(columnId))
                if len(where_vector_match) == 1:
                    value = where_vector_match[0]
                    exist_flag = False
                    for info in user_object.slot_dict["where"]:
                        if value == info["value"]:
                            exist_flag = True
                            break

                    if not exist_flag:
                        user_object.slot_dict["where"].append({"columnId": columnId, "columnName": all_columnId2columnName[columnId], "targetValue": value, "value": get_sorce_from_target(origin_columnId2values, columnId, value), "CompleteMatch": True})

                if len(where_vector_match) > 1:
                    extract_where_result = {"result": '根据您的输入,有以下推荐,例如:' + ','.join(where_vector_match), "need_multi_turn": True, "columnId": columnId}
                    return extract_where_result

        elif "columnName2Id" in user_object.history[-2].keys():  # 多个关联维度中存在相同值来确定哪个维度
            columnName2Id = user_object.history[-2]["columnName2Id"]
            columnName2SourceValue = user_object.history[-2]["columnName2SourceValue"]
            targetValue = user_object.history[-2]["targetValue"]
            if real_input in columnName2Id.keys():
                user_object.slot_dict["where"].append(
                            {"columnId": columnName2Id[real_input], "columnName": real_input, "targetValue": targetValue,
                             "value": columnName2SourceValue[real_input], "CompleteMatch": True})

        elif "fuzzy_word" in user_object.history[-2].keys():  # 多个维度中存在相同模糊值来确定哪个维度
            fuzzy_word = user_object.history[-2]["fuzzy_word"]
            if real_input in all_columnName2columnId.keys():
                user_object.slot_dict["where"].append(
                    {"columnId": all_columnName2columnId[real_input], "columnName": real_input, "targetValue": fuzzy_word,
                     "value": fuzzy_word, "CompleteMatch": False})

    else:
        columnId_list = []
        extract_list = ['剔除', '除去', '排除在外', '摒弃', '忽略', '筛除', '避免', '清除', '消除', '不含', '不包含', '不包括' ]
        if any(word in user_input for word in extract_list):
            new_prompt_extract_exclude = prompt_extract_exclude.replace("{user_input}", real_input)
            extract_json_result = send_llm(new_prompt_extract_exclude)
            extract_json_result = extract_json_from_string(extract_json_result)
        else:
            extract_json_result = {'exclude': []}

        exclude_word_list = []
        if "exclude" in extract_json_result:
            if isinstance(extract_json_result["exclude"], list):
                if len(extract_json_result["exclude"]) > 0:
                    exclude_word_list = extract_json_result["exclude"]

        for where_name, where_info in where_json.items():
            if len(where_name) == 0:
                continue
            value_list = where_info["values"]
            columnId = where_info["columnId"]
            columnId_list.append(columnId)

            # value_jaccard_match = get_where_jaccard_match(user_object, value_list)
            value_jaccard_match = []

            exclude_value_completely_match = get_completely_match_list2list(exclude_word_list, value_list)
            exclude_value_jaccard_match = get_jaccard_match_list2list(exclude_word_list, value_list)
            exclude_value_character_match = set()
            for word in exclude_value_completely_match + exclude_value_jaccard_match:
                exclude_value_character_match.add(word)

            value_character_match = [info for info in value_jaccard_match if info["match_word"] not in exclude_value_completely_match]

            if len(value_character_match) > 0:  # 删除与维度名同名的group
                for group_json in user_object.slot_dict["group"]:
                    if where_name == group_json["columnName"]:
                        user_object.slot_dict["group"].remove(group_json)
                        break

            for info in value_character_match:
                user_object.slot_dict["where"].append({"columnId": where_info["columnId"], "columnName": where_name, "targetValue": info["match_word"], "value": get_sorce_from_target(origin_columnId2values, columnId, info["match_word"]), "CompleteMatch": True})
                user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrases"], "position_list": info["position_list"]})

            for value in exclude_value_character_match:
                user_object.slot_dict["exclude"].append({"columnId": where_info["columnId"], "columnName": where_name, "targetValue": value, "value": get_sorce_from_target(origin_columnId2values, columnId, value), "CompleteMatch": True})
                # user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrases"], "position_list": info["position_list"]})


            if columnId not in user_object.phrase_recall_info.keys():
                user_object.phrase_recall_info[columnId] = []

        columnId_list_relation = list(columnId2columnName_relation.keys())
        print(f"维度分词：{user_object.jieba_window_phrases2positionList.keys()}")

        # 当前指标自身维度的处理
        print("where-columnId_list: ", columnId_list)
        logger.info(f"自身维度列表：{columnId_list}")
        user_object.where_weidu_list = columnId_list
        user_object.self_where_weidu_list = columnId_list
        # columnId_list 可能为空，但也要执行get_where_sentence_vector_match，因为函数内部写了模糊处理，否则模糊处理不起作用
        user_object.phrase_recall_info, user_object.where_fuzzy_dict = get_where_sentence_vector_match(user_object, columnId_list, all_columnId2columnName, origin_columnId2values, columnId_list_relation)

        # 当前指标关联表维度的处理
        logger.info(f"关联维度列表：{columnId_list_relation}")
        user_object.where_weidu_list += columnId_list_relation
        if len(columnId_list_relation) > 0:
            user_object.relation_slot_dict = get_where_sentence_vector_match_relation(user_object, columnId_list_relation, all_columnId2columnName, origin_columnId2values)

    # -------------------- 当前指标自身维度的多轮
    mul_turn_info = {}
    multi_columnId = ''
    for columnId, recall_info_list in user_object.phrase_recall_info.items():
        if len(recall_info_list) > 0:
            mul_turn_info = recall_info_list[0]
            multi_columnId = columnId
            recall_info_list.pop(0)
            user_object.phrase_recall_info[columnId] = recall_info_list
            break

    if len(mul_turn_info) > 0:
        extract_where_result = {"result": '您提到的“' + mul_turn_info["phrases"] + '”具体是指哪一个呢？例如:' + ','.join(mul_turn_info["recall_list"]), "need_multi_turn": True, "columnId": multi_columnId}
        user_object.where_recognize_by_phrase_pos.append({"phrase": mul_turn_info["phrases"], "position_list": mul_turn_info["position_list"]})
        return extract_where_result

    # -------------------- 当前指标关联维度的多轮
    if len(user_object.relation_slot_dict) > 0:
        # 开始将 user_object.relation_slot_dict 的值放入 user_object.slot_dict["where"]
        # 1、relation_slot_dict 中去除所有 值在slot_dict出现的
        targetValue_list = [info["targetValue"] for info in user_object.slot_dict["where"]]
        relation_slot_dict = []
        for info in user_object.relation_slot_dict:
            if info["targetValue"] not in targetValue_list:
                relation_slot_dict.append(info)
        user_object.relation_slot_dict = relation_slot_dict

        # 2、relation_slot_dict 自身去除id和value都互相重复的，防止多轮询问同一维度
        relation_slot_dict = []
        for info in user_object.relation_slot_dict:
            if info not in relation_slot_dict:
                relation_slot_dict.append(info)
        user_object.relation_slot_dict = relation_slot_dict

        # 3、获取自身 value重复id不重复 的进入多轮推荐
        targetValue_list = []
        repeatValue_list = []
        for info in user_object.relation_slot_dict:
            if info["targetValue"] not in targetValue_list:
                targetValue_list.append(info["targetValue"])
            else:
                if info["targetValue"] not in repeatValue_list:
                    repeatValue_list.append({"targetValue": info["targetValue"], "position_list": info["position_list"]})

        logger.info(f"关联维度准备进入多轮：{repeatValue_list}")

        # 4、将剩下的不重复的放入 slot_dict
        for info in user_object.relation_slot_dict:
            if info["targetValue"] not in [rep["targetValue"] for rep in repeatValue_list]:
                logger.info(f"关联维度准备放入where槽位：{info}")
                user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrases"], "position_list": info["position_list"]})
                fuzzy_value_exist, fuzzy_recommend_exist, user_object.where_fuzzy_dict = fuzzy_conflict_selection(user_object, user_object.where_fuzzy_dict, info["phrases"])
                if not fuzzy_value_exist and not fuzzy_recommend_exist:
                    user_object.slot_dict["where"].append({"columnId": info["columnId"], "columnName": info["columnName"], "targetValue": info["targetValue"], "value": info["value"], "CompleteMatch": True})
                    logger.info(f"放入where槽位成功")
                else:
                    logger.info(f"放入where槽位失败")

        # 5、重复的构造多轮问答对
        for rep in repeatValue_list:
            columnName2Id = {}
            columnName2SourceValue = {}
            for info in user_object.relation_slot_dict:
                if info["targetValue"] == rep["targetValue"] and info["columnName"] not in columnName2Id.keys():
                    columnName2Id[info["columnName"]] = info["columnId"]
                    columnName2SourceValue[info["columnName"]] = info["value"]

            fuzzy_value_exist, fuzzy_recommend_exist, user_object.where_fuzzy_dict = fuzzy_conflict_selection(user_object, user_object.where_fuzzy_dict, rep["targetValue"])
            if not fuzzy_value_exist and not fuzzy_recommend_exist:
                mul_turn_q = {"result": '有多个不同的维度包含“' + rep["targetValue"] + '”，您想关注哪一个呢？:' + ','.join(list(columnName2Id.keys())), "need_multi_turn": True, "columnName2Id": columnName2Id, "columnName2SourceValue": columnName2SourceValue, "targetValue": rep["targetValue"]}
                user_object.where_recognize_by_phrase_pos.append({"phrase": rep["targetValue"], "position_list": rep["position_list"]})
                user_object.where_relation_mul_turn.append(mul_turn_q)

    if len(user_object.where_relation_mul_turn) > 0:
        extract_where_result = user_object.where_relation_mul_turn[0]
        user_object.where_relation_mul_turn.pop(0)
        return extract_where_result

    # -------------------- where的模糊值来源于多维度的多轮
    if len(user_object.where_fuzzy_dict) > 0:
        where_name_list = [all_columnId2columnName[id] for id in user_object.where_fuzzy_dict[0]["columnIdList"]]
        extract_where_result = {"result": '有多个不同的维度包含“' + user_object.where_fuzzy_dict[0]["phrases"] + '”，您想关注哪一个呢？:' + ','.join(where_name_list),
                                "need_multi_turn": True, "fuzzy_word": user_object.where_fuzzy_dict[0]["phrases"]}
        user_object.where_recognize_by_phrase_pos.append(
            {"phrase": user_object.where_fuzzy_dict[0]["phrases"], "position_list": user_object.where_fuzzy_dict[0]["position_list"]})
        user_object.where_fuzzy_dict.pop(0)
        return extract_where_result

    if len(user_object.where_weidu_list) > 0:
        sql_muti_dict = get_sql_where_vector_match(user_object, CURRENT_SCENE, all_columnId2columnName)
        user_object.where_weidu_list = []  # 表示当前代码段只执行一次
        for info in sql_muti_dict:
            if len(info["recall_list"]) == 1:  # 直接确定
                targetValue = info["recall_list"][0]
                user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrase"], "position_list": info["position_list"]})
                user_object.slot_dict["where"].append({"columnId": info["columnId"],
                                                       "columnName": all_columnId2columnName[info["columnId"]],
                                                       "targetValue": targetValue,
                                                       "value": get_sorce_from_target(origin_columnId2values, info["columnId"], targetValue),
                                                       "CompleteMatch": True})
                logger.info(f"--通过sql识别到 {info['phrase']} 为 {targetValue} 放入where槽位成功")

            if len(info["recall_list"]) > 1:
                pattern = '(?:[^\w\s、，,。！？]|[0-9]+|[a-zA-Z]+)+'
                matches = re.findall(pattern, info["phrase"])  # 对于编号类的要特殊考虑
                recommend_flag = True
                if len(matches) == 1:
                    if matches[0] == info["phrase"]:
                        targetValue = info["recall_list"][0] # 第一个是概率最大的，只需要判断和第一个一致即可
                        if info["phrase"] == targetValue:
                            user_object.slot_dict["where"].append({"columnId": info["columnId"], "columnName": all_columnId2columnName[info["columnId"]], "targetValue": targetValue,
                                                                   "value": get_sorce_from_target(origin_columnId2values, info["columnId"], targetValue), "CompleteMatch": True})
                            logger.info(f"--通过特殊编号类数据识别到 {info['phrase']} 为 {targetValue} 放入where槽位成功")
                            recommend_flag = False


                if recommend_flag:
                    extract_where_result = {"result": '您提到的“' + info["phrase"] + '”具体是指哪一个呢？您可以继续输入或从下面进行选择，例如:' + ','.join(info["recall_list"] + ['无']), "need_multi_turn": True, "columnId": info["columnId"]}
                    user_object.where_recognize_by_phrase_pos.append({"phrase": info["phrase"], "position_list": info["position_list"]})
                    user_object.where_relation_mul_turn.append(extract_where_result)  # 借用where_relation_mul_turn实现多轮

    if len(user_object.where_relation_mul_turn) > 0:
        extract_where_result = user_object.where_relation_mul_turn[0]
        user_object.where_relation_mul_turn.pop(0)
        return extract_where_result

    extract_where_result = {"result": user_object.slot_dict["where"], "need_multi_turn": False}
    return extract_where_result