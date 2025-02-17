from config import CURRENT_SCENE
from match.completely_match import get_completely_match
from match.jaccard_match import get_timeType_jaccard_match, get_jaccard_match
from match.metric_vector_match import get_metric_word_vector_match
from match.timeType_vector_match import get_timeType_vector_match
from utils.util import remove_list_substring


def extract_timeType(user_object, user_input):

    time_name_list = []
    for metric, table_list in user_object.metric_table.items():
        for table_name in table_list:
            if "time" in user_object.metric_knowledge_graph[metric][table_name].keys():
                time_json = user_object.metric_knowledge_graph[metric][table_name]["time"]
                for info in time_json:
                    if info["columnName"] not in time_name_list:
                        time_name_list.append(info["columnName"])

    mul_turn_flag = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "timeType" in user_object.history[-2].keys():
            real_input = user_input
            mul_turn_flag = True
        else:
            real_input = user_object.history[0]["user"]

    if not mul_turn_flag:
        timeType_character_match = get_timeType_jaccard_match(user_object, time_name_list)
    else:
        metric_completely_match = get_completely_match(real_input, time_name_list)
        metric_jaccard_match = get_jaccard_match(real_input, time_name_list)
        timeType_character_match = set()  # 完全匹配+字形相似度同城字形匹配
        for word in metric_completely_match + metric_jaccard_match:
            timeType_character_match.add(word)

    for timeType in timeType_character_match:
        if timeType not in user_object.slot_dict["timeType"]:
            user_object.slot_dict["timeType"].append(timeType)  # 字形匹配到的都认为是timeType，更新到槽位

    origin_timeType_vector_match = []
    if real_input not in timeType_character_match:  # 当输入是一个完整指标，不需要再用向量
        if not mul_turn_flag:
            origin_timeType_vector_match = get_timeType_vector_match(user_object, CURRENT_SCENE + "_time")
        else:
            origin_timeType_vector_match, no_threshold_match = get_metric_word_vector_match(real_input, CURRENT_SCENE+"_time")

    timeType_vector_match = []
    for word in origin_timeType_vector_match:
        if word in time_name_list:
            timeType_vector_match.append(word)

    if len(timeType_character_match) == 0:  # 字形匹配为空
        if len(timeType_vector_match) != 0:  # 语义相似度有值
            if len(timeType_vector_match) == 1:  # 语义相似度仅有一个值，直接选择该值为指标
                if timeType_vector_match[0] not in user_object.slot_dict["timeType"]:
                    user_object.slot_dict["timeType"].append(timeType_vector_match[0])
                extract_timeType_result = {"result": user_object.slot_dict["timeType"], "need_multi_turn": False}
            else:  # 语义相似度有多个值则进入推荐
                if len(user_object.slot_dict["timeType"]) == 0:
                    extract_timeType_result = {"result": "您想查询哪种时间类型呢？根据问题提供以下时间类型供您选择:" + ','.join(timeType_vector_match), "need_multi_turn": True}
                else:
                    extract_timeType_result = {"result": '您除了想查询时间类型：(' + ', '.join(user_object.slot_dict["timeType"]) + ')，还有其他想查询的时间类型吗？:无,' + ','.join(timeType_vector_match), "need_multi_turn": True}
        else:  # 语义相似度为空
            extract_timeType_result = {"result": user_object.slot_dict["timeType"], "need_multi_turn": False}

    else:  # 字形匹配有值

        user_object.slot_dict["timeType"] = remove_list_substring(user_object.slot_dict["timeType"])  # 去除冗余子串指标
        if len(timeType_vector_match) != 0:  # 语义相似度有值，下面去除字形匹配的结果
            metric_new_match = [metric for metric in timeType_vector_match if metric not in timeType_character_match]
            if len(metric_new_match) == 0:  # 语义相似度没有新值
                extract_timeType_result = {"result": user_object.slot_dict["timeType"], "need_multi_turn": False}
            elif len(metric_new_match) == 1:  # 语义相似度有1个新值，直接选择该值为指标
                if metric_new_match[0] not in user_object.slot_dict["timeType"]:
                    user_object.slot_dict["metric"].append(metric_new_match[0])
                extract_timeType_result = {"result": user_object.slot_dict["timeType"], "need_multi_turn": False}
            else:  # 语义相似度有多个新值，进入多轮推荐
                extract_timeType_result = {"result": '您除了想查询时间类型：(' + ', '.join(user_object.slot_dict["timeType"]) + ')，还有想查询其他时间类型吗？:无,' + ','.join(metric_new_match), "need_multi_turn": True}
        else:
            extract_timeType_result = {"result": user_object.slot_dict["timeType"], "need_multi_turn": False}

    return extract_timeType_result
