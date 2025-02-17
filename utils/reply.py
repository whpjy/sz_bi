import asyncio

from config.log_config import logger
from extract_information.extract_timeRelationship import extract_relationship
from utils.json_util import write_indicator_frequency
from utils.recommend_question import recommend_question
from utils.reply_util import get_target_list
from utils.rule_word import where_filter_group


def unrelated_intent_reply(answer):
    return {
        "type": 2,
        "context": answer,
        "mult": [],
        "history": []
    }


def function_intent_reply(answer):
    relevantIndicator = []
    if isinstance(answer, dict):
        if "relevantIndicator" in answer.keys():
            if isinstance(answer["relevantIndicator"], list):
                relevantIndicator = answer["relevantIndicator"]

    if len(relevantIndicator) > 0:
        context = ''
        for info in relevantIndicator:
            context = context + info["targetName"] + "\n"

        answer = context

    return {
        "type": 2,
        "context": answer,
        "mult": [],
        "metric_selection": False,
        "history": [],
        "relevantIndicator": relevantIndicator,
        "needTime": False
    }


def error_reply():
    return {
        "type": 2,
        "context": "正在维护中",
        "mult": [],  # mult 字段为空列表
        "history": []
    }


def multi_reply(user_object, current_scene="other", table_describe=None):
    context = list(user_object.history[-1].values())[0]
    colon_pos = context.find(':')
    if "找不到包含指标" in context:
        user_object.history = []
    if colon_pos == -1:
        return {
            "type": 2,
            "context": context,
            "mult": [],  # mult 字段为空列表
            "metric_selection": False,
            "history": user_object.history,
            "relevantIndicator": [],
            "needTime": False,
            "stream": False
        }

    intro_text = context[:colon_pos].strip()
    expenditures_str = context[colon_pos + 1:].strip()

    expenditure_measurement = expenditures_str.split(',')

    # 构建 mult 字段的值
    if current_scene == "table" and table_describe is not None:
        mult_dict = [
            {"prompt": intro_text},
            {"expenditures": [{"name": item.strip(),
                               "describle": table_describe[item.strip()] if item.strip() in table_describe.keys() else ""}
                              for item in expenditure_measurement]}
        ]
    else:
        mult_dict = [
            {"prompt": intro_text},
            {"expenditures": [{"name": item.strip()} for item in expenditure_measurement]}
        ]

    metric_selection = False
    if current_scene == "metric":
        metric_selection = True

    needTime = False
    if current_scene == "time":
        needTime = True

    return {
        "type": 2,
        "context": intro_text,
        "mult": mult_dict,
        "metric_selection": metric_selection,
        "history": user_object.history,
        "relevantIndicator": [],
        "needTime": needTime,
        "stream": False
    }


def final_reply(user_object):
    asyncio.create_task(write_indicator_frequency(list(user_object.metric_table.keys())))
    target_list = get_target_list(user_object)

    timeType_list = []
    for timeType in user_object.slot_dict["timeType"]:
        if timeType in user_object.time_name2id_dict.keys():
            timeType_list.append({"targetId": user_object.time_name2id_dict[timeType], "targetName": timeType})

    # 如果where的同一维度有多个值，该维度要放到group中
    for where in user_object.slot_dict["where"]:
        new_group = {'columnId': where['columnId'], 'columnName': where['columnName']}
        count = 0
        for new_where in user_object.slot_dict["where"]:
            if where['columnName'] == new_where['columnName']:
                count += 1
        if count > 1 and new_group not in user_object.slot_dict["group"]:
            user_object.slot_dict["group"].append(new_group)

    # 如果存在了某些where，则可能要舍弃一些group
    for where_name, group_name in where_filter_group.items():
        where_name_list = [info["columnName"] for info in user_object.slot_dict["where"]]
        group_name_list = [info["columnName"] for info in user_object.slot_dict["group"]]
        if where_name in where_name_list and group_name in group_name_list:
            for group_json in user_object.slot_dict["group"]:
                if group_name == group_json["columnName"]:
                    user_object.slot_dict["group"].remove(group_json)

    # 对where去重
    where_name_list = [{'columnId': info['columnId'], 'columnName': info['columnName'], 'targetValue': info['targetValue'],
                        'value': info['value'], 'CompleteMatch': info['CompleteMatch']} for info in user_object.slot_dict["where"]]
    user_object.slot_dict["where"] = []
    for info in where_name_list:
        if info not in user_object.slot_dict["where"]:
            user_object.slot_dict["where"].append(info)

    # 判断group能否保留，where维度值存在一个则不要对应的group值，其他不存在where或同一where有多个值的情况保留
    final_group_list = []
    for group_info in user_object.slot_dict["group"]:
        value_count = 0
        for info in user_object.slot_dict["where"]:
            if group_info["columnName"] == info["columnName"]:
                value_count += 1
        if value_count != 1:
            final_group_list.append({"columnId": group_info["columnId"], "columnName": group_info["columnName"]})
    user_object.slot_dict["group"] = final_group_list

    # 注意where是模糊值的时候，where维度值有一个也要在group中应该要有对应值
    # 即存在where模糊值时，group也要有值
    for info in user_object.slot_dict["where"]:
        if not info["CompleteMatch"]: # 说明是模糊匹配，去group看下是否存在
            group_exist = False
            for group_info in user_object.slot_dict["group"]:
                if info["columnId"] == group_info["columnId"]:
                    group_exist = True
            if not group_exist:
                user_object.slot_dict["group"].append({"columnId": info["columnId"], "columnName": info["columnName"]})

    # 判断 time 有多个时候，是 and 还是 or
    timeRelationship = []
    if len(user_object.slot_dict["time"]) > 1:
        timeRelationship = extract_relationship(user_object)

    # 根据当前信息做问题推荐
    problemRecommendation = recommend_question(user_object)

    test_display_dict = {
        "type": user_object.slot_dict["intent"][0],
        "value": {
            "analyzeType": "指标分析",
            "target": target_list,
            "time":  user_object.slot_dict["time"],
            "timeType": timeType_list,
            "timeRelationship": timeRelationship,
            "group":  user_object.slot_dict["group"],
            "where":  user_object.slot_dict["where"],
            "compare": user_object.slot_dict["compare"],
            "proportion": user_object.slot_dict["proportion"],
            "rank": user_object.slot_dict["rank"],
            "limits": user_object.slot_dict["limits"],
            "exclude": user_object.slot_dict["exclude"]
        }
    }

    return {
        "type": user_object.slot_dict["intent"][0],
        "value": {
            "analyzeType": "指标分析",
            "target": target_list,
            "time":  user_object.slot_dict["time"],
            "timeType": timeType_list,
            "timeRelationship": timeRelationship,
            "group":  user_object.slot_dict["group"],
            "where":  user_object.slot_dict["where"],
            "compare": user_object.slot_dict["compare"],
            "rank": user_object.slot_dict["rank"],
            "limits": user_object.slot_dict["limits"],
            "exclude": user_object.slot_dict["exclude"],
            "problemRecommendation": problemRecommendation
        },
        "history": [],
        "context": ""
    }

    # return {
    #     "type": user_object.slot_dict["intent"][0],
    #     "value": {
    #         "analyzeType": "指标分析",
    #         "target": target_list + [],
    #         "time":  user_object.slot_dict["time"],
    #         "timeType": timeType_list,
    #         "timeRelationship": timeRelationship,
    #         "group":  user_object.slot_dict["group"],
    #         "where":  user_object.slot_dict["where"],
    #         "compare": user_object.slot_dict["compare"],
    #         "rank": user_object.slot_dict["rank"],
    #         "limits": user_object.slot_dict["limits"],
    #         "exclude": user_object.slot_dict["exclude"],
    #         "problemRecommendation": problemRecommendation
    #     },
    #     "history": [],
    #     "context": test_display_dict,
    #     "slot_status": user_object.slot_dict
    # }
