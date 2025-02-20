import json
import numpy as np

from bi_agent.agent_functions_describe import agent_reply, agent_reply_done
from config.log_config import logger
from extract_information.extract_sql import extract_sql
from utils.json_util import find_max_list, query_subject_change, deal_recommendation_recognition
from utils.request_util import send_llm, send_llm_stream, send_llm_system, send_llm_system_stream
from utils.time_util import extract_dates_time_control, judge_input_time, get_current_year, get_last_year


def get_clear_slot_dict():
    fr = open('config/slot_parameters.json', 'r', encoding='utf-8')
    slot_dict = json.load(fr)
    return slot_dict


def get_metric_name_list(data_dict):
    zhibiao_list = []
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        del_word_list = ['最大值', '最小值', '平均', '合计']

        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    targetName = targetName.strip().split('-')[-1]
                    for del_word in del_word_list:
                        if del_word == targetName[-len(del_word):]:
                            targetName = targetName.replace(del_word, '')
                    if targetName not in zhibiao_list:
                        zhibiao_list.append(targetName)
    return zhibiao_list


def get_target_name_list(data_dict):
    target_name_list = []
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    if targetName not in target_name_list:
                        target_name_list.append(targetName)

    return target_name_list


def remove_list_substring(slot_list):
    final_list = []
    for word1 in slot_list:
        reserve_flag = True
        for word2 in slot_list:
            if word1 in word2 and word1 != word2:
                reserve_flag = False
        if reserve_flag:
            final_list.append(word1)

    return final_list


def get_metrc2id(data_dict):
    metrc2id = {}
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys() and "targetId" in zhibiao.keys():
                    metrc2id[zhibiao["targetName"]] = zhibiao["targetId"]

    return metrc2id


def get_metric_type(data_dict):
    metric_type = {}
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys() and "targetId" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    targetName_list = targetName.split('-')
                    if len(targetName_list) != 2 or "targetType" not in zhibiao.keys():
                        continue
                    metric = targetName_list[1].replace("最大值", "").replace("最小值", "").replace("平均值", "").replace("合计", "")
                    if len(metric) == 0:
                        continue
                    if zhibiao["targetType"] == 1:
                        metric_type[metric] = "原子指标"
                    elif zhibiao["targetType"] == 2:
                        metric_type[metric] = "派生指标"
                    elif zhibiao["targetType"] == 3:
                        metric_type[metric] = "组合指标"
                    else:
                        metric_type[metric] = "其他"

    return metric_type


def get_time_name2id_dict(data_dict):
    time_name2id = {}
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "time" in zhibiao.keys():
                    for i in range(0, len(zhibiao["time"])):
                        if len(zhibiao["time"][i]["columnName"].strip()) > 0:
                            time_name2id[zhibiao["time"][i]["columnName"]] = zhibiao["time"][i]["columnId"]

    return time_name2id


def get_table_map(data_dict):

    table_id2name = data_dict["tableMap"]
    table_name2id = {}
    for id, name in table_id2name.items():
        table_name2id[name] = id

    return table_name2id, table_id2name


def get_table_relation(data_dict):
    table_relation = []
    tableRelationsMap = data_dict["tableRelationsMap"]
    for tableId, tableRelationsList in tableRelationsMap.items():
        if len(table_relation) == 0:
            table_relation.append(tableRelationsList)
        else:
            exist_flag = False
            for i in range(0, len(table_relation)):
                intersection_list = [value for value in tableRelationsList if value in table_relation[i]]
                if len(intersection_list) > 0:
                    exist_flag =True
                    for value in tableRelationsList:
                        if value not in table_relation[i]:
                            table_relation[i].append(value)
                    break

            if not exist_flag:
                table_relation.append(tableRelationsList)

    return table_relation


def update_tableRelation(user_object, tableRelation, tableId):
    new_tableRelation = tableRelation
    for relation_list in user_object.table_relation:
        if tableId in relation_list:
            for id in relation_list:
                if id not in new_tableRelation:
                    new_tableRelation.append(id)
            break

    return new_tableRelation


def rewrite_input_for_group(user_object, real_input, group_name_list):
    for word in user_object.metric_recognize_by_phrase:
        real_input = real_input.replace(word, ' ')

    # exist_mingcheng_list = []
    # for group_name in group_name_list:
    #     if "名称" in group_name and "手术" not in group_name:
    #         exist_mingcheng_list.append(group_name)
    #
    # for exist_name in exist_mingcheng_list:
    #     prefix_group_exist_flag = False
    #     prefix_name = exist_name.replace("名称", "")
    #     for group_name in group_name_list:
    #         if prefix_name in group_name and group_name in real_input:
    #             prefix_group_exist_flag = True
    #
    #     # 到此表明，group_name_list存在xx开的的名称，并且xx开头的所有分组条件都不在（包括xx名称）
    #     if not prefix_group_exist_flag:
    #         real_input = real_input.replace(prefix_name, exist_name)
    #
    # exist_xingming_list = []
    # for group_name in group_name_list:
    #     if "姓名" in group_name:
    #         exist_xingming_list.append(group_name)
    #
    # for exist_name in exist_xingming_list:
    #     prefix_group_exist_flag = False
    #     prefix_name = exist_name.replace("姓名", "")
    #     for group_name in group_name_list:
    #         if prefix_name in group_name and group_name in real_input:
    #             prefix_group_exist_flag = True
    #
    #     # 到此表明，group_name_list存在xx开的的名称，并且xx开头的所有分组条件都不在（包括xx名称）
    #     if not prefix_group_exist_flag:
    #         real_input = real_input.replace(prefix_name, exist_name)

    return real_input


def get_position(group_character_match, origin_input):
    group_character_match_add_position = []
    for info in group_character_match:
        phrases = info["phrases"]
        recall_word = info["recall_word"]
        max_score = info["max_score"]
        start_position = origin_input.find(phrases)
        new_dict = {"phrases": phrases, "recall_word": recall_word, "columnId": info["columnId"], "max_score": max_score,
                        "position_list": [i for i in range(start_position, start_position + len(phrases))]}
        group_character_match_add_position.append(new_dict)

    return group_character_match_add_position


def get_sorce_from_target(columnId2values, columnId, target_value):
    if columnId in columnId2values.keys():
        values = columnId2values[columnId]
        for info in values:
            if info["targetValue"] == target_value:
                return info["sourceValue"]

    return target_value


def judge_group_name_exist(user_object, new_group_name):
    '''
    判断新获取的名称是否已经存在
    '''

    exist_flag = False
    group_result = user_object.slot_dict["group"]
    for info in group_result:
        if new_group_name == info["columnName"]:
            exist_flag = True
            break

    return exist_flag


def cal_start_pos(pre_label_input, phrase):
    '''
    通过label召回group词后计算label的起始位置，方便对group进行插入排序
    '''
    try:
        start_pos = pre_label_input.find(phrase)
    except:
        start_pos = len(pre_label_input)

    return start_pos


def insert_group(group_dict_list, new_dict):
    print("当前：", group_dict_list)
    print("插入：", new_dict)
    '''
    group_dict_list 本身为有序
    根据start_pos顺序, 对 new_dict 进行插入到 group_dict_list
    '''

    # 1、根据 new_dict 的 start_pos、phrase、max_score进行 group_dict_list 的过滤
    # 即 label 召回可能和已有的冲突，根据冲突进行分析选择
    insert_flag = True
    overlap_group_list = []
    if new_dict["start_pos"] > -1:
        new_position_list = [new_dict["start_pos"] + i for i in range(0, len(new_dict["phrases"]))]
        for group_info in group_dict_list:
            current_position_list = [group_info["start_pos"] + i for i in range(0, len(group_info["phrases"]))]
            intersection_list = [value for value in current_position_list if value in new_position_list]
            if len(intersection_list) != 0:  # 发现冲突
                if new_dict["max_score"] > group_info["max_score"]:
                    overlap_group_list.append(group_info)  # 舍弃group_info
                else:
                    insert_flag = False
                break

    # 2、删除
    if len(overlap_group_list) > 0:
        for info in overlap_group_list:
            group_dict_list.remove(info)

    # 3、插入
    if insert_flag:
        # 2、按序插入
        new_start_pos = new_dict["start_pos"]
        if new_start_pos == -1:
            new_start_pos = len(group_dict_list)
        index = 0
        for group_info in group_dict_list:
            if "start_pos" in group_info.keys():
                if group_info["start_pos"] > new_start_pos:
                    break
            index += 1
        new_group_dict_list = group_dict_list[:index] + [new_dict] + group_dict_list[index:]
    else:
        new_group_dict_list = group_dict_list

    return new_group_dict_list


def get_group_label_dict(group_json_list):
    group_label_dict = {}
    for type_dict in group_json_list:
        columnName = type_dict["columnName"]
        # print("columnName", columnName)
        if "labels" in type_dict.keys():
            # print("labels", type_dict["labels"])
            for label in type_dict["labels"]:
                if label in group_label_dict.keys():
                    if columnName not in group_label_dict[label]:
                        group_label_dict[label].append(columnName)
                else:
                    group_label_dict[label] = [columnName]

    return group_label_dict


def get_metric_table_dict(user_object):
    metric_table_dict = {}
    for metric in user_object.slot_dict["metric"]:
        print(metric)
        all_table_name_list = list(user_object.metric_knowledge_graph[metric].keys())

        # 组合指标即targetType=3的优先级最高
        table_name_list = []
        for table_name in all_table_name_list:
            if user_object.metric_knowledge_graph[metric][table_name]["targetType"] == 3:
                table_name_list.append(table_name)

        if len(table_name_list) == 0:
            table_name_list = all_table_name_list

        for table_name in table_name_list:
            tableId = -1
            tableRelation = {}

            if len(user_object.metric_knowledge_graph[metric][table_name]["分组条件"]) > 0:
                if "tableId" in user_object.metric_knowledge_graph[metric][table_name]["分组条件"][0].keys():
                    tableId = user_object.metric_knowledge_graph[metric][table_name]["分组条件"][0]["tableId"]
                    if str(tableId) in user_object.all_metric_data["tableRelationsMap"].keys():
                        tableRelation = user_object.all_metric_data["tableRelationsMap"][str(tableId)]

            if metric not in metric_table_dict.keys():
                metric_table_dict[metric] = [{"tableName": table_name, "tableId": tableId, "tableRelation": tableRelation}]

            else:
                new_dict = {"tableName": table_name, "tableId": tableId, "tableRelation": tableRelation}
                if new_dict not in metric_table_dict[metric]:
                    metric_table_dict[metric].append(new_dict)
    return metric_table_dict


def get_new_metric_table_dict(metric_table_dict):
    metric_count = len(metric_table_dict)
    table_count_dict = {}
    for k, v in metric_table_dict.items():
        for info in v:
            if info['tableId'] not in table_count_dict.keys():
                table_count_dict[info['tableId']] = 1
            else:
                table_count_dict[info['tableId']] += 1

    reserve_tableId_list = [tableid for tableid, count in table_count_dict.items() if count >= metric_count]

    new_metric_table_dict = {}
    for k, v in metric_table_dict.items():
        new_metric_table_dict[k] = []  # 默认先给空列表，后续根据存在为空来来判断没有共同业务域
        for info in v:
            if info["tableId"] in reserve_tableId_list:
                new_metric_table_dict[k].append({"tableName": info["tableName"], "tableId": info["tableId"]})

    return new_metric_table_dict


def get_table_name_list(new_metric_table_dict):
    table_name_list = []
    for k, v in new_metric_table_dict.items():
        for info in v:
            if info["tableName"] not in table_name_list:
                table_name_list.append(info["tableName"])

    return table_name_list


def get_table_list_from_history(ask_question):

    context = ask_question
    colon_pos = context.find(':')

    if colon_pos == -1:
        return []

    expenditures_str = context[colon_pos + 1:].strip()
    expenditure_measurement = expenditures_str.split(',')
    expenditures = [item.strip() for item in expenditure_measurement]

    return expenditures


def get_table_attribute_list(user_object):
    table_attribute_list = []
    for metric, table_list in user_object.metric_table.items():
        for tabe_name in table_list:
            current_attribute_list = list(user_object.metric_knowledge_graph[metric][tabe_name].keys())
            for attribute in current_attribute_list:
                if attribute not in table_attribute_list:
                    table_attribute_list.append(attribute)

    return table_attribute_list


def get_best_group(user_object, recall_word, groupList):

    user_input = user_object.history[0]["user"]
    tableNameList = [user_object.table_id2name[str(info["tableId"])] for info in groupList]

    prompt = f'''用户输入文本：{user_input}
已知存在{len(tableNameList)}张数据表：{"、".join(tableNameList)}
每张数据表都同时包含了“{recall_word}”这个维度
请结合用户的输入文本和“{recall_word}”这个维度，选择一张最佳的数据表
一定存在一个数据表是最合适的，请从 {"、".join(tableNameList)} 中进行选择
不要回答其他内容，直接输出唯一的数据表名即可
'''
    print(prompt)
    result = send_llm(prompt)
    print("LLM: ", result)
    for tableName in tableNameList:
        if tableName in result:
            tableId = int(user_object.table_name2id[tableName])
            for info in groupList:
                if info["tableId"] == tableId:
                    return info

    return groupList[0]


def get_keshi_bianma_name_id(user_object):

    group_json_list = []
    for metric, table_list in user_object.metric_table.items():
        for table_name in table_list:
            if "分组条件" in user_object.metric_knowledge_graph[metric][table_name].keys():
                current_group_json_list = user_object.metric_knowledge_graph[metric][table_name]["分组条件"]
                for info in current_group_json_list:
                    if info not in group_json_list:
                        group_json_list.append(info)

    keshi_bianma_list = []
    for group_json in group_json_list:
        if "columnName" in list(group_json.keys()):
            columnName = group_json["columnName"]
            columnId = group_json["columnId"]
            if columnName[-4:] == "科室编码" and len(columnName) > 4:
                keshi_bianma_list.append({"columnName": columnName, "columnId": columnId})

    return keshi_bianma_list


def judge_where_group_name_exist(user_object, new_group_name):
    '''
    判断where中的维度名称是否已经存在
    '''

    exist_flag = False
    group_result = user_object.slot_dict["where"]
    for info in group_result:
        if new_group_name == info["columnName"]:
            exist_flag = True
            break

    return exist_flag



def get_user_history_prompt(user_history):
    user_history_str=""
    for it in user_history:
        for key,value in it.items():
            user_history_str=user_history_str+key+":"+str(value)+"\n"
    prompt=f'''### 用户历史对话：
{user_history_str}


### 输出：
'''
    return prompt


def get_data_fenxi(history, request_data):
    measures = request_data["measures"]
    measure_name_list = []
    data=""
    if len(measures) > 0:
        measure_name_list = [measure_name.split('-')[-1] for measure_name in measures]
        data = data + "分析的主题是：" + str(measure_name_list) + "\n"

    time = request_data["time"]
    if len(time) > 0:
        data = data + "时间范围：" + str(time[0]) + "\n"

    group = request_data["group"]
    if len(group) > 0:
        group_name_list = [info["columnName"] for info in group]
        data = data + "分组条件：" + str(group_name_list) + "\n"

    demisions = request_data["demisions"]
    demisions_name_list = []
    if len(demisions) > 0:
        demisions_name_list = [info["dimesionName"] for info in demisions]

    data_all = request_data["data_all"]
    data = data + "我将按照：" + str(demisions_name_list + measure_name_list) + " 的形式为你提供所有数据\n"
    data = data + "所有数据：" + str(data_all) + " \n"
    prompt = get_user_history_prompt(history)

    system_prompt = f"""### 角色能力：
你是一个数据分析助手，负责根据用户历史对话和数据库的数据，给出对用户问题的回复以及对数据的分析。


### 任务描述：
- 回复用户问题：根据用户历史对话和数据库的数据，生成对用户问题的回复。
- 数据分析：对数据库的数据进行分析,生成分析内容。


### 数据库的数据：
{data}

### 输出要求：
- 根据任务描述，你需要首先回复用户问题，然后进行数据分析，并将其整合成一段完整的话，不要使用分段标记。
- 输出内容不要包含“根据提供的数据”及类似的表述。
- 如果数据不完整，输出内容不要包含“建议提供完整数据”、“请提供更多详细信息”、“请提供更详细的数据”及类似的表述。
"""
    # print(f"system_prompt:{system_prompt}")
    # print(f"user_prompt:{prompt}")
    print(f"data:\n{data}")
    response = send_llm_system(system_prompt,prompt)
    return response


def get_data_fenxi_stream(history, request_data):
    measures = request_data["measures"]
    measure_name_list = []
    data=""
    if len(measures) > 0:
        measure_name_list = [measure_name.split('-')[-1] for measure_name in measures]
        data = data + "分析的主题是：" + str(measure_name_list) + "\n"

    time = request_data["time"]
    if len(time) > 0:
        data = data + "时间范围：" + str(time[0]) + "\n"

    group = request_data["group"]
    if len(group) > 0:
        group_name_list = [info["columnName"] for info in group]
        data = data + "分组条件：" + str(group_name_list) + "\n"

    demisions = request_data["demisions"]
    demisions_name_list = []
    if len(demisions) > 0:
        demisions_name_list = [info["dimesionName"] for info in demisions]

    data_all = request_data["data_all"]
    data = data + "我将按照：" + str(demisions_name_list + measure_name_list) + " 的形式为你提供所有数据\n"
    data = data + "所有数据：" + str(data_all) + " \n"
    prompt = get_user_history_prompt(history)

    system_prompt = f"""### 角色能力：
你是一个数据分析助手，负责根据用户历史对话和数据库的数据，给出对用户问题的回复以及对数据的分析。


### 任务描述：
- 回复用户问题：根据用户历史对话和数据库的数据，生成对用户问题的回复。
- 数据分析：对数据库的数据进行分析,生成分析内容。


### 数据库的数据：
{data}

### 输出要求：
- 根据任务描述，你需要首先回复用户问题，然后进行数据分析，并将其整合成一段完整的话，不要使用分段标记。
- 输出内容不要包含“根据提供的数据”及类似的表述。
- 如果数据不完整，输出内容不要包含“建议提供完整数据”、“请提供更多详细信息”、“请提供更详细的数据”及类似的表述。
- 如果询问整年的数据只提供了一个数据，并不是指是1月的数据，也不是各个月都是相同的数据，提供的就是整体的数据
"""
    pre_chunk = ''
    for chunk in send_llm_system_stream(system_prompt, prompt):
        if pre_chunk != chunk:
            pre_chunk = chunk
            data = agent_reply(pre_chunk)
            yield json.dumps(data, ensure_ascii=False).encode('utf-8') + b'\n\n\n'

    data = agent_reply_done(pre_chunk)
    yield json.dumps(data, ensure_ascii=False).encode('utf-8') + b'\n\n\n'
# def get_data_fenxi(history, request_data):
#
#     prompt = '你是一个数据分析大师，请对供给你结构化数据进行分析。\n'
#
#     measures = request_data["measures"]
#     measure_name_list = []
#     if len(measures) > 0:
#         measure_name_list = [measure_name.split('-')[-1] for measure_name in measures]
#         prompt = prompt + "分析的主题是：" + str(measure_name_list) + "\n"
#
#     time = request_data["time"]
#     if len(time) > 0:
#         prompt = prompt + "时间范围：" + str(time[0]) + "\n"
#
#     group = request_data["group"]
#     if len(group) > 0:
#         group_name_list = [info["columnName"] for info in group]
#         prompt = prompt + "分组条件：" + str(group_name_list) + "\n"
#
#     demisions = request_data["demisions"]
#     demisions_name_list = []
#     if len(demisions) > 0:
#         demisions_name_list = [info["dimesionName"] for info in demisions]
#
#     data_all = request_data["data_all"]
#     prompt = prompt + "我将按照：" + str(demisions_name_list + measure_name_list) + " 的形式为你提供所有数据\n"
#     prompt = prompt + "所有数据：" + str(data_all) + " \n"
#
#     rule = '''要求：1、直接对数据进行分析，不要询问或输出无关的内容。
# 2、你只关心可以明确做出分析的内容
# 3、输出内容不要包含疑问句
#     '''
#     prompt = prompt + rule
#
#     prompt = prompt + "请开始你的分析："
#
#     response = send_llm(prompt)
#     return response


# def get_data_fenxi_stream(history, request_data):
#     prompt = '你是一个数据分析大师，请对供给你结构化数据进行分析。\n'
#
#     measures = request_data["measures"]
#     measure_name_list = []
#     if len(measures) > 0:
#         measure_name_list = [measure_name.split('-')[-1] for measure_name in measures]
#         prompt = prompt + "分析的主题是：" + str(measure_name_list) + "\n"
#
#     time = request_data["time"]
#     if len(time) > 0:
#         prompt = prompt + "时间范围：" + str(time[0]) + "\n"
#
#     group = request_data["group"]
#     if len(group) > 0:
#         group_name_list = [info["columnName"] for info in group]
#         prompt = prompt + "分组条件：" + str(group_name_list) + "\n"
#
#     demisions = request_data["demisions"]
#     demisions_name_list = []
#     if len(demisions) > 0:
#         demisions_name_list = [info["dimesionName"] for info in demisions]
#
#     data_all = request_data["data_all"]
#     prompt = prompt + "我将按照：" + str(demisions_name_list + measure_name_list) + " 的形式为你提供所有数据\n"
#     prompt = prompt + "所有数据：" + str(data_all) + " \n"
#
#     rule = '''要求：1、直接对数据进行分析，不要询问或输出无关的内容。
# 2、你只关心可以明确做出分析的内容
# 3、输出内容不要包含疑问句
#     '''
#     prompt = prompt + rule
#
#     prompt = prompt + "请开始你的分析："
#
#     for chunk in send_llm_stream(prompt):
#         data = agent_reply(chunk)
#         # print(data)
#         yield json.dumps(data)+ "\n"



    # print(data)
    # yield json.dumps(data) + "\n"





def get_zhibiao_list(data_dict):
    zhibiao_list = []
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        del_word_list = ['最大值', '最小值', '平均', '合计']

        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    targetName = targetName.strip().split('-')[-1].strip()
                    for del_word in del_word_list:
                        if del_word == targetName[-len(del_word):]:
                            targetName = targetName.replace(del_word, '')
                    if targetName not in zhibiao_list:
                        zhibiao_list.append(targetName)
    return zhibiao_list




def get_zhibiao_sort_by_type(data_dict):
    '''
    按照指标类型（组合、派生、原子）进行重新排序
    user_object.metric_type[metric] 除了以上三种，还可能为 “其他”
    '''

    combination = []
    derivative = []
    atom = []
    other = []

    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        del_word_list = ['最大值', '最小值', '平均', '合计']

        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    targetType = zhibiao["targetType"]
                    targetName = targetName.strip().split('-')[-1].strip()
                    for del_word in del_word_list:
                        if del_word == targetName[-len(del_word):]:
                            targetName = targetName.replace(del_word, '')

                    if targetType == 1:
                        if targetName not in atom:
                            atom.append(targetName)
                    elif targetType == 2:
                        if targetName not in derivative:
                            derivative.append(targetName)
                    elif targetType == 3:
                        if targetName not in combination:
                            combination.append(targetName)
                    else:
                        if targetName not in other:
                            other.append(targetName)

    return combination + derivative + atom + other



def metric_recommend(no_threshold_sorted_reranker):
    # 1、初步筛选重排结果分数大于0的，结果至少有两个，可能包含负值
    seen_values = []
    positive_result = []
    negative_result = []
    for item in no_threshold_sorted_reranker:
        if item['value'] not in seen_values:
            seen_values.append(item['value'])
            if item['score'] > 0:
                positive_result.append(item)
            else:
                negative_result.append(item)

    # print("positive_result: ", positive_result)
    # print("negative_result: ", negative_result)
    unique_results_reranker = positive_result

    # 2、根据 positive_result 的长度进行考虑
    if len(positive_result) == 0:
        return negative_result
    elif len(positive_result) == 1:
        if positive_result[0]['score'] >= 5.0:
            return positive_result
        else:
            return positive_result + negative_result[:4]
    elif len(positive_result) <= 5:
        if positive_result[0]['score'] - positive_result[1]['score'] >= 5.0:
            return positive_result[:1]
        else:
            return positive_result + negative_result[:5-len(positive_result)]

    else:  # 正相关的6个及以上
        scores = [float(item['score']) for item in positive_result]
        # 计算均值和标准差
        mean_score = np.mean(scores)
        std_dev_score = np.std(scores)
        # 计算标准分数
        selected_items = [
            item for item in positive_result
            if (item['score'] - mean_score) / std_dev_score > -0.5
        ]
        return selected_items


def group_recommend(no_threshold_sorted_reranker):
    # 只考虑正相关，根据结果直接返回，不考虑返回几个
    seen_values = []
    positive_result = []
    for item in no_threshold_sorted_reranker:
        if item['value'] not in seen_values:
            seen_values.append(item['value'])
            if item['score'] > 0:
                positive_result.append(item)

    # 2、根据 positive_result 的长度进行考虑
    if len(positive_result) == 0:
        return []

    elif len(positive_result) == 1:
        if positive_result[0]['score'] >= 2.0:
            return positive_result
        else:
            return []

    else:  #
        scores = [float(item['score']) for item in positive_result]
        # 计算均值和标准差
        mean_score = np.mean(scores)
        std_dev_score = np.std(scores)
        # 计算标准分数
        selected_items = [
            item for item in positive_result
            if (item['score'] - mean_score) / std_dev_score > -1
        ]
        return selected_items


def refactoring_history(user_object, new_input, history):

    if len(user_object.history) >= 2:
        if "model" in user_object.history[-1].keys():
            if len(history) == 0:
               # 上次意图达成后的新输入
               # 第一步：根据历史判断是查询类意图（用户输入可能仅是某个条件的改变但结合历史仍是查询意图）
                if new_input_continuity_assessment(user_object.history, new_input):
                    # 还要细分两种情况：1、原查询主体（指标）只改变条件 2、新的查询主体（指标）
                    # 主体（指标）改变只继承时间类通用条件
                    # 1、先生成上一轮的最终问题 2、结合历史对新问题改写 3、比较两个最终问题的sql抽取的主体结果
                    last_question = rewrite_continuity_question_history(user_object.history)
                    new_question = rewrite_continuity_question_all(new_input, user_object.history)
                    logger.info(f"last_question：{last_question}")
                    sql1 = extract_sql(last_question)
                    logger.info(f"new_question：{new_question}")
                    sql2 = extract_sql(new_question)

                    # 通过判断两个sql的from字段是否相同来判断主体是否改变
                    if query_subject_change(sql1, sql2):
                        # 主体改变，改写的可能会包含过去主体的条件，应该舍弃大部分条件进行重新改写，如保留时间
                        logger.info(f"查询主体改变")
                        second_new_question = rewrite_continuity_question_time(new_input, user_object.history)
                        logger.info(f"延续性意图改写（仅考虑继承时间）：{second_new_question}")
                        print(f"延续性意图改写（仅考虑继承时间）：{second_new_question}")
                        user_object.multi_recommendation_recognition = {}
                        user_object.test2sql_information = {}
                        user_object.history = [{"user": second_new_question}]
                    else:
                        # 主体（指标）不变，已识别的条件都要考虑继承和修改，由模型判断
                        logger.info(f"查询主体不变")
                        logger.info(f"延续性意图改写（考虑继承全部）：{new_question}")
                        print(f"延续性意图改写（考虑继承全部）：{new_question}")
                        last_multi_recommendation_recognition = user_object.multi_recommendation_recognition
                        user_object.multi_recommendation_recognition = deal_recommendation_recognition(user_object.history)
                        if "metric" not in user_object.multi_recommendation_recognition:
                            user_object.multi_recommendation_recognition["metric"] = last_multi_recommendation_recognition["metric"]
                        logger.info(f"user_object.multi_recommendation_recognition：{user_object.multi_recommendation_recognition}")
                        user_object.test2sql_information = sql2
                        user_object.history = [{"user": new_question}]

                    return user_object
                else:
                    user_object.multi_recommendation_recognition = {}

    ## len(history) > 0表示多轮过程中，判断当前用户问题是否是上一轮对话的延续,如果不是，则清空历史
    if len(history) > 0:  # history是通过上次reply给前端的history再转发来的
        if not continue_multiple_rounds_of_judgment(new_input, history):
            user_object.history =[{"user": new_input}]
            return user_object

    '''
       判断新输入是否是之前推荐过的指标，若是则带上相关历史信息
    '''

    if len(user_object.history) >= 2:
        if "model" in user_object.history[-1].keys():
            if "metric" in user_object.history[1].keys():
                context = user_object.history[1]["metric"]
                last_colon_index = context.rfind(':')
                if last_colon_index != -1:
                    zhibiao_list_str = context[last_colon_index:]
                    if new_input in zhibiao_list_str:
                        new_history = user_object.history
                        new_all_metric_data = user_object.all_metric_data
                        test2sql_information = user_object.test2sql_information
                        user_object.__init__()
                        user_object.extract_plan = "指标"
                        user_object.history = new_history[:2] + [{"user": new_input}]
                        user_object.all_metric_data = new_all_metric_data
                        user_object.metric2id = get_metrc2id(new_all_metric_data)
                        user_object.metric_type = get_metric_type(new_all_metric_data)
                        user_object.time_name2id_dict = get_time_name2id_dict(new_all_metric_data)
                        user_object.table_relation = get_table_relation(new_all_metric_data)
                        user_object.table_name2id, user_object.table_id2name = get_table_map(new_all_metric_data)
                        user_object.test2sql_information = test2sql_information

                        return user_object

            user_object.history = [{"user": new_input}]
            return user_object

        else:  # 即 {"model": "CompleteOutput"} 不在历史
            if len(history) == 0:  # 前端发来hsitory只有两种情况，1、刷新页面后 2、一次意图达成
                # 此时history 为0 ，但{"model": "CompleteOutput"} 不在历史，只会是页面刷新
                user_object.history = [{"user": new_input}]
                return user_object

    user_object.history = user_object.history + [{"user": new_input}]
    return user_object


def fuzzy_conflict_selection(user_object, final_fuzzy_dict, phrases):
    # 先判断准备放入slot_dict["where"]的值是否属于 模糊值或模糊推荐
    fuzzy_value_exist = False
    fuzzy_recommend_exist = False
    for where_info in user_object.slot_dict["where"]:
        # 对于模糊值，where_info["targetValue"] 就是 phrases
        if where_info["CompleteMatch"] is False and phrases in where_info["targetValue"]:
            fuzzy_value_exist = True
            break

    for fuzzy_info in final_fuzzy_dict:
        if phrases in fuzzy_info["phrases"]:
            fuzzy_recommend_exist = True
            break

    if not fuzzy_value_exist and not fuzzy_recommend_exist:
        # 不属于模糊值或模糊推荐，此时可以放入slot_dict["where"]，但要判断已有的模糊值和模糊推荐是否属于当前where值
        current_where = []
        for where_info in user_object.slot_dict["where"]:
            if where_info["CompleteMatch"] is False:
                if where_info["targetValue"] not in phrases:
                    current_where.append(where_info)
            else:
                current_where.append(where_info)
        user_object.slot_dict["where"] = current_where

        current_fuzzy_dict = []
        for fuzzy_info in final_fuzzy_dict:
            if fuzzy_info["phrases"] not in phrases:
                current_fuzzy_dict.append(fuzzy_info)

        final_fuzzy_dict = current_fuzzy_dict

    return fuzzy_value_exist, fuzzy_recommend_exist, final_fuzzy_dict


def sort_by_metric_type(user_object, no_threshold_values_reranker):
    '''
    按照指标类型（组合、派生、原子）进行重新排序
    user_object.metric_type[metric] 除了以上三种，还可能为 “其他”
    '''
    combination = []
    derivative = []
    atom = []
    other = []
    for metric in no_threshold_values_reranker:
        if metric in user_object.metric_type.keys():
            if user_object.metric_type[metric] == "派生指标":
                combination.append(metric)
            elif user_object.metric_type[metric] == "组合指标":
                derivative.append(metric)
            elif user_object.metric_type[metric] == "原子指标":
                atom.append(metric)
            else:
                other.append(metric)

        else:
            other.append(metric)

    logger.info(f"组合指标：{combination}")
    logger.info(f"派生指标：{derivative}")
    logger.info(f"原子指标：{atom}")
    logger.info(f"其他指标：{other}")

    return combination + derivative + atom + other

# 今年各个科室的申请人次

def jaccard_similarity(str1, str2):
    set1 = set(str1)
    set2 = set(str2)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    value = float(len(intersection)) / len(union)
    if value >= 1:
        value = 1

    return value


def str_truncation(start, length, text):
    return text[start:start + length]


def str_truncation_jieba(i, window_size, default_word_position):
    target_list = default_word_position[i: i + window_size]
    phrase = ''
    position_list = []
    for info in target_list:
        phrase = phrase + info["word"]
        position_list = position_list + info["position_list"]

    return phrase, position_list


def continue_multiple_rounds_of_judgment(user_input,history):
    logger.info("continue_multiple_rounds_of_judgment\n")
    logger.info(f"user_input:\n{user_input}")
    logger.info(f"history:\n{history}")

    # 对于多轮中用户输入的一些回答可以直接判断为true
    user_input = user_input.strip()
    if len(history) > 0:
        if 'time' in history[-1].keys():  # 对应时间多轮
            if user_input in ["近7天", "近30天", "本周", "上周", "本月", "上个月", "今年", "去年"]:
                return True
            if extract_dates_time_control(user_input) != '[]':
                return True
            if judge_input_time(user_input):
                return True

        elif 'metric' in history[-1].keys():  # 对应指标多轮
            _, zhibiao_str = history[-1]["metric"].split(':')
            recommend_zhibiao = zhibiao_str.split(',')
            if user_input in recommend_zhibiao:
                return True
            if ',' in user_input:
                input_split = user_input.split(',')
                for metric in input_split:
                    if metric.strip() in recommend_zhibiao:
                        return True

        elif 'table' in history[-1].keys():  # 对应指标多轮
            _, table_str = history[-1]["table"].split(':')
            recommend_table = table_str.split(',')
            if user_input in recommend_table:
                return True

        elif 'group' in history[-1].keys():  # 对应指标多轮
            _, group_str = history[-1]["group"].split(':')
            recommend_group = group_str.split(',')
            if user_input in recommend_group:
                return True

        elif 'where' in history[-1].keys():  # 对应指标多轮
            _, where_str = history[-1]["where"].split(':')
            recommend_where = where_str.split(',')
            if user_input in recommend_where:
                return True

        elif 'other' in history[-1].keys():  # 对应指标多轮
            _, other_str = history[-1]["other"].split(':')
            recommend_other = other_str.split(',')
            if user_input in recommend_other:
                return True

        elif 'timeType' in history[-1].keys():  # 对应指标多轮
            _, timeType_str = history[-1]["timeType"].split(':')
            recommend_timeType = timeType_str.split(',')
            if user_input in recommend_timeType:
                return True

    history_str=""
    for i,it in enumerate(history):
        value=''
        if i%2==0:
            value="user:"+list(it.values())[0]+"\n"
        else:
            multurn_name = list(it.keys())[0]
            if multurn_name == "time":
                value="assistant:"+list(it.values())[0]+" (或者输入其他时间)\n"
            else:
                value = "assistant:" + list(it.values())[0] + "\n"
        history_str+=value
    
    system_prompt=f"""### 任务：
你需要判断用户当前输入是否是用户历史对话的延续。如果是，则输出："是"；否则输出"否"。

### 输出规则：
输出内容必须是"是"或"否"。
禁止反问用户。
禁止解释或者说明。

"""
    user_prompt=f"""### 用户历史对话：
{history_str}
### 用户当前输入：
{user_input}

### 输出：
"""
    logger.info(f"system_prompt:\n{system_prompt}")
    logger.info(f"user_prompt:\n{user_prompt}")
    output=send_llm_system(system_prompt,user_prompt)
    logger.info(f"output:\n{output}")
    if "是" in output:
        return True
    else:
        return False


def new_input_continuity_assessment(history, new_input):

    rewrite_history = []
    for info in history:
        if "user" in info.keys():
            rewrite_history.append(info)
        else:
            if "model" in info.keys():
                rewrite_history.append({"assistant": "完整意图识别成功，已为您展示结果"})
            else:
                rewrite_history.append({"assistant": list(info.values())[0]})

    prompt = f'''结合历史信息，判断用户新输入所属于的问题类型。
历史信息：{rewrite_history}
用户新输入：{new_input}

问题类型有以下三种：
咨询类（咨询如何使用、有什么指标）
查询类（查询具体的指标数据,或构造查询的指令）
其他类（无法判断）

要求：
1.只能且必须从 咨询类、查询类、其他类 这三种类型中选择一个
2.直接输出问题类型，不要输出任何解释类信息'''

    result = send_llm(prompt)
    logger.info(f"新意图延续性判断历史：{rewrite_history}")
    logger.info(f"结合历史，新输入类型判断（用来判断改写）：{result}")
    if "查询类" in result:
        logger.info(f"是否需要改写：需要")
        return True
    else:
        logger.info(f"是否需要改写：不需要")
        return False


def extract_time_word(information):
    prompt = f'''根据对话信息提取最符合用户意图的或用户曾经提及的时间条件关键词，将所有最符合的关键词放到一个列表输出。
    对话信息：{information}

    要求：
    1、以能够被json解析的列表形式输出，不要输出其他解释性内容。
    2、如果不存在符合用户意图的时间关键词，则输出空列表[]'''

    time_result = send_llm(prompt)
    time_word = find_max_list(time_result)
    return time_word


def rewrite_continuity_question_time(quetion, history):
    rewrite_history_time = ''
    for info in history:
        if "user" in info.keys():
            rewrite_history_time = rewrite_history_time + '，' + info['user']

    # 用户输入中已有时间词则不做改写
    user_time_word = extract_time_word(quetion)
    logger.info(f"用户新输入提取时间关键词：{user_time_word}")
    if len(user_time_word) > 0:
        return quetion

    else:
        # 用户输入中不存在时间，需要先从历史对话中抽取再改写
        history_time_word = extract_time_word(rewrite_history_time)
        logger.info(f"历史对话提取时间关键词：{history_time_word}")
        if len(history_time_word) == 0:
            logger.info(f"时间关键词不存在，直接返回原问题")
            return quetion
        else:
            # 用户输入中不存在时间，历史对话中存在，需要改写补充时间
            prompt = f'''判断根据时间关键词对用户的输入进行改写。
时间关键词：{history_time_word}
用户新输入：{quetion}

规则
1.只需要输出你改写的新问题的最终版本，不能反问用户，不要输出“注意”等信息
2.改写的问题尽可能是连贯的、没有停顿的、简洁的，不使用逗号'''
            rewrite = send_llm(prompt)
            return rewrite


def rewrite_continuity_question_history(history):
    # 通过一轮完整的对话总结出来一句话

    # 剔除掉table的推荐过程，因为改写容易把table改成指标
    new_history = []
    del_index = []
    i = 0
    for info in history:
        if "table" in info.keys():
            del_index.append(i)
            del_index.append(i + 1)
        else:
            if i not in del_index:
                new_history.append(info)
        i = i + 1

    # 没有进入多轮时直接返回原问题
    if len(new_history) == 2:
        if "model" in new_history[-1].keys():
            return list(new_history[0].values())[0]

    rewrite_history = []
    for info in new_history:
        if "user" in info.keys():
            rewrite_history.append(info)
        else:
            if "model" in info.keys():
                rewrite_history.append({"assistant": "完整意图识别成功，已为您展示结果"})
            else:
                rewrite_history.append({"assistant": list(info.values())[0]})

    prompt = f'''参考历史信息的条件信息进行改写

历史信息：{rewrite_history}

规则
1.只需要输出你改写的新问题的最终版本，不要输出解释及其他内容
2.新问题尽可能是连贯的、没有停顿的、简洁的，不使用逗号
3.不能反问用户，不要输出“注意”等信息'''

    rewrite = send_llm(prompt)

    return rewrite



def rewrite_continuity_question_all(quetion, history):
    # 通过一轮完整的对话和新输入总结出来一句话
    current_year = str(get_current_year()) + '年'
    last_year = str(get_last_year()) + '年'
    # 剔除掉table的推荐过程，因为改写容易把table改成指标
    new_history = []
    del_index = []
    i = 0
    for info in history:
        if "table" in info.keys():
            del_index.append(i)
            del_index.append(i + 1)
        else:
            if i not in del_index:
                new_history.append(info)
        i = i + 1

    rewrite_history = []
    for info in new_history:
        if "user" in info.keys():
            info["user"] = info["user"].replace("去年", last_year).replace("今年", current_year)
            rewrite_history.append(info)
        else:
            if "model" in info.keys():
                rewrite_history.append({"assistant": "完整意图识别成功，已为您展示结果"})
            else:
                rewrite_history.append({"assistant": list(info.values())[0]})

    # 把用户输入的口语时间转换成数字

    quetion = quetion.replace("去年", last_year).replace("今年", current_year)


    prompt = f'''根据历史信息对用户新输入进行改写，参考历史信息的条件信息进行补充，如果用户意图和历史不相关则不进行补充。
以最新输入的条件为最后条件，覆盖历史中已出现的同类条件，保留历史信息中的其他出现的条件。
如果用户新输入提到了新的查询对象，以用户最新输入的为准，覆盖之前提到的查询对象。

历史信息：{rewrite_history}
用户新输入：{quetion}

规则
1.只需要输出你改写的新问题的最终版本
2.新问题尽可能是连贯的、没有停顿的、简洁的，不使用逗号
3.不能反问用户，不要输出“注意”等信息'''

    rewrite = send_llm(prompt)

    return rewrite


def exist_same_table(metric_list, target_name_list):
    # 判断指标是否存在相同的业务域
    target_name2table = {}
    for target_name in target_name_list:
        if '-' in target_name:
            table_name = target_name.split('-')[0]
            metric_name = target_name.split('-')[-1].replace("合计", "").replace("平均", "").replace("最大值", "").replace("最小值", "")
            if metric_name not in target_name2table.keys():
                target_name2table[metric_name] = [table_name]
            else:
                if table_name not in target_name2table[metric_name]:
                    target_name2table[metric_name].append(table_name)

    # 所有table放一起，如果存在某个table数等于指标数，那么存在共同业务域
    table_count = {}
    for metric_name in metric_list:
        if metric_name in target_name2table.keys():
            for table_name in target_name2table[metric_name]:
                if table_name in table_count.keys():
                    table_count[table_name] += 1
                else:
                    table_count[table_name] = 1

    if len(table_count) == 0:
        return False

    max_value = max(count for table_name, count in table_count.items())
    if max_value == len(metric_list):
        return True

    return False