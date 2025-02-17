import re

from config import CURRENT_SCENE
from config.log_config import logger
from match.group_vector_match import get_group_vector_match, get_group_label_vector_match, get_group_sql_vector_match
from match.jaccard_match import get_group_jaccard_match_muti, get_group_jaccard_match_first
from utils.util import rewrite_input_for_group, get_position, judge_group_name_exist, cal_start_pos, insert_group, \
    get_group_label_dict, update_tableRelation


def extract_group(user_object, user_input):

    relation_table_id = []  # 考虑多指标下的多表的关联关系
    main_table_id = []
    group_json_list = []
    for metric, table_list in user_object.metric_table.items():
        for table_name in table_list:
            tableId = int(user_object.table_name2id[table_name])
            if tableId not in main_table_id:
                main_table_id.append(tableId)
            relation_table_id = update_tableRelation(user_object, relation_table_id, tableId)
            if "分组条件" in user_object.metric_knowledge_graph[metric][table_name].keys():
                current_group_json_list = user_object.metric_knowledge_graph[metric][table_name]["分组条件"]
                for info in current_group_json_list:
                    if info not in group_json_list:
                        group_json_list.append(info)

    clean_relation_table_id = [id for id in relation_table_id if id not in main_table_id]
    relation_table_id = clean_relation_table_id

    print("*****", "main_table_id: ", main_table_id)
    print("*****", "relation_table_id: ", relation_table_id)

    # user_object.table_map
    group_name_list = []
    group_name2id_dict = {}
    self_group_name2id_dict = {}
    for group_json in group_json_list:
        if "columnName" in list(group_json.keys()):
            if group_json["columnName"] not in group_name_list:
                group_name_list.append(group_json["columnName"])
                group_name2id_dict[group_json["columnName"]] = group_json["columnId"]
                self_group_name2id_dict[group_json["columnName"]] = group_json["columnId"]

    print("*****", "当前指标->表的group(不考虑关联表)：", group_name_list)
    logger.info(f"当前指标->表的group(不考虑关联表): {group_name_list}")

    all_table_id = main_table_id + relation_table_id
    user_object.relation_table_id = relation_table_id  # 将自身group和关联group放一起统一识别

    group_json_list = [info for info in user_object.all_metric_data["valueJson"] if info["tableId"] in all_table_id]
    group_label_dict = get_group_label_dict(group_json_list)
    # group_name_list = []
    group_name2id_dict = {}
    for group_json in group_json_list:
        if "columnName" in list(group_json.keys()):
            if group_json["columnName"] not in group_name_list:
                group_name_list.append(group_json["columnName"])
                group_name2id_dict[group_json["columnName"]] = group_json["columnId"]
    print("*****", "当前指标->表的group(考虑关联表)：", group_name_list)
    logger.info(f"当前指标->表的group(考虑关联表)：{group_name_list}")

    mul_turn_flag = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "group" in user_object.history[-2].keys():
            real_input = user_input
            mul_turn_flag = True
        else:
            real_input = user_object.history[0]["user"]

    extract_group_result = {"result": '', "need_multi_turn": False}

    if not mul_turn_flag:
        real_input = rewrite_input_for_group(user_object, real_input, group_name_list)
        pre_label_input = real_input

        group_character_match = get_group_jaccard_match_first(user_object, real_input, group_name_list, group_name2id_dict, self_group_name2id_dict)
        logger.info(f"group character 识别：{group_character_match}")
        group_character_match_add_position = get_position(group_character_match, real_input)

        # 处理group_label词，有些group词难以匹配，通过标签的方式再次判断
        for info in group_character_match_add_position:  # 记录语义匹配的片段（先不考虑向量召回）
            user_object.group_recognize_by_phrase.append(info['phrases'])

        group_vector_match = get_group_vector_match(user_object, CURRENT_SCENE + "_group", group_name_list, main_table_id, relation_table_id, self_group_name2id_dict)
        # [{'phrases': '四级手术', 'position_list': [15, 16, 17, 18], 'recall_word': '手术级别'}]
        # group_vector_match 可能是由 where的具体值召回得到的

        group_vector_match_add_position = get_position(group_vector_match, real_input)

        merge_match_json = group_character_match_add_position  # 排序用的
        for vector_info in group_vector_match_add_position:
            overlap_flag = False
            for character_info in group_character_match_add_position:
                intersection_list = [value for value in vector_info["position_list"] if value in character_info["position_list"]]
                if len(intersection_list) != 0:
                    overlap_flag = True
                    break
            if not overlap_flag:
                merge_match_json.append(vector_info)

        merge_match_json = sorted(merge_match_json, key=lambda x: x["position_list"][0], reverse=False)

        if len(merge_match_json) > 0:
            for info in merge_match_json:
                group_name = info["recall_word"]
                if group_name not in ','.join(user_object.slot_dict["metric"]):
                    exist_flag = judge_group_name_exist(user_object, group_name)
                    if not exist_flag and len(group_name) > 0:
                        user_object.slot_dict["group"].append({"columnId": info["columnId"], "columnName": group_name, "start_pos": info["position_list"][0], "phrases": info["phrases"], "max_score": info["max_score"]})
                        user_object.group_recognize_by_phrase.append(info['phrases'])
                        logger.info(f"group vector 成功识别：{info['phrases']}：{group_name}")

        # group 多轮使用的
        group_ask_list = []
        label_use_phrase = []
        #  标签的向量计算
        # print("group_label_dict", group_label_dict)
        if len(group_label_dict) > 0:
            group_label_vector_match = get_group_label_vector_match(user_object, CURRENT_SCENE + "_group_label", group_name_list)
            # print("group_label_vector_match: ", group_label_vector_match)
            for match_info in group_label_vector_match:
                phrase = match_info["phrases"]
                max_score = match_info["max_score"]
                # 计算label时候real_input已经发生变化，因此要重新计算start_pos
                start_pos = cal_start_pos(pre_label_input, phrase)  # 用来标记识别到groupname的phrase的起始位置，使最终的group有序
                recall_weidu_list = match_info["recall_weidu_list"]
                if len(recall_weidu_list) == 0:
                    continue
                elif len(recall_weidu_list) == 1:
                    group_name = recall_weidu_list[0]
                    exist_flag = judge_group_name_exist(user_object, group_name)
                    if not exist_flag:
                        if group_name in self_group_name2id_dict.keys():
                            new_dict = {"columnId": self_group_name2id_dict[group_name], "columnName": group_name, "start_pos": start_pos, "phrases": phrase, "max_score": max_score}
                        else:
                            new_dict = {"columnId": group_name2id_dict[group_name], "columnName": group_name, "start_pos": start_pos, "phrases": phrase, "max_score": max_score}
                        user_object.slot_dict["group"] = insert_group(user_object.slot_dict["group"], new_dict)
                        user_object.group_recognize_by_phrase.append(phrase)
                        logger.info(f"group label vector 成功识别：{phrase}：{group_name}")
                else:  # 需要进入多轮的
                    user_object.group_phrase_start_pos[phrase] = start_pos
                    group_ask_list.append('您提到的“' + phrase + '”具体是指哪个？例如:' + ','.join(recall_weidu_list[:6]))
                    label_use_phrase.append(phrase)

        group_sql_vector_match = get_group_sql_vector_match(user_object, group_name_list, label_use_phrase, CURRENT_SCENE + "_group")
        for match_info in group_sql_vector_match:
            phrase = match_info["phrases"]
            start_pos = cal_start_pos(pre_label_input, phrase)  # 用来标记识别到groupname的phrase的起始位置，使最终的group有序
            recall_weidu_list = match_info["recall_weidu_list"]
            if len(recall_weidu_list) == 0:
                continue
            elif len(recall_weidu_list) == 1:
                group_name = recall_weidu_list[0]
                exist_flag = judge_group_name_exist(user_object, group_name)
                if not exist_flag:
                    if group_name in self_group_name2id_dict.keys():
                        new_dict = {"columnId": self_group_name2id_dict[group_name], "columnName": group_name,
                                    "start_pos": start_pos, "phrases": phrase, "max_score": 1}
                    else:
                        new_dict = {"columnId": group_name2id_dict[group_name], "columnName": group_name,
                                    "start_pos": start_pos, "phrases": phrase, "max_score": 1}
                    user_object.slot_dict["group"] = insert_group(user_object.slot_dict["group"], new_dict)
                    user_object.group_recognize_by_phrase.append(phrase)
                    logger.info(f"group label vector 成功识别：{phrase}：{group_name}")
            else:  # 需要进入多轮的
                recall_weidu_list.append('无')
                user_object.group_phrase_start_pos[phrase] = start_pos
                group_ask_list.append('您提到的“' + phrase + '”具体是指哪个？例如:' + ','.join(recall_weidu_list[:6]))
                label_use_phrase.append(phrase)

        if len(group_ask_list) > 0:
            extract_group_result = {"result": '', "need_multi_turn": True, "group_ask_list": group_ask_list}

    else:
        phrase = ''
        if "group" in user_object.history[-2].keys():
            ask_question = user_object.history[-2]["group"]
            pattern = r'您提到的“([^"]+)”具体是指哪个？'
            match = re.search(pattern, ask_question)
            if match:
                phrase = match.group(1)

        start_pos = -1
        if phrase in user_object.group_phrase_start_pos.keys():
            start_pos = user_object.group_phrase_start_pos[phrase]

        group_character_match = get_group_jaccard_match_muti(real_input, group_name_list, group_name2id_dict, self_group_name2id_dict)
        for info in group_character_match:
            group_name = info["recall_word"]
            exist_flag = judge_group_name_exist(user_object, group_name)
            if not exist_flag:
                # 注意此时的 phrase 用多轮之前的，而不是从新输入确定的
                if group_name in self_group_name2id_dict.keys():
                    new_dict = {"columnId": self_group_name2id_dict[group_name], "columnName": group_name, "start_pos": start_pos, "phrases": phrase, "max_score": info["max_score"]}
                else:
                    new_dict = {"columnId": group_name2id_dict[group_name], "columnName": group_name, "start_pos": start_pos, "phrases": phrase, "max_score": info["max_score"]}
                user_object.slot_dict["group"] = insert_group(user_object.slot_dict["group"], new_dict)
                user_object.group_recognize_by_phrase.append(phrase)


    return extract_group_result