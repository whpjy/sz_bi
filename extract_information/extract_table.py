import re

from config.log_config import logger
from match.completely_match import get_completely_match
from match.jaccard_match import get_jaccard_match_table
from utils.util import get_metric_table_dict, get_new_metric_table_dict, get_table_name_list, \
    get_table_list_from_history


def extract_table(user_object, user_input):

    mul_turn_flag = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "table" in user_object.history[-2].keys():
            real_input = user_input
            mul_turn_flag = True
        else:
            real_input = user_object.history[0]["user"]

    if not mul_turn_flag:

        print("*****", "识别(指标)：", user_object.slot_dict["metric"])
        for metric in user_object.slot_dict["metric"]:  # metric_table用来存储指标对应的表
            user_object.metric_table[metric] = []

        # 所有已识别指标先获取拥有的所有表
        metric_table_dict = get_metric_table_dict(user_object)  # 获取各个指标下的表及其表关系
        logger.info(f"metric_table_dict：{str(metric_table_dict)}")
        # 单指标
        if len(metric_table_dict) == 1:
            new_metric_table_dict = metric_table_dict
        # 多指标通过tableRelation过滤保留公共表，公共表不存在则业务域不存在
        else:
            new_metric_table_dict = get_new_metric_table_dict(metric_table_dict)  # 根据表关系筛选出指标下可用的表
            logger.info(f"new_metric_table_dict：{str(new_metric_table_dict)}")
            # 经过筛选后如果某个指标下的表为空则没有共同业务域
            for k, v in new_metric_table_dict.items():
                if len(v) == 0:
                    extract_table_result = {"result": f'找不到包含指标{"，".join(user_object.slot_dict["metric"])}的业务域', "need_multi_turn": True}
                    return extract_table_result

        table_name_list = get_table_name_list(new_metric_table_dict)
        table_completely_match = get_completely_match(real_input, table_name_list)
        table_jaccard_match = get_jaccard_match_table(user_object, real_input, table_name_list)
        print("table测试", "table_jaccard_match", table_jaccard_match)

        table_character_match = []
        for table_name in table_completely_match + table_jaccard_match:
            if table_name not in table_character_match:
                table_character_match.append(table_name)

        # 当前默认问题中最多只出现一个表，如果出现多个取第一个
        if len(table_character_match) > 0:  # 如果用户已经输入了表名，可以确定的则确定
            for metric, table_list_json in new_metric_table_dict.items():
                table_list = [info["tableName"] for info in table_list_json]
                table_name = table_character_match[0]
                if table_name in table_list:
                    if len(user_object.metric_table[metric]) == 0:
                        user_object.metric_table[metric] = [table_name]

            extract_table_result = {"result": '', "need_multi_turn": False}
            return extract_table_result

        else:

            if len(table_name_list) == 1:
                for metric, _ in new_metric_table_dict.items():
                    user_object.metric_table[metric] = [table_name_list[0]]
                extract_table_result = {"result": '', "need_multi_turn": False}
                return extract_table_result

            elif len(table_name_list) > 1:

                # 进入多轮前要检查一下是否上个意图和本次意图是否在同个指标之下，可以使用上次意图的结果来避免多余的多轮
                if "table" in user_object.multi_recommendation_recognition.keys():  # 有table说明上个意图和本次指标一致

                    table_name = user_object.multi_recommendation_recognition["table"][0]
                    if table_name in table_name_list:
                        for metric, _ in new_metric_table_dict.items():
                            user_object.metric_table[metric] = [table_name]
                        extract_table_result = {"result": '', "need_multi_turn": False}
                        return extract_table_result

                table_ask_list = []
                table_ask_list.append(f"识别出以下业务域包含“{','.join(list(new_metric_table_dict.keys()))}”，请您选择具体业务域:" + ','.join(table_name_list))
                extract_table_result = {"result": '', "need_multi_turn": True, "table_ask_list": table_ask_list}
                return extract_table_result
    else:

        metric_match = ''
        metric_list = []
        table_name_list = []
        if "table" in user_object.history[-2].keys():
            ask_question = user_object.history[-2]["table"]
            pattern = r'识别出以下业务域包含“([^"]+)”'
            match = re.search(pattern, ask_question)
            if match:
                metric_match = match.group(1)
                metric_list = metric_match.split(',')
            table_name_list = get_table_list_from_history(ask_question)

        if len(metric_list) > 0 and len(table_name_list) > 0:
            table_completely_match = get_completely_match(real_input, table_name_list)


            for metric in metric_list:
                user_object.metric_table[metric] = []
                for table_name in table_completely_match:
                    if table_name in user_object.metric_knowledge_graph[metric].keys():
                        user_object.metric_table[metric].append(table_name)

    extract_table_result = {"result": '', "need_multi_turn": False}
    return extract_table_result
