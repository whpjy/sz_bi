import time
from extract_information.extract_aggregation import extract_aggregation
from extract_information.extract_group import extract_group
from extract_information.extract_intent import extract_intent
from extract_information.extract_metric import extract_metric
from extract_information.extract_other import extract_other
from extract_information.extract_sql import extract_sql
from extract_information.extract_table import extract_table
from extract_information.extract_time import extract_time
from extract_information.extract_timeType import extract_timeType
from extract_information.extract_where import extract_where
from knowledge_graph.get_metric_knowledge_graph import get_knowledge_graph
from utils.util import get_clear_slot_dict, get_metrc2id, get_time_name2id_dict, get_table_attribute_list, \
    get_table_map, get_table_relation, get_metric_type
from utils.reply import final_reply, multi_reply
from config.log_config import logger
from utils.window_phrases import get_window_phrases


def main_logic_deal(user_object, user_input, table_describe):

    if len(user_object.history) == 1:  # 防止刷新浏览器造成extract_plan、slot_dict非空
        logger.info(f"------开始数据对象初始化------")
        new_history = user_object.history
        new_all_metric_data = user_object.all_metric_data
        test2sql_information = user_object.test2sql_information
        multi_recommendation_recognition = user_object.multi_recommendation_recognition
        user_object.__init__()
        user_object.extract_plan = "指标"
        user_object.history = new_history
        user_object.all_metric_data = new_all_metric_data
        user_object.metric2id = get_metrc2id(user_object.all_metric_data)
        user_object.metric_type = get_metric_type(user_object.all_metric_data)
        user_object.time_name2id_dict = get_time_name2id_dict(user_object.all_metric_data)
        user_object.table_relation = get_table_relation(user_object.all_metric_data)
        user_object.table_name2id, user_object.table_id2name = get_table_map(user_object.all_metric_data)
        user_object.multi_recommendation_recognition = multi_recommendation_recognition
        user_object.test2sql_information = test2sql_information
        if len(user_object.test2sql_information) == 0:
            user_object.test2sql_information = extract_sql(user_input)
        logger.info(f"------完成数据对象初始化------")

    # 抽取指标
    if user_object.extract_plan == "指标":
        logger.info(f"------开始指标识别------")
        user_object.jieba_window_phrases2positionList = get_window_phrases(user_object)
        extract_metric_result = extract_metric(user_object, user_input)
        if extract_metric_result["need_multi_turn"]:
            user_object.history.append({"metric": extract_metric_result["result"]})
            return multi_reply(user_object, current_scene="metric")
        else:
            for metric in extract_metric_result["result"]:
                zhibiao_knowledge_graph = get_knowledge_graph(user_object.all_metric_data, metric)
                user_object.metric_knowledge_graph[metric] = zhibiao_knowledge_graph[metric]
            user_object.slot_dict["metric"] = extract_metric_result["result"]
            if "metric" not in user_object.multi_recommendation_recognition.keys():
                user_object.multi_recommendation_recognition["metric"] = user_object.slot_dict["metric"]

            logger.info(f"------完成指标识别------")
            user_object.extract_plan = "表"

    # 抽取表
    if user_object.extract_plan == "表":
        logger.info(f"------开始表识别------")
        extract_table_result = extract_table(user_object, user_input)
        if extract_table_result["need_multi_turn"]:
            if "table_ask_list" in extract_table_result.keys():
                user_object.table_ask_list = extract_table_result["table_ask_list"]
            else:
                user_object.history.append({"table": extract_table_result["result"]})
                return multi_reply(user_object)

        if len(user_object.table_ask_list) > 0:
            ask_str = user_object.table_ask_list[0]
            user_object.table_ask_list.remove(ask_str)
            user_object.history.append({"table": ask_str})
            return multi_reply(user_object, current_scene="table", table_describe=table_describe)
        else:
            user_object.extract_plan = ""
            user_object.table_attribute_list = get_table_attribute_list(user_object)
            logger.info(f"------完成表识别------")
            print("*****", "指标属性：", user_object.table_attribute_list)
            if "time" not in user_object.table_attribute_list:
                user_object.table_attribute_list.append("time")
                print("--指标不存在时间条件, 但仍然抽取时间")

    if "聚合方式" in user_object.table_attribute_list:
        user_object.extract_plan = "聚合方式"
        user_object.table_attribute_list.remove("聚合方式")

    # 抽取聚合方式
    if user_object.extract_plan == "聚合方式":
        logger.info(f"------开始聚合方式识别------")
        extract_aggregation_result = extract_aggregation(user_object, user_input)
        if extract_aggregation_result["need_multi_turn"]:
            user_object.history.append({"aggregation": extract_aggregation_result["result"]})
            return multi_reply(user_object)
        else:
            user_object.extract_plan = ""
            logger.info(f"------完成聚合方式识别------")

    if "time" in user_object.table_attribute_list:
        user_object.extract_plan = "time"
        user_object.table_attribute_list.remove("time")

    # 抽取time
    if user_object.extract_plan == "time":
        logger.info(f"------开始time识别------")
        extract_time_result = extract_time(user_object, user_input)
        if extract_time_result["need_multi_turn"]:
            user_object.history.append({"time": extract_time_result["result"]})
            return multi_reply(user_object, current_scene="time")
        else:
            user_object.slot_dict["time"] = extract_time_result["result"]
            logger.info(f"------完成time识别------")
            user_object.extract_plan = "timeType"

    # 抽取timeType, 这是和time一起的
    if user_object.extract_plan == "timeType":
        logger.info(f"------开始timeType识别------")
        extract_timeType_result = extract_timeType(user_object, user_input)
        if extract_timeType_result["need_multi_turn"]:
            user_object.history.append({"timeType": extract_timeType_result["result"]})
            return multi_reply(user_object)
        else:
            user_object.slot_dict["timeType"] = extract_timeType_result["result"]
            logger.info(f"------完成timeType识别------")
            user_object.extract_plan = ""

    if "分组条件" in user_object.table_attribute_list:
        user_object.extract_plan = "分组条件"
        user_object.table_attribute_list.remove("分组条件")

    # 抽取分组条件
    if user_object.extract_plan == "分组条件":
        logger.info(f"------开始分组条件识别------")
        time1 = time.time()
        extract_group_result = extract_group(user_object, user_input)
        time2 = time.time()
        print("-----", "group用时：", time2 - time1)
        if extract_group_result["need_multi_turn"]:
            user_object.group_ask_list = extract_group_result["group_ask_list"]

        if len(user_object.group_ask_list) > 0:
            ask_str = user_object.group_ask_list[0]
            user_object.group_ask_list.remove(ask_str)
            user_object.history.append({"group": ask_str})
            return multi_reply(user_object)
        else:
            user_object.extract_plan = ""
            logger.info(f"------完成分组条件识别------")

    if "维度" in user_object.table_attribute_list:
        user_object.extract_plan = "维度"
        user_object.table_attribute_list.remove("维度")
        user_object.jieba_window_phrases2positionList = get_window_phrases(user_object, task="where")  #

    # 抽取维度
    if user_object.extract_plan == "维度":
        logger.info(f"------开始维度识别------")
        time1 = time.time()
        extract_where_result = extract_where(user_object, user_input)
        time2 = time.time()
        print("-----", "where用时：", time2 - time1)
        if extract_where_result["need_multi_turn"]:
            history_dict = {"where": extract_where_result["result"]}
            for k, v in extract_where_result.items():
                if k != "result" and k != "need_multi_turn":
                    history_dict[k] = v
            user_object.history.append(history_dict)
            return multi_reply(user_object)
        else:
            user_object.slot_dict["where"] = extract_where_result["result"]
            logger.info(f"------完成维度识别------")
            user_object.extract_plan = ""

    if len(user_object.slot_dict["intent"]) == 0:
        user_object.extract_plan = "意图"

    if user_object.extract_plan == "意图":
        logger.info(f"------开始分析方式意图识别------")
        time1 = time.time()
        extract_intent_result = extract_intent(user_object, user_input)
        time2 = time.time()
        print("-----", "意图用时：", time2 - time1)
        if extract_intent_result["need_multi_turn"]:
            user_object.history.append({"intent": extract_intent_result["result"]})
            return multi_reply(user_object)
        else:
            user_object.slot_dict["intent"] = extract_intent_result["result"]
            logger.info(f"------完成分析方式识别------")
            user_object.extract_plan = "other"

    if user_object.extract_plan == "other":
        logger.info(f"------开始other识别------")
        extract_other_result = extract_other(user_object, user_input)
        if extract_other_result["need_multi_turn"]:
            user_object.history.append({"other": extract_other_result["result"]})
            return multi_reply(user_object)
        else:
            user_object.extract_plan = ""
            logger.info(f"------完成other识别------")

    reply = final_reply(user_object)
    user_object.history.append({"model": "CompleteOutput"})
    user_object.slot_dict = get_clear_slot_dict()

    return reply