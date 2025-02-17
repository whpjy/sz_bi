from utils.json_util import extract_json_from_string
from utils.key_word_rule import rewrite_input_for_aggregation
from utils.prompt import prompt_aggregation_match
from utils.request_util import send_llm


def extract_aggregation(user_object, user_input):

    mul_turn_flag = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "aggregation" in user_object.history[-2].keys():
            real_input = user_input
            mul_turn_flag = True
        else:
            real_input = user_object.history[0]["user"]

    user_object.metric_aggregation = {}
    for metric, table_list in user_object.metric_table.items():
        user_object.metric_aggregation[metric] = {}
        for table_name in table_list:
            if "聚合方式" in user_object.metric_knowledge_graph[metric][table_name].keys():
                aggregation_name_list = user_object.metric_knowledge_graph[metric][table_name]["聚合方式"]
                if len(aggregation_name_list) == 0:
                    user_object.metric_aggregation[metric][table_name] = ''
                elif len(aggregation_name_list) == 1:
                    user_object.metric_aggregation[metric][table_name] = aggregation_name_list[0]
                else:
                    aaggregation_match = prompt_aggregation_match
                    aggregation_match = aaggregation_match.replace('{agg}', str(aggregation_name_list))
                    real_input = rewrite_input_for_aggregation(real_input)
                    aggregation_match = aggregation_match.replace('{question}', real_input)
                    # aggregation_match = aggregation_match.replace('{metric}', metric)
                    example = ''
                    if '合计' in aggregation_name_list:
                        example += '''<user>:2023年基金使用情况
<response>:{"result":"合计"}
<user>:2024年每月统筹基金支出是多少
<response>:{"result":"合计"}\n'''
                    if '平均' in aggregation_name_list:
                        example += '''<user>:2023年基金平均使用情况
<response>:{"result":"平均"}\n'''
                    if '最大值' in aggregation_name_list:
                        example += '''<user>:2023年基金使用最大值
<response>:{"result":"最大值"}\n'''
                    if '最小值' in aggregation_name_list:
                        example += '''<user>:2023年基金使用最小值
<response>:{"result":"最小值"}\n'''

                    aggregation_match = aggregation_match.replace("{example}", example)

                    llm_result = send_llm(aggregation_match)
                    json_result = extract_json_from_string(llm_result)
                    if "result" in json_result.keys():
                        if json_result["result"] in aggregation_name_list:
                            user_object.metric_aggregation[metric][table_name] = json_result["result"]
                        else:
                            if "合计" in aggregation_name_list:
                                user_object.metric_aggregation[metric][table_name] = "合计"
                            else:
                                user_object.metric_aggregation[metric][table_name] = aggregation_name_list[0]
                    else:
                        if "合计" in aggregation_name_list:
                            user_object.metric_aggregation[metric][table_name] = "合计"
                        else:
                            user_object.metric_aggregation[metric][table_name] = aggregation_name_list[0]
            else:
                user_object.metric_aggregation[metric][table_name] = ''

    extract_aggregation_result = {"result": '', "need_multi_turn": False}

    return extract_aggregation_result
