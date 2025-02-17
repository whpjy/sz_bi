import json
import time

import requests
from opencc import OpenCC


def clean_targetName(targetName):
    targetName = targetName.strip().split('-')[-1]
    del_word_list = ['最大值', '最小值', '平均', '合计']
    for del_word in del_word_list:
        if del_word == targetName[-len(del_word):]:
            targetName = targetName.replace(del_word, '')

    return targetName


def get_agg(targetName):
    agg_list = ["合计", "平均", "最大值", "最小值"]
    for agg in agg_list:
        if agg == targetName[-len(agg):]:
            return agg
    return ''


def get_knowledge_graph(data_json, zhibiao):
    # 定义接口 URL
    valueJson = {valueJson["columnId"]:{"values":[value["targetValue"] for value in valueJson["values"]], "columnName":valueJson["columnName"]}
                 for valueJson in data_json["valueJson"]}

    knowledge_graph_dict = {zhibiao: {}}
    if len(data_json) > 0:
        for zhibiao_json in data_json["targetJson"]:
            targetName = zhibiao_json["targetName"]
            cleanName = clean_targetName(targetName)

            if zhibiao == cleanName:
                # 获取表名
                targetName_list = targetName.split('-')
                if len(targetName_list) != 2:
                    continue
                table_name = targetName_list[0]
                if table_name not in knowledge_graph_dict[zhibiao].keys():
                    knowledge_graph_dict[zhibiao][table_name] = {}

                # 获取聚合条件
                agg = get_agg(targetName)
                if len(agg) > 0:
                    if "聚合方式" not in knowledge_graph_dict[zhibiao][table_name].keys():
                        knowledge_graph_dict[zhibiao][table_name]["聚合方式"] = []
                    if agg not in knowledge_graph_dict[zhibiao][table_name]["聚合方式"]:
                        knowledge_graph_dict[zhibiao][table_name]["聚合方式"].append(agg)

                if "time" in zhibiao_json.keys():
                    if len(zhibiao_json["time"]) > 0:
                        if "time" not in knowledge_graph_dict[zhibiao][table_name].keys():
                            knowledge_graph_dict[zhibiao][table_name]["time"] = []
                        for i in range(0, len(zhibiao_json["time"])):
                            if len(zhibiao_json["time"][i]["columnName"].strip()) > 0:
                                if zhibiao_json["time"][i] not in knowledge_graph_dict[zhibiao][table_name]["time"]:
                                    knowledge_graph_dict[zhibiao][table_name]["time"].append(zhibiao_json["time"][i])

                if "group" in zhibiao_json.keys():
                    if "分组条件" not in knowledge_graph_dict[zhibiao][table_name].keys():
                        knowledge_graph_dict[zhibiao][table_name]["分组条件"] = []
                    for group in zhibiao_json["group"]:
                        if group not in knowledge_graph_dict[zhibiao][table_name]["分组条件"]:
                            knowledge_graph_dict[zhibiao][table_name]["分组条件"].append(group)
                        # if group["columnName"] not in knowledge_graph_dict[zhibiao][table_name]["分组条件"]:
                        #     knowledge_graph_dict[zhibiao][table_name]["分组条件"].append(group["columnName"])

                if "type" in zhibiao_json.keys():
                    if "维度" not in knowledge_graph_dict[zhibiao][table_name].keys():
                        knowledge_graph_dict[zhibiao][table_name]["维度"] = {}

                    for dimension in zhibiao_json["type"]:
                        if dimension["columnId"] in valueJson.keys():
                            knowledge_graph_dict[zhibiao][table_name]["维度"][dimension["columnName"]] = {
                                "columnId": dimension["columnId"],
                                "values": valueJson[dimension["columnId"]]["values"]
                            }
                        else:
                            print("数据错误，columnId：", dimension["columnId"], "在valueJson中不存在")
                            pass
                            # return {"数据错误，columnId：", dimension["columnId"], "在valueJson中不存在"}

    # print("数据处理用时", time2 - time1)
    return knowledge_graph_dict



def get_all_metric_data():

    url = 'http://10.1.24.179:1087/chatAi/createModelJsonV2/1'
    # url = 'http://172.18.1.104:1082/chatAi/createModelJsonV2/1'
    # url = 'http://192.168.110.160:1088/chatAi/createModelJsonV2/8'
    # 发送 GET 请求
    response = requests.get(url)
    # 检查请求是否成功
    if response.status_code == 200:
        # 将响应的 JSON 数据存储到本地变量
        data_json = response.json()
        if "targetJson" in data_json.keys():
            zhibiao_data = data_json["targetJson"]
            print(f"请求成功，指标数量{len(zhibiao_data)}个")
        with open('origin_data.json', 'w', encoding='utf-8') as f:
            json.dump(data_json, f, ensure_ascii=False, indent=4)
    else:
        print(f"请求失败，状态码: {response.status_code}")
        data_json = {}

    return data_json


#
all_metric_data = get_all_metric_data()


# def convert_json_to_simplified(json_data):
#     # 初始化 OpenCC 实例，用于繁体转简体
#     cc = OpenCC('t2s')
#
#     def recursive_convert(data):
#         if isinstance(data, dict):
#             return {key: recursive_convert(value) for key, value in data.items()}
#         elif isinstance(data, list):
#             return [recursive_convert(item) for item in data]
#         elif isinstance(data, str):
#             return cc.convert(data)
#         else:
#             return data
#
#     return recursive_convert(json_data)
#
#
# time1 = time.time()
#
# all_metric_data = convert_json_to_simplified(all_metric_data)
#
# time2 = time.time()
#
# print("转换耗时", time2-time1)

#
def get_weidu_dict(data_dict):
    weidu_dict = {}
    if "valueJson" in data_dict.keys():
        weidu_data = data_dict["valueJson"]
        if isinstance(weidu_data, list):
            for weidu in weidu_data:
                if "columnId" in weidu.keys() and "values" in weidu.keys():
                    values_list = []
                    weidu_value = weidu["values"]
                    if isinstance(weidu_value, list):
                        for value in weidu_value:
                            if "targetValue" in value.keys():
                                values_list.append(value["targetValue"])
                    weidu_dict[weidu["columnId"]] = values_list
                    print(weidu["columnName"], ": ", len(values_list))

    return weidu_dict

get_weidu_dict(all_metric_data)

# print("构建指标：\n")
# zhibiao_dict = get_knowledge_graph(all_metric_data, "平均住院天数")
# print(zhibiao_dict)
#
# for info in all_metric_data["targetJson"]:
#     targetName = info["targetName"]
#     print(targetName)
