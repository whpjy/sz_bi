import requests
import json
import time

def get_agg(targetName):
    agg_list = ["合计", "平均", "最大值", "最小值"]
    for agg in agg_list:
        if agg in targetName:
            return agg
    return ''


def get_all_metric_data():

    # url = "http://10.1.24.179:1087/chatAi/createModelJsonV2/1"
    url = "http://192.168.110.160:1088/chatAi/createModelJsonV2/8"
    time1 = time.time()
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

    time2 = time.time()
    print(f"请求fuyiyuan全部指标数据用时", time2 - time1)

    return data_json


def clean_targetName(targetName):
    targetName = targetName.strip().split('-')[-1]
    del_word_list = ['最大值', '最小值', '平均', '合计']
    for del_word in del_word_list:
        if del_word in targetName:
            targetName = targetName.replace(del_word, '')

    return targetName


def get_knowledge_graph(data_json, zhibiao):
    # 定义接口 URL
    time1 = time.time()
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
                            pass
                            # return {"数据错误，columnId：", dimension["columnId"], "在valueJson中不存在"}

    time2 = time.time()
    # print("数据处理用时", time2 - time1)
    return knowledge_graph_dict

def get_origin_zhibiao_list(data_dict):
    zhibiao_list = []
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    targetType = zhibiao["targetType"]
                    if len(targetName.strip()) > 0:
                        zhibiao_list.append(targetName)
                        # zhibiao_list.append("targetType: " + str(targetType))
    return zhibiao_list


metric = "门诊人次"

all_metric_data = get_all_metric_data()
origin_zhibiao_list = get_origin_zhibiao_list(all_metric_data)
metric_knowledge_graph_data = get_knowledge_graph(all_metric_data, metric)

with open('zhibiao_origin_name.txt', 'w', encoding='utf-8') as f:
    for name in origin_zhibiao_list:
        f.write(name + '\n')
#
# with open('clear'
#           'l'
#           '.json', 'w', encoding='utf-8') as f:
#     json.dump(metric_knowledge_graph_data, f, ensure_ascii=False, indent=4)