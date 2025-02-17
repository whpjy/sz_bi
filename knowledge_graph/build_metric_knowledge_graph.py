import requests
import json
import time
from py2neo import Graph
from py2neo import Node, Relationship
from config import SCENE_URL_DICT, CURRENT_SCENE


def get_agg(targetName):
    agg_list = ["合计", "平均", "最大值", "最小值"]
    for agg in agg_list:
        if agg == targetName[-len(agg):]:
            return agg
    return ''


def get_all_metric_data():

    url = SCENE_URL_DICT[CURRENT_SCENE]
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
    print(f"请求{CURRENT_SCENE}全部指标数据用时", time2 - time1)

    return data_json


def clean_targetName(targetName):
    targetName = targetName.strip().split('-')[-1]
    del_word_list = ['最大值', '最小值', '平均', '合计']
    for del_word in del_word_list:
        if del_word == targetName[-len(del_word):]:
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


def build(data_json):
    graph = Graph('bolt://localhost:7687', auth=('neo4j', '12345678'))
    graph.delete_all()
    zhibiao_name = list(data_json.keys())[0]
    # 定义node
    node_zhibiao = Node('指标', name=zhibiao_name)
    graph.create(node_zhibiao)

    table_list = list(data_json[zhibiao_name].keys())
    for table_name in table_list:
        node_table = Node('数据表', name=table_name)
        graph.create(node_table)
        node_zhibiao_to_table = Relationship(node_zhibiao, '所属', node_table)
        graph.create(node_zhibiao_to_table)

        attribute_list = list(data_json[zhibiao_name][table_name].keys())
        for attribute_name in attribute_list:
            node_attribute = Node('条件属性', name=attribute_name)
            graph.create(node_attribute)
            node_table_to_attribute = Relationship(node_table, '拥有', node_attribute)
            graph.create(node_table_to_attribute)

            attribute_value = data_json[zhibiao_name][table_name][attribute_name]
            if isinstance(attribute_value, list):
                for value in attribute_value:
                    if isinstance(value, dict):
                        node_attribute_value = Node('值', name=value["columnName"])
                    else:
                        node_attribute_value = Node('值', name=value)
                    graph.create(node_attribute_value)
                    node_attribute_to_value = Relationship(node_attribute, '包含', node_attribute_value)
                    graph.create(node_attribute_to_value)
                    if attribute_name == "分组条件":
                        label_list = value["labels"]
                        if len(label_list) > 0:
                            for label in label_list:
                                node_attribute_label = Node('标签', name=label)
                                node_value_to_label = Relationship(node_attribute_value, '标签', node_attribute_label)
                                graph.create(node_value_to_label)


            if isinstance(attribute_value, dict):
                dimension_list = list(attribute_value.keys())
                for dimension_name in dimension_list:
                    node_dimension = Node('维度属性', name=dimension_name)
                    graph.create(node_dimension)
                    node_attribute_to_dimension = Relationship(node_attribute, '拥有', node_dimension)
                    graph.create(node_attribute_to_dimension)

                    dimension_value = attribute_value[dimension_name]["values"]
                    if isinstance(dimension_value, list):
                        if dimension_name == "医院名称":
                            continue
                        for v in dimension_value:
                            node_dimension_value = Node('值', name=v)
                            graph.create(node_dimension_value)
                            node_dimension_to_value = Relationship(node_dimension, '包含', node_dimension_value)
                            graph.create(node_dimension_to_value)

def get_origin_zhibiao_list(data_dict):
    zhibiao_list = []
    if "targetJson" in data_dict.keys():
        zhibiao_dict = data_dict["targetJson"]
        if isinstance(zhibiao_dict, list):
            for zhibiao in zhibiao_dict:
                if "targetName" in zhibiao.keys():
                    targetName = zhibiao["targetName"]
                    if len(targetName.strip()) > 0:
                        zhibiao_list.append(targetName)
    return zhibiao_list


metric = "病案数量"

all_metric_data = get_all_metric_data()

# origin_zhibiao_list = get_origin_zhibiao_list(all_metric_data)
# metric_knowledge_graph_data = get_knowledge_graph(all_metric_data, metric)
# build(metric_knowledge_graph_data)
#
# with open('zhibiao_origin_name.txt', 'w', encoding='utf-8') as f:
#     for name in origin_zhibiao_list:
#         f.write(name + '\n')
#
# with open('metric_knowledge_graph.json', 'w', encoding='utf-8') as f:
#     json.dump(metric_knowledge_graph_data, f, ensure_ascii=False, indent=4)

