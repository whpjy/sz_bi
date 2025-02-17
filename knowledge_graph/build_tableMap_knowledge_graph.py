import requests
import json
import time
from py2neo import Graph
from py2neo import Node, Relationship
from config import SCENE_URL_DICT, CURRENT_SCENE


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


def build(data_json):
    graph = Graph('bolt://localhost:7687', auth=('neo4j', '12345678'))
    graph.delete_all()

    exist_tableName2Node = {}
    tableRelationsMap = data_json["tableRelationsMap"]
    tableMap = data_json["tableMap"]

    for tableId, tableRelationsList in tableRelationsMap.items():
        tableName = tableMap[tableId]
        if tableName not in exist_tableName2Node.keys():
            node_table = Node('数据表', name=tableName, id=tableId)
            graph.create(node_table)
            exist_tableName2Node[tableName] = node_table

        node_table = exist_tableName2Node[tableName]

        for RelationTableId in tableRelationsList:
            tableName = tableMap[str(RelationTableId)]
            if tableName not in exist_tableName2Node.keys():
                relation_table = Node('数据表', name=tableName, id=RelationTableId)
                graph.create(relation_table)
                exist_tableName2Node[tableName] = relation_table

            relation_table = exist_tableName2Node[tableName]

            table_relation = Relationship(node_table, '关联', relation_table)
            graph.create(table_relation)


all_metric_data = get_all_metric_data()
# build(all_metric_data)
