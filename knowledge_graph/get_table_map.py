import requests
import json
import time
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


all_metric_data = get_all_metric_data()


with open('table_map.json', 'w', encoding='utf-8') as f:
    json.dump({"tableRelationsMap": all_metric_data["tableRelationsMap"], "tableMap": all_metric_data["tableMap"]}, f, ensure_ascii=False, indent=4)

