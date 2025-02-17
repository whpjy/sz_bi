import asyncio
import json
import logging
import os.path

import requests
import time as time_module
import threading
import time

from config import CURRENT_SCENE, SCENE_URL_DICT, LANGUAGE_MODE
from utils.json_util import convert_json_to_simplified, get_table_describe, get_targetName_describe


class BeijingTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        try:
            utc_timestamp = record.created
            local_timestamp = utc_timestamp + 8 * 3600  # 添加8小时的秒数
            time_tuple = time_module.localtime(local_timestamp)
            if datefmt:
                s = time_module.strftime(datefmt, time_tuple)
            else:
                s = time_module.strftime("%Y-%m-%d %H:%M:%S", time_tuple)
                s += ".%03d" % (record.msecs % 1000)
            return s
        except Exception as e:
            print(f"Failed to format time: {e}")
            return super().formatTime(record, datefmt)


logger = logging.getLogger('main_logger')
logger.setLevel(logging.DEBUG)  # 设置最低日志级别
# 创建一个用于记录正常运行信息的处理器
info_handler = logging.FileHandler('./log/public_data_get.log', encoding='utf-8')
info_handler.setLevel(logging.DEBUG)  # 只记录INFO及以上级别的信息
info_handler.setFormatter(BeijingTimeFormatter('%(asctime)s - %(levelname)s - %(message)s'))
# 将处理器添加到日志记录器
logger.addHandler(info_handler)


def get_origin_targetId2Name(update_all_metric_data):
    origin_targetId2Name = {}
    if "targetJson" in update_all_metric_data.keys():
        for target in update_all_metric_data["targetJson"]:
            targetName = target["targetName"]
            targetId = target["targetId"]
            origin_targetId2Name[targetId] = targetName
    return origin_targetId2Name

def run_async_coroutine(coro):
    asyncio.run(coro)


class PublicMetricData:
    def __init__(self):
        self.all_metric_data = {}
        self.origin_targetId2Name = {}  # 这个记录原始请求的结果，而all_metric_data可能是繁体转简体的结果
        self.metric_data_status = False
        self.table_describe = self.load_table_describe()
        self.targetName_describe = self.load_targetName_describe()
        self.targetDefine_dict = {}
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True).start()
        self.table_thread = threading.Thread(target=self.table_describe_logic, daemon=True).start()

    def load_table_describe(self):
        path = 'config/table_describe.json'
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)
            return {}
        else:
            fr = open('config/table_describe.json', 'r', encoding='utf-8')
            table_describe = json.load(fr)
            return table_describe

    def load_targetName_describe(self):
        path = 'config/targetName_describe.json'
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)
            return {}
        else:
            fr = open('config/targetName_describe.json', 'r', encoding='utf-8')
            zhibiao_describe = json.load(fr)
            return zhibiao_describe

    def table_describe_logic(self):
        while True:
            try:
                if self.metric_data_status:
                    if "tableMap" in self.all_metric_data.keys():
                        tableMap = self.all_metric_data["tableMap"]
                        # 找到目前没有记录的表名称
                        new_table_list = []
                        for table_name in list(tableMap.values()):
                            if table_name not in self.table_describe.keys():
                                new_table_list.append(table_name)

                        if len(new_table_list) > 0:
                            tableName2columnName = {}
                            for _, name in self.all_metric_data["tableMap"].items():
                                tableName2columnName[name] = []

                            for info in self.all_metric_data["valueJson"]:
                                id = info["tableId"]
                                name = self.all_metric_data["tableMap"][str(id)]
                                tableName2columnName[name].append(info["columnName"])

                            for table_name in new_table_list:
                                describe = get_table_describe(table_name, tableName2columnName[table_name])
                                self.table_describe[table_name] = describe
                                print(table_name, ": ", describe)
                                time.sleep(1)

                            path = 'config/table_describe.json'
                            with open(path, 'w', encoding='utf-8') as f:
                                json.dump(self.table_describe, f, ensure_ascii=False, indent=4)

                    if "targetJson" in self.all_metric_data.keys():
                        targetJson = self.all_metric_data["targetJson"]
                        new_targetName_list = []
                        targetName2columnName = {}
                        targetName2targetDefine = {}
                        for info in targetJson:
                            targetName = info["targetName"]
                            targetDefine = info["targetDefine"]
                            if targetName not in self.targetName_describe.keys():
                                new_targetName_list.append(targetName)
                                targetName2columnName[targetName] = [info_type["columnName"] for info_type in info["type"]]
                                targetName2targetDefine[targetName] = targetDefine

                        if len(new_targetName_list) > 0:
                            for targetName in new_targetName_list:
                                describe = get_targetName_describe(targetName, targetName2columnName[targetName], targetName2targetDefine[targetName])
                                self.targetName_describe[targetName] = describe
                                print(targetName,": ", describe)
                                time.sleep(1)

                            path = 'config/targetName_describe.json'
                            with open(path, 'w', encoding='utf-8') as f:
                                json.dump(self.targetName_describe, f, ensure_ascii=False, indent=4)

            except Exception as e:
                logger.error(f"程序异常：{e}", exc_info=True)
            time.sleep(10)


    def _update_loop(self):
        while True:
            try:
                update_all_metric_data = self.fetch_new_data()
            except Exception as e:
                update_all_metric_data = {}
                self.metric_data_status = False
                logger.error(f"程序异常：{e}", exc_info=True)

            if len(update_all_metric_data) > 0:
                if "targetJson" in update_all_metric_data.keys():
                    zhibiao_data = update_all_metric_data["targetJson"]
                    # logger.info(f"更新成功，指标数量{len(zhibiao_data)}个")
                    self.all_metric_data = update_all_metric_data
                    self.origin_all_metric_data = update_all_metric_data
                    self.origin_targetId2Name = get_origin_targetId2Name(update_all_metric_data)
                    if LANGUAGE_MODE == "traditional":
                        self.all_metric_data = convert_json_to_simplified(update_all_metric_data)

                    self.targetDefine_dict = self.get_targetDefine(self.all_metric_data)
                    self.metric_data_status = True
                else:
                    logger.info(f"更新失败，targetJson 不是键值")
                    self.metric_data_status = False
            else:
                self.metric_data_status = False

            time.sleep(5)

    def fetch_new_data(self):
        url = SCENE_URL_DICT[CURRENT_SCENE]
        response = requests.get(url)
        if response.status_code == 200:
            data_json = response.json()
        else:
            logger.info(f"请求失败，状态码: {response.status_code}")
            data_json = {}
        return data_json

    def get_targetDefine(self, all_metric_data):
        targetDefine_dict = {}
        if "targetJson" in all_metric_data.keys():
            targetJson = all_metric_data["targetJson"]
            for info in targetJson:
                targetName = info["targetName"]
                if "targetDefine" in info.keys():
                    targetDefine = info["targetDefine"]
                else:
                    targetDefine = ""
                if targetName not in targetDefine_dict.keys():
                    targetDefine_dict[targetName] = targetDefine

        return targetDefine_dict

