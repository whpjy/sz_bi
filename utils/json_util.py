import ast
import asyncio
import json
import os
import re
import time

from opencc import OpenCC

from config import CURRENT_SCENE, write_lock, LANGUAGE_MODE, t2s, s2t
from config.log_config import logger
from utils.request_util import send_llm, send_embedding_message


def extract_json_from_string(input_string):
    """
    JSON抽取函数
    返回包含JSON对象的列表
    """
    try:
        # 正则表达式假设JSON对象由花括号括起来
        matches = re.findall(r'\{.*?\}', input_string, re.DOTALL)
        # 验证找到的每个匹配项是否为有效的JSON
        valid_jsons = []
        for match in matches:
            try:
                json_obj = json.loads(match)
                valid_jsons.append(json_obj)
            except json.JSONDecodeError:
                continue  # 如果不是有效的JSON，跳过该匹配项
        return valid_jsons[0]
    except Exception as e:
        print(f"Error occurred，从模型输出提取json失败: {e}")
        #  例如这样的 {"exclude": [], "time": [["2024-01", "2024-03"]] 不是完整的json，仍要考虑
        if "time" in input_string:
            matches = re.findall(r'"time":.*\]\]', input_string, re.DOTALL)
            if len(matches) > 0:
                print(f"time数据提取json成功")
                result = json.loads('{' + matches[0] + '}')
                return result

        if "exclude" in input_string:
            matches = re.findall(r'"exclude":.*\]\]', input_string, re.DOTALL)
            if len(matches) > 0:
                print(f"time数据提取json成功")
                result = json.loads('{' + matches[0] + '}')
                return result

        return {}



def extract_list_from_string(input_string):
    """
    list抽取函数
    返回包含list对象的列表
    """
    try:
        # 正则表达式假设JSON对象由花括号括起来
        matches = re.findall(r'\[.*?\]', input_string, re.DOTALL)
        # 验证找到的每个匹配项是否为有效的JSON
        valid_list = []
        for match in matches:
            try:
                parsed_list = ast.literal_eval(match)
                if isinstance(parsed_list, list):
                    valid_list.append(parsed_list)
            except json.JSONDecodeError:
                continue  # 如果不是有效的JSON，跳过该匹配项
        return valid_list
    except Exception as e:
        print(f"Error occurred，从模型输出提取list失败: {e}")
        return []


def find_max_list(s):
    lists = extract_list_from_string(s)

    if not lists:
        return []  # 没有找到任何列表

    # 找出元素数量最多的列表
    max_list = max(lists, key=len)

    return max_list


async def write_indicator_frequency(metric_list):
    path = 'indicator_frequency/' + CURRENT_SCENE + '.json'
    try:
        fr = open(path, 'r', encoding='utf-8')
        indicator_frequency_dict = json.load(fr)
        for metric in metric_list:
            if metric in indicator_frequency_dict.keys():
                indicator_frequency_dict[metric] += 1
            else:
                indicator_frequency_dict[metric] = 1
        indicator_frequency_dict = dict(sorted(indicator_frequency_dict.items(), key=lambda x: x[1], reverse=True))
        async with write_lock:
            with open(path, 'w', encoding='utf-8') as fw:
                json.dump(indicator_frequency_dict, fw, ensure_ascii=False, indent=4)

    except Exception as e:
        print("***********")
        print(e)
        if not os.path.exists(path):
            indicator_frequency_dict = {}
            for metric in metric_list:
                indicator_frequency_dict[metric] = 1
            async with write_lock:
                with open(path, 'w', encoding='utf-8') as fw:
                    json.dump(indicator_frequency_dict, fw, ensure_ascii=False, indent=4)


def detect_text_type(text):
    converter_to_traditional = OpenCC('s2t')  # 简体到繁体
    converter_to_simplified = OpenCC('t2s')  # 繁体到简体

    if converter_to_traditional.convert(text) == text and converter_to_simplified.convert(text) == text:
        # 无法区分时, 根据当前语言模式返回
        if LANGUAGE_MODE == "simplified":
            return "simplified"
        else:
            return "traditional"

    if text == converter_to_traditional.convert(text):
        return "traditional"
    elif text == converter_to_simplified.convert(text):
        return "simplified"



def convert_json_to_simplified(json_data):

    def recursive_convert(data):
        if isinstance(data, dict):
            return {key: recursive_convert(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [recursive_convert(item) for item in data]
        elif isinstance(data, str):
            return t2s.convert(data)
        else:
            return data

    return recursive_convert(json_data)


def convert_json_to_traditional(json_data):
    def recursive_convert(data):
        if isinstance(data, dict):
            return {key: recursive_convert(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [recursive_convert(item) for item in data]
        elif isinstance(data, str):
            return s2t.convert(data)
        else:
            return data

    return recursive_convert(json_data)



def convert_json_to_traditional_final(json_data, origin_targetId2Name):
    # 对最终返回的数据进行有选择的繁体处理

    traditional_json_data = convert_json_to_traditional(json_data)
    # 1、先把target换成原始数据的
    if "value" in json_data.keys():
        if "target" in json_data["value"].keys():
            new_target = []
            for target in json_data["value"]["target"]:
                if target["targetId"] in origin_targetId2Name.keys():
                    new_target.append({"targetId": target["targetId"], "targetName": origin_targetId2Name[target["targetId"]]})
                else:
                    new_target.append(target)

            json_data["value"]["target"] = new_target

    # 2、仅处理需要繁体的部分
    need_traditional = ["timeType", "group", "where", "proportion", "exclude"]
    if "value" in json_data.keys():
        for key in need_traditional:
            if key in json_data["value"].keys():
                json_data["value"][key] = traditional_json_data["value"][key]

    return json_data


def get_table_describe(table_name, columnNameList):

    prompt_table_describe = f'''请你对以下业务域进行50字左右的解释，目的是让非业务人员对该业务域认识和理解。
    业务域名：{table_name}\n'''
    if len(columnNameList) > 0:
        prompt_table_describe = prompt_table_describe + f'''业务域包含的字段列表：{columnNameList[:20]}\n'''
    else:
        prompt_table_describe = prompt_table_describe + f'''不要捏造字段名\n'''

    prompt_table_describe = prompt_table_describe + f'''规则如下:
1、不要输出其他无关内容，不要在开头输出 “这是xxx”、“此业务域xxx”、“该业务域xxx”等类似话术，应该直入主题
2、回答中不要包含 "{table_name}" 等文字
3、不要提到确保xxx，直接解释
4、不要过多的叙述包含的字段，重点是让其他非业务人员明白业务域是什么意思
'''
    table_describe = send_llm(prompt_table_describe)
    return table_describe


# def get_targetName_describe(targetName, columnNameList, targetDefine):
#
#     prompt_table_describe = f'''请你对以下指标进行50字左右的解释，目的是让非业务人员对该指标认识和理解。
#     指标名：{targetName}\n'''
#     if len(columnNameList) > 0:
#         prompt_table_describe = prompt_table_describe + f'''指标包含的字段列表：{columnNameList[:20]}\n'''
#     else:
#         prompt_table_describe = prompt_table_describe + f'''不要捏造字段名\n'''
#
#     prompt_table_describe = prompt_table_describe + f'''规则如下:
# 1、不允许使用 “该指标统计了....”、“该指标展示了....”、“这个指标....”、“xx指标....” 等类似话术，即不需要主语
# 2、回答中不要包含 "{targetName}" 等文字
# 3、不要提到确保xxx，直接解释
# 4、不要过多的叙述包含的字段，重点是让其他非业务人员明白指标是什么意思
# '''
#     targetName_describe = send_llm(prompt_table_describe)
#     return targetName_describe


def get_targetName_describe(targetName, columnNameList, targetDefine):

    prompt_table_describe = f'''请你结合指标名称和指标定义，对指标进行一个专业且准确的自然语言表达。
指标名：{targetName}\n
指标定义：{targetDefine}\n
规则如下:
1、不允许使用 “该指标名为....”、“该指标定义是....”等类似话术，即不需要主语'''
    targetName_describe = send_llm(prompt_table_describe)
    return targetName_describe



async def generate_fake_stream_response(response):
    # 分词模拟逐步返回
    print(response)
    if isinstance(response, dict):
        if "context" in response.keys():
            all_char = ''
            for context_char in str(response["context"]):
                all_char += context_char
                result = response
                response["context"] = all_char
                yield json.dumps(result, ensure_ascii=False).encode('utf-8') + b'\n\n\n'
                await asyncio.sleep(0.01)

            response["DONE"] = "DONE"
            yield json.dumps(response, ensure_ascii=False).encode('utf-8') + b'\n\n\n'
        else:
            yield json.dumps(response, ensure_ascii=False).encode('utf-8') + b'\n\n\n'
    else:
        yield json.dumps(response, ensure_ascii=False).encode('utf-8') + b'\n\n\n'



def query_subject_change(sql1, sql2):
    '''
    判断sql抽取结果中的 FROM 字段是否相同、相互包含
    默认返回 主体改变
    '''

    if "FROM" in sql1.keys() and "FROM" in sql2.keys():
        all_match = True
        for sql2_word in sql2["FROM"]:
            current_match = False
            for sql1_word in sql1["FROM"]:
                if sql1_word == sql2_word:
                    current_match = True
                    break
                elif sql1_word in sql2_word:
                    current_match = True
                    break
                elif sql1_word in sql2_word:
                    current_match = True
                    break
                else:
                    current_match = False

            if not current_match:
                all_match = False
                break

        if all_match:  # 全部匹配，说明主体没改变
            return False

    # 例如 住院人数 和 住院总人数，上述认为不一致，其实一致
    # 通过sql2的from做一次向量召回，如果向量最大值一致，那么还认为不改变
    if len(sql2["FROM"]) > 0:
        embedding_results = send_embedding_message(sql2["FROM"][0], CURRENT_SCENE + "_zhibiao", 0)
        max_value = embedding_results[0]["payload"]["value"]
        logger.info(f"对第二个SQL-FROM做一次指标召回：max_value = {max_value}")

        for sql1_word in sql1["FROM"]:
            if sql1_word == max_value:
                return False
            elif sql1_word in max_value:
                return False
            elif sql1_word in max_value:
                return False
            else:
                continue

    return True


def deal_recommendation_recognition(history):
    multi_recommendation_recognition = {}
    history_copy = history
    i = 0
    for info in history_copy:
        if "metric" in info.keys():
            if len(history) > i+1:
                if "user" in history[i+1].keys():
                    user_input = list(history[i+1].values())[0]
                    multi_recommendation_recognition["metric"] = user_input.split(',')

        elif "table" in info.keys():
            if len(history) > i+1:
                if "user" in history[i+1].keys():
                    user_input = list(history[i + 1].values())[0]
                    multi_recommendation_recognition["table"] = user_input.split(',')

        i = i + 1

    return multi_recommendation_recognition