# encoding=utf-8
import json
import time

from qwen_agent.llm import get_chat_model
from config import MODEL_NAME, LLM_MODEL_CFG_URL, CURRENT_SCENE
from config.log_config import logger
from bi_agent.agent_functions_describe import functions_describe, agent_reply, use_tool_output, agent_reply_done
from utils.agent_util import get_relevantIndicator_targetId
from utils.json_util import find_max_list
from utils.request_util import send_llm, get_bge_reranker, send_llm_stream
from utils.util import get_zhibiao_list, get_zhibiao_sort_by_type


class general_agent_class:

    def __init__(self):
        self.all_metric_data = {}  # 存储全部的实时数据
        self.user_input = ''
        self.messages = []
        self.zhibiao_list = []
        self.relevantIndicator = []
        self.functions_use_list = []
        self.functions = functions_describe
        self.targetDefine_dict = {}
        self.used_function_list = []

    def get_small_talk_reply(self):
        """与用户进行闲聊"""
        if CURRENT_SCENE == "weimeng" or CURRENT_SCENE == "feilida":
            prompt = '''我是基于熵智大语言模型开发的一个平台，旨在通过简单的对话形式为用户提供便捷的数据查询体验。
你可以解答的问题有 平台如何使用、有多少指标、我想了解xx应该查询什么指标等等。以及具体的问题如：查询2023年各个部门的在职人数 等\n'''
        else:
            prompt = '''我是基于熵智大语言模型开发的一个平台，旨在通过简单的对话形式为用户提供便捷的数据查询体验。
你可以解答的问题有 平台如何使用、有多少指标、我想了解xx应该查询什么指标等等。 以及具体的问题如：查询2023年各个科室的住院人数 等\n\n'''

        return f'回答：{prompt}'

    def get_total_indicators_num(self):
        """获取全部指标数量"""
        return str({'指标数量': len(self.zhibiao_list)})

    # def get_total_indicators_name(self):
    #     """输出全部指标名称"""
    #     zhibiao_list = get_zhibiao_sort_by_type(self.all_metric_data)
    #     return str({"全部指标": zhibiao_list})

    def get_instructions(self):
        """获取使用说明"""
        if CURRENT_SCENE == "weimeng" or CURRENT_SCENE == "feilida":
            return str({'标准的使用说明，最终需要Markdown形式输出数据': '''欢迎使用智能数据查询助手，为了让您的查询更加精准有效，请参考以下指南来构建您的问题。
## 如何构造问题
1. **明确想要了解的具体信息**
  在提问时，请明确指出您感兴趣的特定数据或统计信息。例如：“2024年各个部门的在职人数是多少？”这里的“在职人数”就是您想要查询的具体信息。请确保这一关键信息清晰且具体。
2. **设定查询条件与范围**
  请为您的查询提供必要的条件和时间范围，这有助于缩小搜索范围并获得更精确的结果。例如：“列出从上个月1日至今日在职人数超过5人的所有部门”。”这里的“时间区间”、“在职人数阈值”以及“部门”都是重要的筛选条件。
3. **指定排序、分组等额外需求**
  如果需要对结果进行排序或分组，请明确这些要求。例如：“去年客户服务部门每位员工处理的请求数量，按升序排列。”这里“按升序排列”是特别的展示需求。
## 示例问题
- 查询张经理在过去一年里每月的销售订单数趋势。
- 提取最近一个月内客服部门处理的客户咨询总数。
- 对比去年各区域产生的总销售额。
## 注意事项
- 尽量使用完整句子来表达您的查询请求。
- 提供充足的信息以确保系统能准确理解您的查询意图。
- 若初次提问不够具体，可以先提出较为宽泛的问题，根据初步结果再逐步细化。

如果您有任何疑问或者需要进一步的帮助，请随时咨询。\n'''})
        else:
            return str({'标准的使用说明，最终需要Markdown形式输出数据': '''欢迎使用智能数据查询助手，为了让您的查询更加精准有效，请参考以下指南来构建您的问题。
## 如何构造问题
1. **明确想要了解的具体信息**
  在提问时，请明确指出您感兴趣的特定数据或统计信息。例如：“2024年各个科室的住院人数是多少？”这里的“住院人数”就是您想要查询的具体信息。请确保这一关键信息清晰且具体。
2. **设定查询条件与范围**
  请为您的查询提供必要的条件和时间范围，这有助于缩小搜索范围并获得更精确的结果。例如：“列出从上个月1日至今日手术人数超过5人的所有科室”。”这里的“时间区间”、“手术人数阈值”以及“科室”都是重要的筛选条件。
3. **指定排序、分组等额外需求**
  如果需要对结果进行排序或分组，请明确这些要求。例如：“去年普外科各个医生处理的病案数，按升序排列。”这里“按升序排列”是特别的展示需求。
## 示例问题
- 查询张医生在过去一年里每月的手术人数趋势。
- 提取最近一个月内普外科门诊接待的患者总数。
- 对比去年各病区产生的总费用。
## 注意事项
- 尽量使用完整句子来表达您的查询请求。
- 提供充足的信息以确保系统能准确理解您的查询意图。
- 若初次提问不够具体，可以先提出较为宽泛的问题，根据初步结果再逐步细化。

如果您有任何疑问或者需要进一步的帮助，请随时咨询。\n'''})

    # def get_total_indicators_name(self):
    #     """输出全部指标名称"""
    #     zhibiao_list = get_zhibiao_sort_by_type(self.all_metric_data)
    #     return str({"全部指标": zhibiao_list})

    def recommend_relevant_indicators(self):
        zhibiao_list = get_zhibiao_sort_by_type(self.all_metric_data)
        prompt = f'''你是一个抽取关键词的助手，我下一步需要根据关键词计算相关指标，现在需要你从用户问题中提取一个关键词
用户问题：{self.user_input}
要求：
1、直接输出最终的一个关键词，不要输出其他解释性的内容
'''
        key_word = send_llm(prompt)
        key_word = key_word.replace('指标', '')
        print("     Tool Process计算相关指标-先抽取关键词:", key_word)
        logger.info(f"     Tool Process计算相关指标-先抽取关键词: {key_word}")
        prompt_type = '关键词'
        if "相关" in key_word:
            key_word = self.user_input
            prompt_type = '用户问题'
        if len(key_word) > 0:
            rerank_result = get_bge_reranker(key_word, zhibiao_list)
            zhibiao_list = list(rerank_result.keys())[:30]
        else:
            return str({'用户问题': self.user_input, '相关的指标': []})

        prompt = f'''所有指标如下：
{str(zhibiao_list)}
{prompt_type}：{key_word}
请问{prompt_type}涉及的相关指标有哪些？
要求如下：
1、请输出最相关的指标
2、最终输出以列表 ['x', 'y', ...] 的形式输出，如果不存在要返回空列表 []
3、不要输出其他的分析内容'''
        result = send_llm(prompt)
        relevantIndicatorName_list = find_max_list(result)
        self.relevantIndicator = get_relevantIndicator_targetId(self.targetDefine_dict, relevantIndicatorName_list, self.all_metric_data)
        return f'相关的指标: {result}'

    def choose_function(self):
        exist_function_name_list = []
        exist_function_list = []
        for info in self.functions:
            if info['name'] not in self.used_function_list:
                exist_function_name_list.append(info['name'])
                exist_function_list.append({'name': info['name'], 'description': info['description']})

        prompt = f'''你是一个工具函数选择助手，规划选择解答用户问题所需要使用的工具函数。
你不需要解答用户的具体问题，选择一个或多个工具函数名称放到一个列表中。

用户问题：{self.user_input}
可选择的工具函数：{exist_function_list}

要求：
1、以json可解析的列表形式输出最终结果
2、至少选择一个工具函数
3、不要输出其他解释性的内容
'''
        result = send_llm(prompt)
        result = find_max_list(result)
        final_list = [tool_name for tool_name in result if tool_name in exist_function_name_list]
        print("所需工具列表:", final_list)
        logger.info(f"所需工具列表: {final_list}")
        return final_list

    def final_summary_output(self):
        prompt = f'''根据提供的信息组织语言解答用户的原始问题。
当前历史：{self.messages}
用户原始问题：{self.user_input}
要求：
详细输出对用户原始问题的回答
'''
        pre_chunk = ''
        for chunk in send_llm_stream(prompt):
            if pre_chunk != chunk:
                pre_chunk = chunk
                data = agent_reply(pre_chunk)
                yield json.dumps(data, ensure_ascii=False).encode('utf-8') + b'\n\n\n'

        self.messages.append({
            'role': 'assistant',
            'content': pre_chunk
        })

    def agent_entrance_stream(self, targetDefine_dict):

        self.targetDefine_dict = targetDefine_dict
        print("*" * 20, "Loop Use Tool", "*" * 20)
        logger.info(f"{'*' * 20} Loop Use Tool {'*' * 20}")

        available_functions = {
            'get_small_talk_reply': self.get_small_talk_reply,
            'get_total_indicators_num': self.get_total_indicators_num,
            'recommend_relevant_indicators': self.recommend_relevant_indicators,
            'get_instructions': self.get_instructions
            # 'get_total_indicators_name': self.get_total_indicators_name
        }

        function_list = self.choose_function()

        for function_name in function_list:
            print("#### Request Tool:", function_name)
            logger.info(f"#### Request Tool: {function_name}")
            # data = agent_reply(use_tool_output[function_name])
            # yield json.dumps(data) + "\n"

            function_to_call = available_functions.get(function_name)
            function_response = function_to_call()

            print('**** Tool Response:', function_response)
            logger.info(f"**** Tool Response: {function_response}")
            self.used_function_list.append(function_name)
            self.messages.append({
                'role': 'function',
                'name': function_name,
                'content': function_response
            })

        yield from self.final_summary_output()

        data = agent_reply_done(self.messages[-1]['content'], self.relevantIndicator)
        yield json.dumps(data, ensure_ascii=False).encode('utf-8') + b'\n\n\n'
