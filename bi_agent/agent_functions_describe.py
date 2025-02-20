from config import LANGUAGE_MODE
from utils.json_util import convert_json_to_traditional

functions_describe = [
    {
        'name': 'get_small_talk_reply',
        'description': '与用户进行闲聊',
        "parameters": {
            "type": "object",
            "properties": {
                "user_problem": {
                    "type": "string",
                    "description": "用户问题"
                }
            },
            "required": ["user_problem"]
        }
    },
    {
        'name': 'get_instructions',
        'description': '解释如何使用',
        'parameters': {
            'type': 'object',
            'properties': {},
            'required': []}
    },

    {
        'name': 'get_total_indicators_num',
        'description': '获取全体指标数量',
        'parameters': {
            'type': 'object',
            'properties': {},
            'required': []}
    },
    # {
    #     'name': 'get_total_indicators_name',
    #     'description': '直接输出全部指标名称',
    #     'parameters': {
    #         'type': 'object',
    #         'properties': {},
    #         'required': []
    #     }
    # },
    {
        "name": "recommend_relevant_indicators",
        "description": "推荐相关指标",
        "parameters": {
            "type": "object",
            "properties": {
                "key_word": {
                    "type": "string",
                    "description": "用户问题中的关键词"
                }
            },
            "required": ["key_word"]
        }
    },
]

def agent_reply(context, relevantIndicator=[]):

    reply = {
                "type": 2,
                "context": context,
                "mult": [],
                "history": [],
                "slot_status": {},
                "relevantIndicator": relevantIndicator
            }

    return reply


def agent_reply_done(context, relevantIndicator=[]):
    reply = {
        "type": 2,
        "context": context,
        "mult": [],
        "history": [],
        "slot_status": {},
        "relevantIndicator": relevantIndicator,
        "DONE": "DONE"
    }

    return reply

    # if LANGUAGE_MODE == "simplified":
    #     return reply
    # else:
    #     return convert_json_to_traditional(reply)


use_tool_output = {
    "get_small_talk_reply": "⏳ 正在分析 ······",
    "get_total_indicators_num": "⏳ 正在查询指标数量 ······",
    "recommend_relevant_indicators": "⏳ 正在获取相关的指标 ······",
    "get_instructions": "⏳正在查阅使用说明 ······",
    "final_summary_output": "⏳正在总结输出 ······"
    # "get_total_indicators_name": "⏳ 正在查询全部指标信息 ······"
}