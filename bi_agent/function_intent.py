from config import CURRENT_SCENE
from config.log_config import logger
from bi_agent.agent_class import agent_class
from utils.util import get_zhibiao_list


def function_intention_analysis(targetDefine_dict, user_input, user_object):

    logger.info(f"agent")
    new_agent = agent_class()
    new_agent.all_metric_data = user_object.all_metric_data
    new_agent.zhibiao_list = get_zhibiao_list(user_object.all_metric_data)
    if CURRENT_SCENE == "weimeng":
        system_prompt = '''已知：你是基于熵智大语言模型开发的用于数据查询和分析的智能体，旨在通过简单的对话形式为用户提供便捷的数据查询体验。
    你可以解答具体的查询问题如 查询2023年各个部门的在职人数 等。'''
    else:
        system_prompt = '''已知：你是基于熵智大语言模型开发的用于数据查询和分析的智能体，旨在通过简单的对话形式为用户提供便捷的数据查询体验。
    你可以解答具体的查询问题如 查询2023年各个科室的住院人数 等。'''
    new_agent.messages = [{'role': 'system', 'content': system_prompt}]
    new_agent.messages.append({'role': 'user', 'content': user_input})
    new_agent.user_input = user_input

    result = new_agent.agent_entrance(targetDefine_dict)
    return result


def function_intention_analysis_stream(targetDefine_dict, user_input, user_object):

    logger.info(f"进入 Agent")
    new_agent = agent_class()
    new_agent.all_metric_data = user_object.all_metric_data
    new_agent.zhibiao_list = get_zhibiao_list(user_object.all_metric_data)
    if CURRENT_SCENE == "weimeng" or CURRENT_SCENE == "feilida":
        system_prompt = '''已知：你是基于熵智大语言模型开发的用于数据查询和分析的智能体，旨在通过简单的对话形式为用户提供便捷的数据查询体验。
你可以解答具体的查询问题如 查询2023年各个部门的在职人数 等。'''
    else:
        system_prompt = '''已知：你是基于熵智大语言模型开发的用于数据查询和分析的智能体，旨在通过简单的对话形式为用户提供便捷的数据查询体验。
你可以解答具体的查询问题如 查询2023年各个科室的住院人数 等。'''

    new_agent.messages = [{'role': 'system', 'content': system_prompt}]
    new_agent.messages.append({'role': 'user', 'content': user_input})
    new_agent.user_input = user_input

    yield from new_agent.agent_entrance_stream(targetDefine_dict)


