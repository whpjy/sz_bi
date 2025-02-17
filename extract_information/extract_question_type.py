from config.log_config import logger
from extract_information.extract_intent import related_intent_judge
from utils.util import get_target_name_list


def extract_question_type(user_input, all_metric_data):

    logger.info(f"------开始问题类型判断------")

    target_name_list = get_target_name_list(all_metric_data)
    user_input = user_input.strip()
    for target_name in target_name_list:
        if user_input in target_name:
            question_type = "数据查询或统计分析"
            logger.info(f"user_input属于 “{target_name}” ，直接确定：{question_type}")
            return question_type
        if ',' in user_input:
            input_split = user_input.split(',')
            for input in input_split:
                if input.strip() in target_name:
                    question_type = "数据查询或统计分析"
                    logger.info(f"user_input属于 “{target_name}” ，直接确定：{question_type}")
                    return question_type

    question_type = related_intent_judge(user_input)
    logger.info(f"意图识别结果：{question_type}")
    return question_type

    #
    # if related_intent == '数据查询或统计分析':
    #     logger.info(f"------完成意图识别------")
    #     user_object.extract_plan = "指标"
    #
    # elif related_intent == '系统功能咨询':
    #     logger.info(f"进入agent...")
    #     user_object.history.append({"model": "CompleteOutput"})
    #     if stream:
    #         yield from function_intention_analysis(user_input, user_object, stream)
    #     else:
    #         answer = function_intention_analysis(user_input, user_object, stream)
    #         return function_intent_reply(answer)
    # else:
    #
    #     user_object.history.append({"model": "CompleteOutput"})
    #     if stream:
    #         yield from function_intention_analysis(user_input, user_object, stream)
    #     else:
    #         print("----------")
    #         answer = function_intention_analysis(user_input, user_object, stream)
    #         print(answer)
    #         return function_intent_reply(answer)