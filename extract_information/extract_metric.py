from config import CURRENT_SCENE
from match.completely_match import get_completely_match
from match.jaccard_match import get_jaccard_match
from match.metric_vector_match import get_metric_vector_match, get_metric_word_vector_match
from utils.util import get_metric_name_list, remove_list_substring, get_target_name_list
from config.log_config import logger


def extract_metric(user_object, user_input):
    metric_name_list = get_metric_name_list(user_object.all_metric_data)
    mul_turn_flag = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "metric" in user_object.history[-2].keys():
            real_input = user_input
            mul_turn_flag = True
        else:
            real_input = user_object.history[0]["user"]

    if not mul_turn_flag:
        # 如果原始输入就是指标，直接确定并结束
        real_input = real_input.strip()
        if real_input in metric_name_list:
            user_object.slot_dict["metric"].append(real_input)
            user_object.metric_recognize_by_phrase.append(real_input)
            extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}
            return extract_metric_result

        target_name_list = get_target_name_list(user_object.all_metric_data)
        if real_input in target_name_list:
            if '-' in real_input:
                metric_list = real_input.split('-')
                if len(metric_list) == 2:
                    metric_name = metric_list[1]
                    if metric_name in metric_name_list:
                        user_object.slot_dict["metric"].append(metric_name)
                        user_object.metric_recognize_by_phrase.append(metric_name)
                        user_object.metric_recognize_by_phrase.append(metric_list[0])
                        extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}
                        return extract_metric_result

            # logger.info(f"全部指标: {metric_name_list}")
        metric_character_match = []
        # metric_character_match = get_metric_jaccard_match(user_object, metric_name_list)
        # for metric in metric_character_match:
        #     if metric not in user_object.slot_dict["metric"]:
        #         user_object.slot_dict["metric"].append(metric)  # 字形匹配到的都认为是指标，更新到槽位
    else:
        logger.info(f"指标识别-多轮-字符相似度")
        metric_completely_match = get_completely_match(real_input, metric_name_list)
        real_input = ',' + real_input  # 防止real_input replace 后为空报错
        for metric_name in metric_completely_match:
            real_input = real_input.replace(metric_name, '')

        metric_jaccard_match = get_jaccard_match(real_input, metric_name_list)
        metric_character_match = set()  # 完全匹配+字形相似度 统称字形匹配
        for word in metric_completely_match + metric_jaccard_match:
            metric_character_match.add(word)

        for metric in metric_character_match:
            if metric not in user_object.slot_dict["metric"]:
                user_object.slot_dict["metric"].append(metric)  # 字形匹配到的都认为是指标，更新到槽位
                if metric in user_object.metric_mul_turn_match2phrase.keys():  # 从推荐的指标选择后记录该片段
                    user_object.metric_recognize_by_phrase.append(user_object.metric_mul_turn_match2phrase[metric])

    metric_vector_match = []
    if real_input not in metric_character_match:  # 当输入是一个完整指标，不需要再用向量
        if not mul_turn_flag:
            logger.info(f"指标识别-非多轮-向量相似度")
            metric_vector_match, no_threshold_match, final_match2phrase = get_metric_vector_match(user_object, CURRENT_SCENE + "_zhibiao")
            user_object.metric_mul_turn_match2phrase = final_match2phrase
        else:
            logger.info(f"指标识别-多轮-向量相似度")
            metric_vector_match, no_threshold_match = get_metric_word_vector_match(real_input, CURRENT_SCENE+"_zhibiao")

    if len(metric_character_match) == 0:  # 字形匹配为空
        if len(metric_vector_match) != 0:  # 语义相似度有值，有值的情况一定多于1个，等于1个的情况在get_metric_vector_match处理了
            # 语义相似度有多个值则进入推荐, 有多个值不需要多轮的情况也在get_metric_vector_match处理了
            if len(user_object.slot_dict["metric"]) == 0:
                extract_metric_result = {"result": "以下具体指标请您选择:" + ','.join(metric_vector_match), "need_multi_turn": True}
            else:
                extract_metric_result = {"result": '您除了想查询指标：(' + ', '.join(user_object.slot_dict["metric"]) + ')，还有想查询的其他指标吗？:没了,' + ','.join(metric_vector_match), "need_multi_turn": True}
        else:  # 语义相似度为空
            if len(user_object.slot_dict["metric"]) == 0:  # 槽位为空则进入多轮推荐（主要是首轮对话）
                extract_metric_result = {"result": "以下具体指标请您选择:" + ','.join(no_threshold_match), "need_multi_turn": True}
            else:  # 槽位有值则结束
                extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}

    else:  # 字形匹配有值
        user_object.slot_dict["metric"] = remove_list_substring(user_object.slot_dict["metric"])  # 去除冗余子串指标
        if len(metric_vector_match) != 0:  # 语义相似度有值，下面去除字形匹配的结果
            metric_new_match = [metric for metric in metric_vector_match if metric not in metric_character_match]
            if len(metric_new_match) == 0:  # 语义相似度没有新值
                extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}
            elif len(metric_new_match) == 1:  # 语义相似度有1个新值，直接选择该值为指标
                if metric_new_match[0] not in user_object.slot_dict["metric"]:
                    user_object.slot_dict["metric"].append(metric_new_match[0])
                extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}
            else:  # 语义相似度有多个新值，进入多轮推荐
                extract_metric_result = {"result": '您除了想查询指标：(' + ', '.join(user_object.slot_dict["metric"]) + ')，还有想查询的其他指标吗？:没了,' + ','.join(metric_new_match), "need_multi_turn": True}
        else:
            extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}

    # 进入多轮前要检查一下是否上个意图和本次意图是否在同个指标之下，可以使用上次意图的结果来避免多余的多轮
    if "metric" in user_object.multi_recommendation_recognition.keys(): # 有metric说明上个意图和本次指标一致
        if isinstance(extract_metric_result["result"], str):
            if "以下具体指标请您选择" in extract_metric_result["result"]:
                recommend_metric_list = extract_metric_result["result"].replace("以下具体指标请您选择:", '').split(',')
                choose_flag = False
                for metric in user_object.multi_recommendation_recognition["metric"]:
                    if metric in recommend_metric_list:
                        if metric not in user_object.slot_dict["metric"]:
                            choose_flag = True
                            user_object.slot_dict["metric"].append(metric)
                            if metric in user_object.metric_mul_turn_match2phrase.keys():  # 从推荐的指标选择后记录该片段
                                user_object.metric_recognize_by_phrase.append(user_object.metric_mul_turn_match2phrase[metric])

                if choose_flag:
                    extract_metric_result = {"result": user_object.slot_dict["metric"], "need_multi_turn": False}

    return extract_metric_result