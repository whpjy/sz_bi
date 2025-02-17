from config import CURRENT_SCENE
from match.group_vector_match import extract_keshi_bianma
from utils.json_util import extract_json_from_string
from utils.key_word_rule import extract_compare, extract_rank, extract_limits
from utils.prompt import prompt_extract_zhanbi_word
from utils.request_util import send_llm
from utils.time_util import huanbi_time_clean, tongbi_time_clean, judge_time_group_exist, cal_time_span
from utils.util import judge_group_name_exist, judge_where_group_name_exist, get_keshi_bianma_name_id


def extract_other(user_object, user_input):

    mul_turn_flag = False
    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "other" in user_object.history[-2].keys():
            real_input = user_input
            mul_turn_flag = True
        else:
            real_input = user_object.history[0]["user"]

    if not mul_turn_flag:

        # 如果group或者where中的维度已经识别到“科室名称” 并且自身group_name_list存在 入院科室编码、出院科室编码、再转科室编码等
        group_name_exist_flag = judge_group_name_exist(user_object, "科室名称")
        where_group_name_exist_flag = judge_where_group_name_exist(user_object, "科室名称")
        if group_name_exist_flag or where_group_name_exist_flag:
            keshi_bianma_list = get_keshi_bianma_name_id(user_object)  # xx科室编码 存在
            if len(keshi_bianma_list) > 0:
                keshi_bianma = extract_keshi_bianma(user_object, CURRENT_SCENE + "_group", keshi_bianma_list)
                if len(keshi_bianma) > 0:
                    user_object.slot_dict["group"].append(keshi_bianma[0])

        compare = extract_compare(real_input)
        if len(compare) > 0:
            user_object.slot_dict["compare"] = compare

        rank = extract_rank(real_input)
        if len(rank) > 0:
            user_object.slot_dict["rank"] = rank

        limits = extract_limits(real_input)
        if len(limits) > 0:
            user_object.slot_dict["limits"] = limits

        if "环比分析" in user_object.slot_dict["intent"]:
            if "环比" not in user_object.slot_dict["compare"]:
                user_object.slot_dict["compare"] = ["环比"]
            user_object.slot_dict["time"] = huanbi_time_clean(user_object.slot_dict["time"])
            print("环比时间处理：", user_object.slot_dict["time"])

        if "同比分析" in user_object.slot_dict["intent"]:
            if "同比" not in user_object.slot_dict["compare"]:
                user_object.slot_dict["compare"] = ["同比"]
            user_object.slot_dict["time"] = tongbi_time_clean(user_object.slot_dict["time"])
            print("同比时间处理：", user_object.slot_dict["time"])

        zhanbi_flag = False
        for metric, table_list in user_object.metric_table.items():
            if "占比" in metric:
                zhanbi_flag = True

        if "占比分析" in user_object.slot_dict["intent"] and not zhanbi_flag:
            user_object.slot_dict["compare"] = ["占比"]

            where_list = user_object.slot_dict["where"]
            if len(where_list) == 1:
                user_object.slot_dict["proportion"] = where_list

            if len(where_list) >= 2:
                targetValue_list = [where["targetValue"] for where in where_list]
                prompt_zhanbi = prompt_extract_zhanbi_word
                prompt_zhanbi = prompt_zhanbi.replace("{user_input}", real_input)
                prompt_zhanbi = prompt_zhanbi.replace("{targetValue_list}", str(targetValue_list))
                llm_answer = send_llm(prompt_zhanbi)
                extract_json_result = extract_json_from_string(llm_answer)
                if "exclude" in extract_json_result.keys():
                    if isinstance(extract_json_result["exclude"], list):
                        exclude_list = extract_json_result["exclude"]
                        for where in where_list:
                            if where["targetValue"] in exclude_list:
                                user_object.slot_dict["proportion"].append(where)

        # 2023年12月7日住院人数变化趋势 [[2023-12-07]]
        # 2023年12月住院人数变化趋势 [[2023-12]]
        # 2023年住院人数变化趋势

        extract_other_result = {"result": '', "need_multi_turn": False}

        # 如果是趋势分析 + group没time_group值 -> group能确定的确定，不能确定的进多轮
        if "趋势分析" in user_object.slot_dict["intent"]:
            time_group_exist = judge_time_group_exist(user_object)  # 判断group有没有时间分组词
            if not time_group_exist:
                time_result = user_object.slot_dict["time"]
                if len(time_result) == 1:  # 只考虑[[x]]和[[x,y]]这样，不考虑[[],[]...]
                    if len(time_result[0]) == 1:  # 1 [[x]]
                        time_split = time_result[0][0].split('-')
                        # 1.1 [[x]] -> [[yy-mm-dd]] 此时时间改成当月，time_group给日
                        if len(time_split) == 3:
                            user_object.slot_dict["time"] = [[time_split[0] + '-' + time_split[1]]]
                            user_object.slot_dict["group"].append({"columnId": -1, "columnName": "天"})
                        # 1.2 [[x]] -> [[yy-mm]] 此时进入多轮，询问[周, 日]
                        if len(time_split) == 2:
                            extract_other_result = {"result": "您想按照哪种时间周期展示数据:周,天", "need_multi_turn": True}
                    if len(time_result[0]) == 2:  # 2 [[x,y]]
                        time_span = cal_time_span(time_result)
                        if len(time_span) > 0:
                            if time_span == '年':
                                extract_other_result = {"result": "您想按照哪种时间周期展示数据:年,季度,月,周,天", "need_multi_turn": True}
                            elif time_span == '季度':
                                extract_other_result = {"result": "您想按照哪种时间周期展示数据:季度,月,周,天", "need_multi_turn": True}
                            elif time_span == '月':
                                extract_other_result = {"result": "您想按照哪种时间周期展示数据:月,周,天", "need_multi_turn": True}
                            elif time_span == '周':
                                extract_other_result = {"result": "您想按照哪种时间周期展示数据:周,天", "need_multi_turn": True}
                            elif time_span == '天':  # 直接确定
                                user_object.slot_dict["group"].append({"columnId": -1, "columnName": "天"})

    else:
        if real_input == '年':
            user_object.slot_dict["group"].append({"columnId": -1, "columnName": "年"})
        elif real_input == '季度':
            user_object.slot_dict["group"].append({"columnId": -1, "columnName": "季度"})
        elif real_input == '月':
            user_object.slot_dict["group"].append({"columnId": -1, "columnName": "月"})
        elif real_input == '周':
            user_object.slot_dict["group"].append({"columnId": -1, "columnName": "周"})
        elif real_input == '天':
            user_object.slot_dict["group"].append({"columnId": -1, "columnName": "天"})

        extract_other_result = {"result": '', "need_multi_turn": False}

    return extract_other_result