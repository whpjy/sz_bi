from config.log_config import logger
from utils.json_util import extract_json_from_string
from utils.prompt import prompt_extract_time
from utils.request_util import send_llm
from utils.time_util import get_week_info, get_week_start_end, get_last_week_start_end, get_last_two_week_start_end, \
    get_past_7day_start_end, get_current_year, get_last_year, get_year_before_last, remove_non_date_numbers, \
    get_current_date_year, contains_time, get_clean_time, get_current_month_start_end, get_last_month_start_end, \
    get_year_start_to_today, get_past_30day_start_end, extract_dates_time_control


def extract_time(user_object, user_input):

    if len(user_object.history) == 1:  # user_object.history 长度至少为1，且为单数，首尾都为user_input
        real_input = user_object.history[0]["user"]
    else:
        if "time" in user_object.history[-2].keys():
            real_input = user_input
        else:
            real_input = user_object.history[0]["user"]

    datatime_info = get_week_info()
    current_week = get_week_start_end()
    last_week = get_last_week_start_end()
    last_two_week = get_last_two_week_start_end()
    past_7day = get_past_7day_start_end()
    past_30day = get_past_30day_start_end()
    current_month = get_current_month_start_end()
    last_month = get_last_month_start_end()
    year_to_today = get_year_start_to_today()
    current_year = get_current_year()
    last_year = get_last_year()
    year_before_last = get_year_before_last()

    # 先用规则匹配时间，匹配不到再用llm
    user_input = user_input.strip()
    rule_time = None
    time_control = extract_dates_time_control(user_input)
    if time_control != '[]':
        # 如果是时间控件输入
        rule_time = '{"time": ' + str(time_control) + '}'

    if user_input == "近7天":
        rule_time = past_7day
    elif user_input == "近30天":
        rule_time = past_30day
    elif user_input == "本周":
        rule_time = current_week
    elif user_input == "上周":
        rule_time = last_week
    elif user_input == "本月":
        rule_time = current_month
    elif user_input == "上个月":
        rule_time = last_month
    elif user_input == "今年":
        rule_time = year_to_today

    if rule_time is not None:
        logger.info(f"规则确定时间, rule_time: {rule_time}")
        extract_json_result = extract_json_from_string(rule_time)
        clean_time = get_clean_time(extract_json_result["time"])
        logger.info(f"时间处理结果: {clean_time}")
        print("时间处理结果: ", clean_time)
        if len(clean_time) > 0:
            extract_time_result = {"result": clean_time, "need_multi_turn": False}
            return extract_time_result
        extract_time_result = {"result": "请您选择时间范围补充语义:近7天,近30天,本周,上周,本月,上个月,今年", "need_multi_turn": True}
        return extract_time_result
    if rule_time is None:
        real_input = remove_non_date_numbers(real_input)  # 去除输入中的非日期数字
        real_input = real_input.replace("同比", '')
        real_input = real_input.replace("环比", '')
        final_prompt_extract_time = prompt_extract_time.replace("{current_data}", datatime_info)
        final_prompt_extract_time = final_prompt_extract_time.replace("{current_year}", current_year)
        final_prompt_extract_time = final_prompt_extract_time.replace("{last_year}", last_year)
        final_prompt_extract_time = final_prompt_extract_time.replace("{year_before_last}", year_before_last)
        final_prompt_extract_time = final_prompt_extract_time.replace("{current_week}", current_week)
        final_prompt_extract_time = final_prompt_extract_time.replace("{last_week}", last_week)
        final_prompt_extract_time = final_prompt_extract_time.replace("{last_two_week}", last_two_week)
        final_prompt_extract_time = final_prompt_extract_time.replace("{past_7day}", past_7day)
        final_prompt_extract_time = final_prompt_extract_time.replace("{past_30day}", past_30day)
        final_prompt_extract_time = final_prompt_extract_time.replace("{current_month}", current_month)
        final_prompt_extract_time = final_prompt_extract_time.replace("{last_month}", last_month)
        final_prompt_extract_time = final_prompt_extract_time.replace("{year_to_today}", year_to_today)
        final_prompt_extract_time = final_prompt_extract_time.replace("{user_input}", real_input)
        extract_json_result = send_llm(final_prompt_extract_time)

        print("*****", "模型抽取时间：", extract_json_result)
        extract_json_result = extract_json_from_string(extract_json_result)

        year = get_current_date_year()

        if "time" in extract_json_result.keys():
            if isinstance(extract_json_result["time"], list):
                if len(extract_json_result["time"]) == 0:
                    if '今年' in real_input or '本年' in real_input:
                        extract_json_result["time"] = [[f"{year}-01", f"{year}-12"]]
                    if '去年' in real_input:
                        extract_json_result["time"] = [[f"{int(year) - 1 }-01", f"{int(year) - 1 }-12"]]
                    if '前年' in real_input:
                        extract_json_result["time"] = [[f"{int(year) - 2 }-01", f"{int(year) - 2 }-12"]]

        extract_time_result = {"result": "请您选择时间范围补充语义:近7天,近30天,本周,上周,本月,上个月,今年", "need_multi_turn": True}

        if "time" in extract_json_result.keys():
            if contains_time(real_input, extract_json_result):  # 如果用户输入不包含时间词,这可能是大模型幻觉出来的，则舍弃抽取的时间
                clean_time = get_clean_time(extract_json_result["time"])
                print("*****", "规则处理时间：", clean_time)
                if len(clean_time) > 0:
                    extract_time_result = {"result": clean_time, "need_multi_turn": False}

        return extract_time_result