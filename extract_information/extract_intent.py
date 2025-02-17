import re
from utils.prompt import prompt_intent_judge, prompt_intent_analysis
import datetime

from utils.request_util import send_llm
from utils.time_util import extract_group_time


def unrelated_intention_analysis():
    result = '您提供的信息不足或非业务场景，无法判断指标。'
    return result


def related_intent_judge(user_input):
    new_intent_prompt = prompt_intent_judge.replace('{question}', user_input)
    llm_result = send_llm(new_intent_prompt)

    if "数据查询或统计分析" in llm_result:
        return '数据查询或统计分析'
    elif "系统功能咨询" in llm_result:
        return '系统功能咨询'
    else:
        return '其他'


from datetime import datetime, timedelta
def is_day_adjacent(day1, day2):
    """判断两天是否相邻"""
    return abs((day2 - day1).days) == 1


def is_month_adjacent(month1, month2):
    """判断两个月份是否相邻"""
    return abs(month1 - month2) == 1


def check_adjacent_quarters(input_text):
    """检查两个季度是否相邻"""
    quarter_pattern = r'(\d+|一|二|三|四)季度'
    matches = re.findall(quarter_pattern, input_text)
    if not matches:
        return None

    digit_map = {"一": 1, "二": 2, "三": 3, "四": 4}
    quarters = [int(digit_map.get(match, match)) for match in matches]

    if len(quarters) == 2:
        q1, q2 = sorted(quarters)
        if q2 - q1 == 1:
            return '环比分析'
        else:
            return '对比分布分析'
    return None


def check_adjacent_weeks(input_text):
    """检查周是否相邻并返回分析结果"""
    week_keywords = {
        "这周": 0,
        "本周": 0,
        "这星期": 0,
        "上周": -1,
        "上星期": -1,
        "下星期": 1,
        "下周": 1,
        "上上周": -2,
        "下下周": 2,
        "下一周": 1,
        "上一周": 1,
        "前一周": 1,
        "后一周": 1
    }

    week_matches = [week_keywords[key] for key in week_keywords if key in input_text]

    if len(week_matches) == 2:
        w1, w2 = sorted(week_matches)
        if abs(w2 - w1) == 1:
            return '环比分析'
        else:
            return '对比分布分析'
    return None


def tongbi_analyze_time_periods(time_list):

    if len(time_list) == 2:
        if len(time_list[0]) == 1 and len(time_list[1]) == 1:
            t0_split = time_list[0][0].split('-')
            t1_split = time_list[1][0].split('-')
            if len(t0_split) == 2 and len(t1_split) == 2:
                if t0_split[1] != t1_split[1]:
                    return '对比分布分析'

    return '同比分析'


def huanbi_analyze_time_periods(time_list, origin_input):
    try:
        """分析时间段，并确定是同比分析还是对比分布分析"""
        # 处理季度相邻性
        quarter_analysis = check_adjacent_quarters(origin_input)
        if quarter_analysis:
            return quarter_analysis

        # 处理周相邻性
        week_analysis = check_adjacent_weeks(origin_input)
        if week_analysis:
            return week_analysis

        # 处理日期和月份的相邻性
        days = []
        months = []

        for time_period in time_list:
            if len(time_period) == 1:
                # 处理单一日期或月份
                if len(time_period[0]) == 7:  # 例如 '2023-07'
                    month = datetime.strptime(time_period[0], '%Y-%m').month
                    months.append(month)
                elif '-' in time_period[0]:  # 例如 '2023-12-05'
                    date_obj = datetime.strptime(time_period[0], '%Y-%m-%d')
                    days.append(date_obj)
            elif len(time_period) == 2:
                # 处理日期或月份范围
                if len(time_period[0]) == 7 and len(time_period[1]) == 7:  # 例如 ['2023-07', '2023-08']
                    start_month = datetime.strptime(time_period[0], '%Y-%m').month
                    end_month = datetime.strptime(time_period[1], '%Y-%m').month
                    if is_month_adjacent(start_month, end_month):
                        return '环比分析'
                    else:
                        return '对比分布分析'
                elif '-' in time_period[0] and '-' in time_period[1]:
                    start_date = datetime.strptime(time_period[0], '%Y-%m-%d')
                    end_date = datetime.strptime(time_period[1], '%Y-%m-%d')
                    if is_day_adjacent(start_date, end_date):
                        return '环比分析'
                    else:
                        return '对比分布分析'

        # 检查单一日期或月份的相邻性
        if len(days) == 2:
            if is_day_adjacent(days[0], days[1]):
                return '环比分析'
            else:
                return '对比分布分析'

        if len(months) == 2:
            if is_month_adjacent(months[0], months[1]):
                return '环比分析'
            else:
                return '对比分布分析'

        return '对比分布分析'
    except Exception as e:
        print(f"Error in analyze_time_periods: {e}")

    return '对比分布分析'


def extract_intent(user_object, user_input):
    origin_input = user_object.history[0]["user"]
    group_time_list = extract_group_time(origin_input)

    for group_time in group_time_list:
        user_object.slot_dict["group"].append({"columnId": -1, "columnName": group_time})
    group_list = user_object.slot_dict["group"]
    time_list = user_object.slot_dict["time"]

    # 定义关键词和优先级
    keyword_intents = {
        '趋势分析': ['变化趋势', '走势', '周期', '趋势'],
        '波动分析': ['波动趋势', '异常', '变动情况', '波动'],
        '同比分析': ['同比', '同期'],
        '环比分析': ['环比', '上期'],
        '占比分析': ['占比', '饼图', '组成', '份额', '比率', '比率分布'],
        '排名分析': ['最高', '最低', '排名', '从高到低', '从低到高', '最多', '最少', '前几', '后几', '哪几个', '排序',
                     '名次'],
        '对比分布分析': ['数量分布', '数据形状', '比例分布', '对比分布', '对比分析', '对比'],
        '数值统计': ['合计', '数量有多少', '总计', '数量统计', '数统计']
    }

    # 定义优先级，数字越小优先级越高
    priority = {
        '趋势分析': 2,
        '波动分析': 2,
        '同比分析': 1,
        '环比分析': 1,
        '占比分析': 2,
        '排名分析': 2,
        '对比分布分析': 2,
        '数值统计': 3  # 数值统计优先级最低
    }

    matched_intents = {}
    for intent, keywords in keyword_intents.items():
        matched_keywords = [keyword for keyword in keywords if keyword in origin_input]
        if matched_keywords:
            matched_length = sum(len(keyword) for keyword in matched_keywords)
            matched_intents[intent] = (matched_length, priority[intent])


    # 如果只匹配到一种类型的关键词，直接返回该类型
    if len(matched_intents) == 1:
        completely_match = list(matched_intents.keys())
    else:
        completely_match = []
        if matched_intents:
            # 如果匹配到多个类型的关键词，先按优先级排序，再按匹配关键词总长度排序
            sorted_matched_intents = sorted(matched_intents.items(), key=lambda x: (x[1][1], -x[1][0]))
            highest_priority = sorted_matched_intents[0][1][1]

            # 从排序后的列表中选择优先级最高的分析方式
            completely_match = [intent for intent, (length, priority) in sorted_matched_intents if
                                priority == highest_priority]

    print("*****", "关键词匹配(意图)", completely_match)
    if completely_match:
        # 如果存在 "对比分布分析"，则先不急着返回，保留信息以备后续判断
        # has_comparison_distribution = '对比分布分析' in completely_match
        # if has_comparison_distribution:
        #     completely_match.remove('对比分布分析')
        if "对比分布分析" in completely_match and len(completely_match) == 1:
            completely_match = ["对比分布分析"]
        # elif '数值统计' in completely_match and len(completely_match) == 1:
        #      completely_match = ['数据查询']
        #      return {"result": completely_match, "need_multi_turn": False}
        else:
            return {"result": completely_match, "need_multi_turn": False}

    # 如果没有匹配到关键词，则使用 LLM 模型进行分析
    test_intent = prompt_intent_analysis
    test_intent = test_intent.replace("question", origin_input)
    llm_answer = send_llm(test_intent)

    pattern = r'\[.*?\]'
    matches = re.findall(pattern, str(llm_answer))
    if not matches:
        matches = ['对比分布分析']
    else:
        all_matches = []
        for match in matches:
            try:
                all_matches.extend(eval(match))
            except:
                pass
        matches = all_matches

    matched_intents = []
    for match in matches:
        match = match.strip('[]').strip('"')
        if match in keyword_intents.keys():
            matched_intents.append(match)
    if len(matched_intents) == 0:
        matched_intents = ['对比分布分析']

    print("*****", '模型匹配(意图)', matched_intents)
    matched_intents = list(set(matched_intents))
    # 进一步分析时间段
    if '环比分析' in matched_intents and time_list:
        analysis_result = huanbi_analyze_time_periods(time_list, origin_input)
        matched_intents = [analysis_result]
    elif '同比分析' in matched_intents and time_list:
        analysis_result = tongbi_analyze_time_periods(time_list)
        matched_intents = [analysis_result]

    if len(matched_intents) == 1:
        result = matched_intents
    elif len(matched_intents) == 2:
        if '占比分析' in matched_intents and '排名分析' in matched_intents:
            if group_list:
                result = ['排名分析']
            else:
                result = ['数值统计']
        elif '占比分析' in matched_intents and '趋势分析' in matched_intents:
            if group_list:
                result = ['占比分析']
            else:
                result = ['趋势分析']
        elif ('同比分析' in matched_intents and '环比分析' in matched_intents) or (
                '趋势分析' in matched_intents and '波动分析' in matched_intents):
            if len(time_list) == 2:
                # 是时间段，提取开始时间和结束时间
                start_time, end_time = time_list

                # 解析时间
                start_month = int(start_time.split('-')[1])
                end_month = int(end_time.split('-')[1])
                start_year = int(start_time.split('-')[0])
                end_year = int(end_time.split('-')[0])
                month_diff = (end_year - start_year) * 12 + (end_month - start_month)  # 时间差
                if month_diff > 2:
                    result = ['趋势分析']  # 波动
                else:
                    result = ['同环比分析']
            else:
                result = ['同环比分析']

        elif '数值统计' in matched_intents and '排名分析' in matched_intents:
            if group_list:
                result = ['排名分析']
            else:
                result = ['数值统计']

        elif '数值统计' in matched_intents and '对比分布分析' in matched_intents:
            result = ['数值统计']
        elif '同比分析' in matched_intents and '趋势分析' in matched_intents:
            if group_list:
                result = ['趋势分析']
            else:
                result = ['同比分析']
        else:
            result = [list(matched_intents)[0]]
    else:
        result = ['对比分布分析']

    print("*****", "最终(意图)", result)

    if completely_match:
        if not ('同比分析' in result or '环比分析' in result):
            result = ['对比分布分析']
    if '数值统计' in result:
        # result = ['数据查询']
        if any(period in group['columnName'] for group in group_list for period in ['年', '季', '月', '天']):
            result = ['趋势分析']
            print("执行")
    return {"result": result, "need_multi_turn": False}
