from datetime import datetime, timedelta
import re

from utils.request_util import send_llm


def get_current_date():
    current_date = datetime.now()
    return current_date.strftime("%Y-%m-%d")


def get_current_date_year():
    current_date = datetime.now()
    return current_date.strftime("%Y")


def contains_find(user_input, word):
    positions = []
    start = 0
    while start < len(user_input):
        position = user_input.find(word, start)
        if position == -1:
            break
        positions.append(position)
        start = position + 1
    return positions


def contains_time(sentence, extract_json_result):
    time_word = ["月", "月度", "日", "年", "年度", "季", "季度", "天", "周"]
    time_flag = False
    for word in time_word:
        positions = contains_find(sentence, word)
        if positions != -1:
            for position in positions:
                if re.match(r'^[0-9一二三四五六七八九十上个本今昨前当下后明去过去两]', sentence[position - 1]):
                    time_flag = True
                    return time_flag

    if not time_flag:
        try:
            if extract_json_result["time"][0][0].split('-')[0] in sentence:
                return True
        except:
            pass

    return time_flag


def judging_time_reasonable(time_result):  # 判断时间是否合理，如13月
    #  time_result 格式为 [[ "2024-01", "2024-03"]] 或者 [["2024-03"]]
    try:
        for time_str in time_result[0]:
            split_list = time_str.split('-')
            if len(split_list) == 3:
                if 1 <= float(split_list[-1]) <= 31 and 1 <= float(split_list[-2]) <= 12:
                    continue
            elif len(split_list) == 2:
                if 1 <= float(split_list[-1]) <= 12:
                    continue
            else:
                return False
        return True
    except:
        return False


def get_clean_time(json_time):
    # 例如 [['2023-01-01', '2023-01-31'], ['2023-02-01', '2023-02-28'], ['2023-03-01', '2023-03-31'],
    # ['2023-04-01', '2023-04-30'], ['2023-05-01', '2023-05-31'], ['2023-06-01', '2023-06-30'],
    # ['2023-07-01', '2023-07-31'], ['2023-08-01', '2023-08-31'], ['2023-09-01', '2023-09-30'],
    # ['2023-10-01', '2023-10-31'], ['2023-11-01', '2023-11-30'], ['2023-12-01', '2023-12-31']] 转换成月格式
    new_json_time = []
    for single_time in json_time:  # 先处理一下，例如把内部元素为 ['2023-05-01', '2023-05-31'] 的转换成 ['2023-05']
        single_time = day_convert_month(single_time)
        new_json_time.append(single_time)
    time_result = new_json_time

    # 有时time形式是 [[ "2024-01"],["2024-03"]] 要转换成  [[ "2024-01", "2024-03"]]
    # time_result = json_time
    # if len(json_time) == 2:
    #     if isinstance(json_time[0], list) and isinstance(json_time[1], list):
    #         if len(json_time[0]) == 1 and len(json_time[1]) == 1:
    #             time_result = [json_time[0] + json_time[1]]

    # 有时time形式是 ["2024-01","2024-03"] 要转换成  [[ "2024-01", "2024-03"]]
    if len(time_result) == 2:
        if isinstance(time_result[0], str) and isinstance(time_result[1], str):
            time_result = [json_time]

    # 有时time形式是 [["2023-12-01"],["2023-12-31"]] 要转换成  [["2024-12"]]
    if len(time_result) == 2:
        if isinstance(time_result[0], list) and len(time_result[0]) == 1 and isinstance(time_result[1], list) and len(
                time_result[1]) == 1:
            merge_time = time_result[0] + time_result[1]
            new_time = day_convert_month(merge_time)
            if new_time != merge_time:
                time_result = [new_time]

    # 有时time形式是 [["2023"]] 要转换成  [["2023-01", "2023-12"]]
    if len(time_result) == 1:
        if isinstance(time_result[0], list) and len(time_result[0]) == 1:
            if '-' not in time_result[0][0] and len(time_result[0][0]) == 4:
                time_result = [[time_result[0][0] + '-01', time_result[0][0] + '-12']]

    # 有时time形式是 [['2023-01'], ['2023-02'], ['2023-03'], ['2023-04'], ['2023-05'], ['2023-06']]
    # 有时time形式是 [['2023-01', '2023-03'], ['2023-04', '2023-06'], ['2023-07', '2023-09'], ['2023-10', '2023-12']]
    if len(time_result) > 2:
        if is_consecutive_months(time_result):  # 判断是否是连续的月份
            time_result = [[time_result[0][0], time_result[-1][-1]]]

    #  [['2023-01-01', '2023-12-31']] 要转换成  [[ "2024-01", "2024-12"]]
    #  [['2023-01-01', '2023-01-31']] 要转换成  [[ "2024-01"]]
    if len(time_result) == 1:
        time_result[0] = day_convert_month(time_result[0])

    # 有时time形式是 [['2023-12', '2023-12']] 要转换成  [[ "2024-12"]]
    if len(time_result) == 1:
        if isinstance(time_result[0], list) and len(time_result[0]) == 2:
            if time_result[0][0] == time_result[0][1]:
                time_result = [[time_result[0][0]]]

    if judging_time_reasonable(time_result):
        return time_result
    else:
        return []



def extract_group_time(user_input):
    extract_group_time = []

    time_word = ["月", "日", "年", "季度", "天", "周"]
    rule_prefix_word = ["按", "按照", "各", "各个", "每", "每个", "不同", "不同的"]
    special_word = ['年龄']
    for prefix_word in rule_prefix_word:
        for word in time_word:  # 如果模型没有识别到分类词，规则
            if prefix_word + word in user_input and word not in extract_group_time:
                flag = True
                for special in special_word:
                    if prefix_word + special in user_input:
                        flag = False
                if flag:
                    extract_group_time.append(word)

    return extract_group_time


def get_week_start_end():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date - timedelta(days=date.weekday())
    start_str = start_of_week.strftime("%Y-%m-%d")
    end_of_week = start_of_week + timedelta(days=1)
    end_str = end_of_week.strftime("%Y-%m-%d")
    if start_str != date_str:
        return '{"time": [[' + f'"{start_str}", "{date_str}"' + ']]}'
    else:
        # 当是星期一，本周开始结束是同一天，这里将结束再加一天
        return '{"time": [[' + f'"{start_str}", "{end_str}"' + ']]}'


def get_last_week_start_end():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date - timedelta(days=date.weekday()) - timedelta(days=7)
    end_of_week = start_of_week + timedelta(days=6)
    return '{"time": [[' + f'"{start_of_week.strftime("%Y-%m-%d")}", "{end_of_week.strftime("%Y-%m-%d")}"' + ']]}'


def get_last_two_week_start_end():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date - timedelta(days=date.weekday()) - timedelta(days=14)
    end_of_week = start_of_week + timedelta(days=14)
    return '{"time": [[' + f'"{start_of_week.strftime("%Y-%m-%d")}", "{end_of_week.strftime("%Y-%m-%d")}"' + ']]}'


def get_week_info():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date - timedelta(days=date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    today = datetime.now()
    day_of_week = today.weekday()
    days_of_week = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
    today_day_name = days_of_week[day_of_week]

    return f'今天的日期是{date_str}，今天是{today_day_name}，本周的周一是{start_of_week.strftime("%Y-%m-%d")}日，本周的周日是{end_of_week.strftime("%Y-%m-%d")}日，完整的一周范围定义为周一到周日'


def get_past_7day_start_end():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date - timedelta(days=7)

    return '{"time": [[' + f'"{start_of_week.strftime("%Y-%m-%d")}", "{date_str}"' + ']]}'


def get_current_month_start_end():
    # 获取当前日期时间
    current_date = datetime.now()

    # 获取本月的第一天
    first_day_of_current_month = current_date.replace(day=1)

    # 格式化为所需的字符串格式
    start_date_str = first_day_of_current_month.strftime("%Y-%m-%d")
    end_date_str = current_date.strftime("%Y-%m-%d")
    if start_date_str != end_date_str:
        return '{"time": [[' + f'"{start_date_str}", "{end_date_str}"' + ']]}'
    else:
        end_date = first_day_of_current_month + timedelta(days=1)
        end_date_str = end_date.strftime("%Y-%m-%d")
        return '{"time": [[' + f'"{start_date_str}", "{end_date_str}"' + ']]}'


def get_last_month_start_end():
    # 获取当前日期时间
    current_date = datetime.now()

    # 获取本月的第一天
    first_day_of_current_month = current_date.replace(day=1)

    # 获取上个月的最后一天
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

    # 获取上个月的第一天
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    # 格式化为所需的字符串格式
    start_date_str = first_day_of_previous_month.strftime("%Y-%m-%d")
    end_date_str = last_day_of_previous_month.strftime("%Y-%m-%d")

    return '{"time": [[' + f'"{start_date_str}", "{end_date_str}"' + ']]}'


def get_past_30day_start_end():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y-%m-%d")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = date - timedelta(days=30)

    return '{"time": [[' + f'"{start_of_week.strftime("%Y-%m-%d")}", "{date_str}"' + ']]}'



def get_year_start_to_today():
    # 获取当前日期时间
    current_date = datetime.now()

    # 获取今年的第一天
    start_of_year = current_date.replace(month=1, day=1)

    # 格式化为所需的字符串格式
    start_date_str = start_of_year.strftime("%Y-%m-%d")
    end_date_str = current_date.strftime("%Y-%m-%d")
    return '{"time": [[' + f'"{start_date_str}", "{end_date_str}"' + ']]}'


def get_current_year():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y")
    return str(int(date_str))


def get_last_year():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y")
    return str(int(date_str) - 1)


def get_year_before_last():
    current_date = datetime.now()
    date_str = current_date.strftime("%Y")
    return str(int(date_str) - 2)


def is_numeric_string(s):
    return s.isdigit()


def huanbi_time_clean(time_list):
    # 环比时间处理，应该只返回一个数据
    # 1、2023年8月12日住院人数跟前一天相比有什么变化，按出院时间
    # [['2024-08-11', '2024-08-12']] -> [['2024-08-12']]
    # 时间也可能是这样的 [['2023-08-11'], ['2023-08-12']]
    # 2、本周住院人数跟上一周相比有什么变化，按出院时间
    # [['2024-08-12', '2024-08-18'], ['2024-08-05', '2024-08-11']] -> ['2024-08-12', '2024-08-18']
    try:
        if len(time_list) == 1:  # [['2024-08-11', '2024-08-12']]
            if len(time_list[0]) == 2:
                date1 = time_list[0][0]
                date2 = time_list[0][1]
                if len(date1.split('-')) == 3 and len(date2.split('-')) == 3:
                    date_format = "%Y-%m-%d"
                    date1 = datetime.strptime(date1, date_format)
                    date2 = datetime.strptime(date2, date_format)
                    delta = abs((date2 - date1).days)
                    if delta == 1:
                        return [[max(time_list[0])]]

        elif len(time_list) == 2:
            # [['2023-08-11'], ['2023-08-12']]
            if len(time_list[0]) == 1 and len(time_list[1]) == 1:
                date1 = time_list[0][0]
                date2 = time_list[1][0]
                if date1 > date2:
                    return [time_list[0]]
                else:
                    return [time_list[1]]

            # [['2024-08-12', '2024-08-18'], ['2024-08-05', '2024-08-11']] -> ['2024-08-12', '2024-08-18']
            if len(time_list[0]) == 2 and len(time_list[1]) == 2:
                date1 = time_list[0][1]
                date2 = time_list[1][1]
                if date1 > date2:
                    return [time_list[0]]
                else:
                    return [time_list[1]]
    except:
        pass

    return time_list


def tongbi_time_clean(time_list):
    # 同比时间处理，应该只返回一个数据
    # 1、2023年8月12日相比前一天住院人数
    # [['2024-08-11'], ['2024-08-12']] -> [['2024-08-12']]
    # 2、本周住院人数跟上一周相比有什么变化，按出院时间
    # [['2024-08-12', '2024-08-18'], ['2024-08-05', '2024-08-11']] -> ['2024-08-12', '2024-08-18']
    try:
        if len(time_list) == 2:
            # [['2023-08-11'], ['2023-08-12']]
            if len(time_list[0]) == 1 and len(time_list[1]) == 1:
                date1 = time_list[0][0]
                date2 = time_list[1][0]
                if date1 > date2:
                    return [time_list[0]]
                else:
                    return [time_list[1]]

            # [['2024-08-12', '2024-08-18'], ['2024-08-05', '2024-08-11']] -> ['2024-08-12', '2024-08-18']
            if len(time_list[0]) == 2 and len(time_list[1]) == 2:
                date1 = time_list[0][1]
                date2 = time_list[1][1]
                if date1 > date2:
                    return [time_list[0]]
                else:
                    return [time_list[1]]
    except:
        pass

    return time_list


def day_convert_month(inner_time_result):
    # 例如将 ['2023-01-01', '2023-12-31'] 转换为 ['2023-01', '2023-12']
    #  ['2023-01-01', '2023-01-31'] 要转换成  ["2024-01"]
    try:
        if isinstance(inner_time_result, list) and len(inner_time_result) == 2:
            date_start = inner_time_result[0]
            date_end = inner_time_result[1]
            if len(date_start.split('-')) == 3 and len(date_end.split('-')) == 3:
                date_start = datetime.strptime(date_start, "%Y-%m-%d")
                day_date_start = date_start.strftime("%d")

                date_end = datetime.strptime(date_end, "%Y-%m-%d")
                next_day = date_end + timedelta(days=1)
                next_day_str = next_day.strftime("%d")

                if next_day_str == '01' and day_date_start == '01':
                    final_date_start = date_start.strftime("%Y-%m")
                    final_date_end = date_end.strftime("%Y-%m")
                    if final_date_start == final_date_end:
                        inner_time_result = [final_date_start]
                    else:
                        inner_time_result = [final_date_start, final_date_end]
                    return inner_time_result
    except:
        pass

    return inner_time_result


def is_consecutive_months(time_result):
    # 判断是否是连续的单个月
    # [['2023-01'], ['2023-02'], ['2023-03'], ['2023-04'], ['2023-05'], ['2023-06']]

    try:
        pre_value = 0
        if len(time_result) > 0:
            for single_time in time_result:
                if len(single_time) == 1 and isinstance(single_time, list):
                    current_value = int(single_time[0].split('-')[-1])
                    if pre_value == 0:
                        pre_value = current_value
                    else:
                        if current_value - pre_value == 1:
                            pre_value += 1
                        else:
                            return False

        return True
    except:
        pass

    return False


# 判断group是否已经存在时间分组词
def judge_time_group_exist(user_object):
    group_list = user_object.slot_dict["group"]
    time_group_exist = False
    for group in group_list:
        if group["columnId"] == -1:
            time_group_exist = True

    return time_group_exist

# 获取指定月份有多少天
def get_days_by_month(yy_mm):
    try:
        format_str = "%Y-%m"
        # 解析给定的字符串得到日期对象
        start_of_month = datetime.strptime(yy_mm, format_str)
        # 获取下个月的第一天
        next_month = start_of_month.replace(day=28) + timedelta(days=4)  # this will never fail
        # 减去天数直到到达下个月的第一天
        next_month = next_month - timedelta(days=next_month.day)
        # 返回上个月的最后一天
        return next_month.day
    except:
        return 30


# 计算时间跨度, 返回可推荐的最大时间范围
def cal_time_span(time_result):
    str_time1 = time_result[0][0]
    str_time2 = time_result[0][1]

    if str_time1 > str_time2: # 如果小的时间在前需要换一下
        swap = str_time1
        str_time1 = str_time2
        str_time2 = swap

    split1 = str_time1.split('-')
    split2 = str_time2.split('-')

    if len(split1) == 2 and len(split2) == 2:  # 如果是[['yy-mm', 'yy-mm']]则恢复成 [['yy-mm-dd', 'yy-mm-dd']]
        str_time1 += '-01'
        str_time2 = str_time2 + '-' + str(get_days_by_month(str_time2))
        split1 = str_time1.split('-')
        split2 = str_time2.split('-')

    if len(split1) == 3 and len(split2) == 3:
        time1 = datetime.strptime(str_time1, "%Y-%m-%d")
        time2 = datetime.strptime(str_time2, "%Y-%m-%d")
        days_delta = abs((time2 - time1).days) + 1
        # 例如 [['2023-12-01', '2023-12-31']] 判断是否是整个月
        if split1[0] == split2[0] and split1[1] == split2[1] and split1[2] == '01':  # 是否是同一个月的起始
            next_day = time2 + timedelta(days=1)
            next_day_str = next_day.strftime("%d")
            if next_day_str == '01':  # 说明是完整的一个月份，则最大推荐周
                return '周'

        if split1[0] == split2[0] and split1[1] == "01" and split1[2] == "01" and split2[1] == "12":  # 是否是同一个年的起始
            next_day = time2 + timedelta(days=1)
            next_day_str = next_day.strftime("%d")
            if next_day_str == '01':  # 说明是完整的一个年份，则最大推荐季度
                return '季度'

        else:  # 其他直接根据天差判断
            if days_delta >= 365:
                return '年'
            elif days_delta >= 120:
                return '季度'
            elif days_delta >= 30:
                return '月'
            elif days_delta >= 7:
                return '周'
            else:
                return '天'

    return ''


def remove_non_date_numbers(text):
    # 日期特征关键词
    date_keywords = {"年", "季", "月", "日", "号", "周", "天", "星期", "礼拜"}
    result = []
    i = 0

    while i < len(text):
        if text[i].isdigit():
            # 记录数字的开始位置
            start = i

            # 找到完整的数字
            while i < len(text) and text[i].isdigit():
                i += 1
            end = i

            # 获取数字前后可能的日期关键词
            prev_word = text[start - 1] if start > 0 else ""
            next_word = text[end] if end < len(text) else ""

            # 判断数字是否有效（前或后有日期关键词）
            if prev_word in date_keywords or next_word in date_keywords:
                result.append(text[start:end])  # 保留有效数字
        else:
            # 非数字部分直接添加
            result.append(text[i])
            i += 1

    return ''.join(result)


def get_current_time():
    # 获取当前时间

    now = datetime.now()

    time_string = now.strftime("%Y-%m-%d %H:%M:%S")

    return time_string


def extract_dates_time_control(text):
    # 抽取多轮中从时间控件输入的日期
    # 例如 2024年12月4日到2025年1月15日
    pattern = r'(\d{4}年\d{1,2}月\d{1,2}日)到(\d{4}年\d{1,2}月\d{1,2}日)'

    # 使用findall方法找到所有匹配项
    matches = re.findall(pattern, text)

    # 将匹配项转换为所需的格式
    formatted_dates = '['
    count = 0
    for match in matches:
        if count > 0:
            formatted_dates += ', '
        count += 1
        start_date, end_date = match
        # 替换中文字符为连字符，并确保月份和日期是两位数
        start_date_formatted = "-".join([part.zfill(2) for part in re.split(r'[年月日]', start_date) if part])
        end_date_formatted = "-".join([part.zfill(2) for part in re.split(r'[年月日]', end_date) if part])
        formatted_dates = formatted_dates + '["' + start_date_formatted + '", "' + end_date_formatted + '"]'

    return formatted_dates + ']'



def judge_input_time(user_input):

    prompt = f'''判断用户的输入是否是时间的概念，只需要回答“是”或“否”，不用输出解释性内容和其他内容
    用户输入：{user_input}'''

    result = send_llm(prompt)
    if "是" in result:
        return True
    else:
        return False