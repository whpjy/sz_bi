import re

from utils.json_util import find_max_list
from utils.request_util import send_llm
from utils.rule_word import local_stop_word
from utils.util import jaccard_similarity


def extract_compare(text):
    compare = []
    if "同比" in text:
        compare.append("同比")
    if "环比" in text:
        compare.append("环比")
    return compare


def extract_rank(text):
    # prompt = prompt_extract_rank.replace("{user_input}", text)
    # rank_result = send_llm(prompt)
    # print("llm_rank: ", rank_result)
    rank = ['+']
    chinese_numbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十", "十一", "十二", "十三", "十四"
                       , "十五", "十六", "十七", "十八", "十九", "二十", "二十一"]
    pre_word_list = ["前", "前面", "前面的", "最高", "最高的", "最多", "最多的", "较多的", "从高到低",
                     "后", "后面", "后面的", "哪", "最低", "最低的", "最少", "最少的", "较少的", "从低到高", "降序"]

    lit_word_list = ["后", "后面", "后面的", "最低", "最低的", "最少", "最少的", "较少的", "从低到高", "升序"]
    # 检查 lit_word_list 中的任何词是否在 text 中出现
    for word in lit_word_list:
        if word in text:
            rank = ["-"]  # 如果找到匹配，改变 rank 并立即退出循环
            break
    for pre_word in pre_word_list:
        for i in range(1, 21):
            keyword = pre_word + f"{i}"
            keyword1 = pre_word + f"{chinese_numbers[i]}"
            if keyword1 in text:
                if i not in rank:
                    rank.append(i)
            if keyword in text:
                if i not in rank:
                    rank.append(i)

    return rank


def exist_rank(text):
    rank = []
    chinese_numbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十", "十一", "十二", "十三", "十四"
                       , "十五", "十六", "十七", "十八", "十九", "二十", "二十一"]
    pre_word_list = ["前", "前面", "前面的", "最高", "最高的", "最多", "最多的", "较多的", "从高到低" "哪",
                     "后", "后面", "后面的", "哪", "最低", "最低的", "最少", "最少的", "较少的", "从低到高"]

    for pre_word in pre_word_list:
        for i in range(1, 21):
            keyword = pre_word + f"{i}"
            keyword1 = pre_word + f"{chinese_numbers[i]}"
            if keyword1 in text:
                if i not in rank:
                    rank.append(i)
            if keyword in text:
                if i not in rank:
                    rank.append(i)
    if len(rank) > 0:
        return True
    return False


def extract_value(text: str, keyword: str, direction: str) -> int:
    pattern = fr"{keyword}\s*(-?\d+)"
    match = re.search(pattern, text)
    if match:
        value_str = match.group(1)
        return int(value_str)
    else:
        return 0


def extract_limits(text):
    limits = []
    comparison_words = {
        "同比增长": ["同比上升", "同比增加", "同比增长", "大于", "高于", "超出", "超过", "上升", "提高", "扩大", "增进",
                     "增长"],
        "同比下降": ["同比下跌", "同比减少", "同比下降", "低于", "不及", "不足", "少于", "小于", "下降", "减少",
                     "下跌"],
    }

    for comparison_type, synonyms in comparison_words.items():
        for synonym in synonyms:
            if synonym in text:
                value = extract_value(text, synonym, "元")
                sign = 1 if comparison_type in ["同比增长"] else -1
                if value != 0:
                    limits.append({"sign": sign, "value": value})
                break

    return limits


def exist_special_character(phrases):
    stop_word_list = ['和', '的', '是', '为', '了', '于']
    # 根据词语的起始字符过滤
    for stop_word in stop_word_list:
        if phrases[0] == stop_word or phrases[-1] == stop_word:
            return True

    # 包含以下词语则过滤
    rule_prefix_word = local_stop_word
    for rule_word in rule_prefix_word:
        if rule_word in phrases:
            return True

    completely_match_list = ['多少', '增长']
    for march_word in completely_match_list:
        if phrases == march_word:
            return True

    return False


def rewrite_input_for_aggregation(real_input):

    time_word = ["月", "日", "年", "季度", "天", "周"]
    rule_prefix_word = ["按", "按照", "各", "各个", "每", "每个", "不同", "不同的"]

    for prefix_word in rule_prefix_word:
        for word in time_word:  # 如果模型没有识别到分类词，规则
            if prefix_word + word in real_input:
                real_input = real_input.replace(prefix_word + word, '')

    return real_input


def rule_judge(phrase):

    time_word = ["月", "日", "年", "季度", "天", "周"]

    for word in time_word:
        if word == phrase[-len(word):]:
            return False

    return True


def exist_group_prefix(user_object, phrases):
    real_input = user_object.history[0]["user"]
    rule_prefix_word = ["按", "按照", "各", "各个", "每", "每个", "不同", "不同的", "根据"]
    for prefix_word in rule_prefix_word:
        if prefix_word + phrases in real_input or prefix_word in phrases:
            return True

    return False


def exist_group_prefix_for_interface(question, phrases):
    real_input = question
    rule_prefix_word = ["按", "按照", "各", "各个", "每", "每个", "不同", "不同的", "根据"]
    for prefix_word in rule_prefix_word:
        if prefix_word + phrases in real_input or prefix_word in phrases:
            return True

    return False


def exist_sql_word(phrase, position_list, sql_word_pos):

    for sql_word, sql_pos_list in sql_word_pos.items():
        intersection_list = [value for value in position_list if value in sql_pos_list]
        if len(intersection_list) != 0:
            return True

    return False


def is_begin_metric(sql_word, metric_name_list):

    for metric_name in metric_name_list:
        if sql_word == metric_name[:len(sql_word)]:
            return True

    return False




def exist_sql_word_for_metric(metric_name_list, phrase, position_list, sql_word_pos):
    # 当sql抽取到where和group词，分词结果出现这类词的要剔除
    # 特殊例子
    # 查看十梓街的住院总费用 where[十梓街] -> 导致识别不到指标：十梓街住院总费用
    # 查看不同医师的住院人数 where[医师] -> 如果不考虑where会识别到指标：住院医师数量

    # 如果sql词是phrase起始，同时也是某个指标的起始，并且phrase词和某个指标的jaccard_similarity很高，可以放行phrase
    for sql_word, sql_pos_list in sql_word_pos.items():
        if sql_word == phrase[:len(sql_word)] and is_begin_metric(sql_word, metric_name_list):
            maxx_score = 0
            for metric_name in metric_name_list:
                score = jaccard_similarity(phrase, metric_name)
                if maxx_score < score:
                    maxx_score = score

            if maxx_score > 0.75:
                return False

    # 特殊例子
    # 上个月普外科专病门诊人次 where[上个月、普外科专病] -> 割开了 专病门诊人次这个指标
    # 如果phrase完全是一个指标，也要放行phrase
    if phrase in metric_name_list:
        return False

    for sql_word, sql_pos_list in sql_word_pos.items():
        intersection_list = [value for value in position_list if value in sql_pos_list]
        if len(intersection_list) != 0:
            return True  # 返回 True 表示舍弃该词

    return False


def contain_sql(phrase, sql_word):
    for word in sql_word:
        if word in phrase and len(phrase) > len(word):
            return True

    return False


def get_sql_word_pos(sql_list, sentence):
    sql_word_pos = {}
    for word in sql_list:
        if word in sentence:
            start = sentence.find(word)
            sql_word_pos[word] = [i for i in range(start, start + len(word))]

    return sql_word_pos

def get_time_word_by_llm(sentence):

    prompt = f'''你是一个信息提取大师，从用户输入原文中提取有关时间的提及信息。
    所有涉及时间的片段都应该被抽取，抽取的片段必须和原文一致，不能自己总结。
    将抽取到的所有片段放到一个列表中，最后仅输出可以被json解析的列表，不能输出任何其他内容。
    如果不存在任何时间类信息，输出空列表。
    
    用户输入：{sentence}'''

    result = send_llm(prompt)
    list_result = find_max_list(result)
    return list_result
