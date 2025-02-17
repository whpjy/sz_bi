from config.log_config import logger
from utils.key_word_rule import exist_group_prefix, get_sql_word_pos, exist_sql_word
from utils.time_util import is_numeric_string
from utils.util import jaccard_similarity, str_truncation
from utils.window_phrases import get_window_phrases


def get_jaccard_match(user_input, need_match_word):

    threshold = 0.8
    all_similar_words = []
    final_match_word = []
    sentence_position_use = []

    for phrases in need_match_word:
        window_lower_limit = max(1, len(phrases) - 4)
        window_upper_limit = len(phrases) + 4

        for window_size in range(window_lower_limit, window_upper_limit + 1):
            for i in range(0, len(user_input) - window_size + 1):
                window_phrases = str_truncation(i, window_size, user_input)
                current_similarity_score = jaccard_similarity(phrases, window_phrases)

                if current_similarity_score >= threshold:
                    position_list = [j for j in range(i, i + window_size) if j < len(user_input)]
                    new_dict = {"phrases": window_phrases, "position_list": position_list, "recall_word": phrases, "max_score": current_similarity_score}
                    all_similar_words.append(new_dict)

    phrase_recall_info = sorted(all_similar_words, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_position_use]
        if len(intersection_list) == 0:
            sentence_position_use = sentence_position_use + info["position_list"]
            if info["recall_word"] not in final_match_word:
                final_match_word.append(info["recall_word"])

    return final_match_word


def get_jaccard_match_table(user_object, user_input, need_match_word):

    del_word_list = user_object.metric_recognize_by_phrase
    for delword in del_word_list:
        user_input = user_input.replace(delword, '')
    threshold = 0.5
    all_similar_words = []
    final_match_word = []
    sentence_position_use = []

    for phrases in need_match_word:
        window_lower_limit = max(1, len(phrases) - 4)
        window_upper_limit = len(phrases) + 4

        for window_size in range(window_lower_limit, window_upper_limit + 1):
            for i in range(0, len(user_input) - window_size + 1):
                window_phrases = str_truncation(i, window_size, user_input)
                current_similarity_score = jaccard_similarity(phrases, window_phrases)

                if current_similarity_score >= threshold:
                    position_list = [j for j in range(i, i + window_size) if j < len(user_input)]
                    new_dict = {"phrases": window_phrases, "position_list": position_list, "recall_word": phrases, "max_score": current_similarity_score}
                    all_similar_words.append(new_dict)

    phrase_recall_info = sorted(all_similar_words, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)
    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_position_use]
        if len(intersection_list) == 0:
            sentence_position_use = sentence_position_use + info["position_list"]
            if info["recall_word"] not in final_match_word:
                final_match_word.append(info["recall_word"])

    return final_match_word


def get_group_jaccard_match_muti(user_input, need_match_word, group_name2id_dict, self_group_name2id_dict):

    logger.info(f"开始group字形匹配")
    logger.info(f"当前指标的group词：{need_match_word}")

    threshold = 0.8
    all_similar_words = []
    final_match_word = []
    sentence_position_use = []

    for phrases in need_match_word:
        window_lower_limit = max(1, len(phrases) - 4)
        window_upper_limit = len(phrases) + 4

        for window_size in range(window_lower_limit, window_upper_limit + 1):
            for i in range(0, len(user_input) - window_size + 1):
                window_phrases = str_truncation(i, window_size, user_input)
                current_similarity_score = jaccard_similarity(phrases, window_phrases)

                if current_similarity_score >= threshold:
                    position_list = [j for j in range(i, i + window_size) if j < len(user_input)]
                    new_dict = {"phrases": window_phrases, "position_list": position_list, "recall_word": phrases, "max_score": current_similarity_score}
                    all_similar_words.append(new_dict)

    phrase_recall_info = sorted(all_similar_words, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_position_use]
        if len(intersection_list) == 0:
            sentence_position_use = sentence_position_use + info["position_list"]
            if info["recall_word"] not in final_match_word:
                if info["recall_word"] in self_group_name2id_dict.keys():  # 优先给自身维度
                    final_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"], "columnId": self_group_name2id_dict[info["recall_word"]], "max_score": 1.0})
                else:
                    final_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"], "columnId": group_name2id_dict[info["recall_word"]], "max_score": 1.0})

    logger.info(f"group字形计算结果：{final_match_word}")
    return final_match_word


def get_group_jaccard_match_first(user_object, user_input, need_match_word, group_name2id_dict, self_group_name2id_dict):

    sql_where = []
    if "WHERE" in user_object.test2sql_information.keys():
        sql_where = user_object.test2sql_information["WHERE"]

    sql_word_pos = get_sql_word_pos(sql_where, user_input)

    logger.info(f"开始group字形匹配")
    logger.info(f"当前指标的group词：{need_match_word}")

    threshold = 0.8
    all_similar_words = []
    final_match_word = []
    sentence_position_use = []

    for phrases in need_match_word:
        window_lower_limit = max(1, len(phrases) - 4)
        window_upper_limit = len(phrases) + 4

        for window_size in range(window_lower_limit, window_upper_limit + 1):
            for i in range(0, len(user_input) - window_size + 1):
                window_phrases = str_truncation(i, window_size, user_input)
                current_similarity_score = jaccard_similarity(phrases, window_phrases)

                if current_similarity_score >= threshold:
                    position_list = [j for j in range(i, i + window_size) if j < len(user_input)]
                    if exist_sql_word(window_phrases, position_list, sql_word_pos):
                        continue
                    new_dict = {"phrases": window_phrases, "position_list": position_list, "recall_word": phrases, "max_score": current_similarity_score}
                    all_similar_words.append(new_dict)

    phrase_recall_info = sorted(all_similar_words, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_position_use]
        if len(intersection_list) == 0:
            sentence_position_use = sentence_position_use + info["position_list"]
            if info["recall_word"] not in final_match_word:
                if info["recall_word"] in self_group_name2id_dict.keys():  # 优先给自身维度
                    final_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"], "columnId": self_group_name2id_dict[info["recall_word"]], "max_score": 1.0})
                else:
                    final_match_word.append({"phrases": info["phrases"], "recall_word": info["recall_word"], "columnId": group_name2id_dict[info["recall_word"]], "max_score": 1.0})

    logger.info(f"group字形计算结果：{final_match_word}")
    return final_match_word


def get_jaccard_match_list2list(list1, list2):
    threshold = 0.8
    all_similar_words = []

    for word1 in list1:
        for word2 in list2:
            current_similarity_score = jaccard_similarity(word1, word2)
            if current_similarity_score >= threshold and word2 not in all_similar_words:
                all_similar_words.append(word2)

    return all_similar_words



def get_word_jaccard_match_max(word, list2): # 只返回一个结果
    threshold = 0.8
    all_similar_words = []
    max_similarity_score = 0

    for word2 in list2:
        current_similarity_score = jaccard_similarity(word, word2)
        if current_similarity_score >= threshold and current_similarity_score > max_similarity_score:
            max_similarity_score = current_similarity_score
            all_similar_words.append(word2)

    return all_similar_words



def get_metric_jaccard_match(user_object, value_list):

    threshold = 0.81
    jaccard_match = []
    sentence_jieba_position_use = []
    match_word_pos = []
    all_match_word = []
    user_object.jieba_window_phrases2positionList = get_window_phrases(user_object)  # 剔除字形匹配的指标

    for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
        if exist_group_prefix(user_object, phrases):
            continue

        for match_word in value_list:
            current_similarity_score = jaccard_similarity(phrases, match_word)
            if current_similarity_score >= threshold:
                new_dict = {"phrases": phrases, "position_list": position_list, "max_score": current_similarity_score, "match_word": match_word}
                jaccard_match.append(new_dict)

    new_jaccard_match = []  # 去除前缀，例如达到阈值的住院人数、住院人数人次比 要过滤住院人数
    for info1 in jaccard_match:
        reserve_flag = True
        for info2 in jaccard_match:
            if info1["match_word"] != info2["match_word"] and info1["match_word"] in info2["match_word"]:
                reserve_flag = False
        if reserve_flag:
            new_jaccard_match.append(info1)

    jaccard_match = new_jaccard_match

    jaccard_match = sorted(jaccard_match, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in jaccard_match:
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            if info["match_word"] != "手术":
                match_word_pos.append({"match_word": info["match_word"], "start_pos":info["position_list"][0]})
                user_object.metric_recognize_by_phrase.append(info["phrases"])

    match_word_pos = sorted(match_word_pos, key=lambda x: (x["start_pos"]), reverse=False)
    for info in match_word_pos:
        all_match_word.append(info["match_word"])

    return all_match_word


def get_timeType_jaccard_match(user_object, value_list):

    threshold = 0.8
    jaccard_match = []
    sentence_jieba_position_use = []
    all_match_word = []
    user_object.jieba_window_phrases2positionList = get_window_phrases(user_object)  # 剔除字形匹配的指标
    print(user_object.jieba_window_phrases2positionList)
    for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
        for match_word in value_list:
            current_similarity_score = jaccard_similarity(phrases, match_word)
            if "住院病人数" == phrases:
                print("住院病人数",current_similarity_score)
            if current_similarity_score >= threshold:
                new_dict = {"phrases": phrases, "position_list": position_list, "max_score": current_similarity_score, "match_word": match_word}
                jaccard_match.append(new_dict)

    jaccard_match = sorted(jaccard_match, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in jaccard_match:
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            all_match_word.append(info["match_word"])
            user_object.metric_recognize_by_phrase.append(info["phrases"])

    return all_match_word


def get_where_jaccard_match(user_object, value_list):

    threshold = 0.8
    jaccard_match = []
    sentence_jieba_position_use = []
    all_match_word = []
    origin_input = user_object.history[0]["user"]

    table_name_list = user_object.slot_dict["table"]
    aggregation_name_list = user_object.slot_dict["aggregation"]
    timeType_name_list = user_object.slot_dict["timeType"]
    group_name_list = user_object.group_recognize_by_phrase

    del_word_list = user_object.metric_recognize_by_phrase + table_name_list + aggregation_name_list + timeType_name_list + group_name_list
    for del_word in del_word_list:
        if del_word in origin_input:
            origin_input = origin_input.replace(del_word, ",")

    for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
        if "恶性肿瘤" in phrases:
            continue
        for match_word in value_list:
            # 特殊情况，如三级手术人数，手术人数是指标，三级手术是where，此时phrases为三级来匹配match_word为三级手术
            if phrases in match_word and match_word in origin_input:
                new_dict = {"phrases": phrases, "position_list": position_list, "max_score": 1,
                            "match_word": match_word}
                jaccard_match.append(new_dict)
            else:
                if is_numeric_string(phrases):
                    if phrases == match_word:
                        new_dict = {"phrases": phrases, "position_list": position_list, "max_score": 1, "match_word": match_word}
                        jaccard_match.append(new_dict)
                else:
                    current_similarity_score = jaccard_similarity(phrases, match_word)
                    if current_similarity_score >= threshold:
                        new_dict = {"phrases": phrases, "position_list": position_list, "max_score": current_similarity_score, "match_word": match_word}
                        jaccard_match.append(new_dict)

    jaccard_match = sorted(jaccard_match, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in jaccard_match:
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            all_match_word.append({"match_word": info["match_word"], "phrases": info["phrases"], "position_list": info["position_list"]})

    filter_jieba_window_phrases2positionList = {}
    for phrases, position_list in user_object.jieba_window_phrases2positionList.items():
        intersection_list = [value for value in position_list if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            filter_jieba_window_phrases2positionList[phrases] = position_list

    user_object.jieba_window_phrases2positionList = filter_jieba_window_phrases2positionList

    return all_match_word
