import time

from config.log_config import logger
from utils.json_util import find_max_list
from utils.key_word_rule import exist_group_prefix
from utils.request_util import batch_send_embedding_message, get_bge_reranker, send_embedding_message, send_llm
from utils.util import metric_recommend, sort_by_metric_type, jaccard_similarity
from utils.window_phrases import get_window_phrases_for_metric, get_window_phrases_for_metric_recommend


def get_metric_vector_match(user_object, collection_name):
    sentence = user_object.history[0]["user"].strip()
    phrases2positionList = get_window_phrases_for_metric(user_object)  # 剔除字形匹配的指标
    print("*****", "分词(指标)：", list(phrases2positionList.keys()))
    logger.info(f"指标的分词，不考虑sql抽取结果：{list(phrases2positionList.keys())}")

    threshold = 0.01
    phrase_recall_info = []
    all_match_word = []
    no_threshold_values_reranker = []
    final_match2phrase = {}  # 这是推荐指标记录对应的片段，用户选择指标后，该对应片段被记录并在后续任务中不考虑该片段

    # 获取所有窗口子串
    result_words = list(phrases2positionList.keys())

    if len(result_words) == 0:
        return [], [], []
    embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words), scores_threshold=0)

    num = 0
    for item in embedding_results:
        phrases = result_words[num]
        num += 1
        if exist_group_prefix(user_object, phrases):
            continue
        position_list = phrases2positionList[phrases]
        recall_list = []
        for info in item:
            if info['score'] >= threshold:
                recall_list.append(info['payload']['value'])
                # logger.info(f"指标召回： phrases: {phrases} score: {info['score']} value: {info['payload']['value']}")

        if len(recall_list) > 0:
            if recall_list[0] != "手术":
                max_score = item[0]['score']
                new_dict = {"phrases": phrases, "position_list": position_list, "recall_list": recall_list, "max_score": max_score}
                phrase_recall_info.append(new_dict)

    # 先优先分数排序
    phrase_recall_info = sorted(phrase_recall_info, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    match2phrase = {}
    # 把召回的指标重排，记录指标的分数，和对应的召回原始片段位置区间
    match2phrase_qdrant = {}
    # 初始召回的指标对应的最佳片段，因为有时候重排选择的也未必是最佳片段，用来辅助模型选择多个指标时候来判断是否出自同一片段
    for info in phrase_recall_info:
        phrases = info["phrases"]
        recall_list = info["recall_list"]
        max_score = info["max_score"]
        rerank_result = get_bge_reranker(phrases, recall_list)
        if len(recall_list) > 0:
            if recall_list[0] in match2phrase_qdrant.keys():
                if max_score > match2phrase_qdrant[recall_list[0]]["score"]:
                    match2phrase_qdrant[recall_list[0]] = {"score": max_score, "position_list": info["position_list"], "phrases": info["phrases"]}
            else:
                match2phrase_qdrant[recall_list[0]] = {"score": max_score, "position_list": info["position_list"], "phrases": info["phrases"]}
        for metric, score in rerank_result.items():
            if metric not in match2phrase.keys():
                match2phrase[metric] = {"score": score, "position_list": info["position_list"], "phrases": info["phrases"]}
            else:
                if score > match2phrase[metric]["score"]:
                    match2phrase[metric]["score"] = score
                    match2phrase[metric]["position_list"] = info["position_list"]
                    match2phrase[metric]["phrases"] = info["phrases"]

    match2phrase = dict(sorted(match2phrase.items(), key=lambda x: (x[1]["score"], len(x[1]["position_list"])), reverse=True))
    filter_list = [metric for metric, metric_info in match2phrase.items() if metric_info["score"] > -1]
    logger.info(f"指标召回： {match2phrase}")
    logger.info(f"指标召回(前15)： {filter_list[:15]}")

    if len(filter_list) > 0:
        llm_metric = llm_choose_metric(user_object, sentence, filter_list[:15])
    else:
        llm_metric = []

    logger.info(f"模型选择指标： {llm_metric}")

    if len(llm_metric) > 0:  # 模型判断有结果，此时有两种情况，一是可以直接确定，二是需要进入多轮

        # 如果模型选择只有一个指标，直接确定
        if len(llm_metric) == 1:
            all_match_word = []
            logger.info(f"指标识别结果：{llm_metric}")
            if llm_metric[0] not in user_object.slot_dict["metric"]:
                user_object.slot_dict["metric"].append(llm_metric[0])
                user_object.metric_recognize_by_phrase.append(match2phrase[llm_metric[0]]["phrases"])
        else:
            # 如果模型选了多个指标，需要判断是否需要多轮
            # 判断需要多轮的条件是模型选择的多个指标的来源原始片段有重叠
            sentence_jieba_position_use = []
            need_mul_turn = False
            for metric in llm_metric:
                intersection_list = [value for value in match2phrase[metric]["position_list"] if value in sentence_jieba_position_use]
                if len(intersection_list) == 0:
                    for pos in match2phrase[metric]["position_list"]:
                        if pos not in sentence_jieba_position_use:
                            sentence_jieba_position_use.append(pos)
                else:
                    need_mul_turn = True

            if not need_mul_turn:
                # 模型多选指标同一片段的判断，重排的如果认为不需要多轮，使用qdrant的初始召回再判断一次，因为有时候重排指标对应的最佳片段未必是最好的
                sentence_jieba_position_use = []
                for metric in llm_metric:
                    if metric in match2phrase_qdrant.keys():
                        intersection_list = [value for value in match2phrase_qdrant[metric]["position_list"] if value in sentence_jieba_position_use]
                        if len(intersection_list) == 0:
                            for pos in match2phrase_qdrant[metric]["position_list"]:
                                if pos not in sentence_jieba_position_use:
                                    sentence_jieba_position_use.append(pos)
                        else:
                            need_mul_turn = True
                            # 此时更换match2phrase的phrase和position_list
                            for metric in llm_metric:
                                if metric in match2phrase_qdrant.keys():
                                    match2phrase[metric]["phrases"] = match2phrase_qdrant[metric]["phrases"]
                                    match2phrase[metric]["position_list"] = match2phrase_qdrant[metric]["position_list"]
                            break

            if not need_mul_turn:
                all_match_word = []
                logger.info(f"指标识别结果(不需多轮){llm_metric}")
                for metric in llm_metric:
                    if metric not in user_object.slot_dict["metric"]:
                        user_object.slot_dict["metric"].append(metric)
                        user_object.metric_recognize_by_phrase.append(match2phrase[metric]["phrases"])

            else:
                all_match_word = llm_metric
                logger.info(f"指标识别结果(需要多轮)：{all_match_word}")
                all_match_word = sort_by_metric_type(user_object, all_match_word)
                logger.info(f"指标类型排序结果：{all_match_word}")
                for word in all_match_word:
                    final_match2phrase[word] = match2phrase[word]["phrases"]

    else:  # all_match_word 长度为0，进行无阈值推荐
        logger.info(f"指标识别结果为空-进入分析推荐")

        phrases2positionList = get_window_phrases_for_metric_recommend(user_object)  # 剔除字形匹配的指标
        logger.info(f"指标推荐的分词，不考虑sql抽取结果：{list(phrases2positionList.keys())}")
        # 获取所有窗口子串
        result_words = list(phrases2positionList.keys())
        embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words), scores_threshold=0)

        no_threshold_reranker = []
        num = 0
        for item in embedding_results:
            phrases = result_words[num]
            num += 1
            recall_list = [info['payload']['value'] for info in item if info['score'] >= 0]
            if len(recall_list) > 0:
                rerank_result = get_bge_reranker(phrases, recall_list)
                no_threshold_reranker += [{"phrases": phrases, 'value': k, 'score': v} for k, v in rerank_result.items()]

        no_threshold_sorted_reranker = sorted(no_threshold_reranker, key=lambda x: x['score'], reverse=True)
        logger.info(f"指标待推荐列表（前10个）：{no_threshold_sorted_reranker}")
        unique_results_reranker = metric_recommend(no_threshold_sorted_reranker)
        logger.info(f"指标平方差推荐结果：{unique_results_reranker}")
        positive_list = [info for info in unique_results_reranker if info["score"] > 0]
        if len(positive_list) > 0:
            unique_results_reranker = positive_list
            logger.info(f"只保留正相关：{positive_list}")
        if len(unique_results_reranker) == 1:  # 如果只有1个，则不进入多轮
            all_match_word = [unique_results_reranker[0]["value"]]
            if all_match_word[0] not in user_object.slot_dict["metric"]:
                user_object.metric_recognize_by_phrase.append(unique_results_reranker[0]["phrases"])

        no_threshold_values_reranker = [result['value'] for result in unique_results_reranker[:10]]
        no_threshold_values_reranker = sort_by_metric_type(user_object, no_threshold_values_reranker)
        logger.info(f"指标类型排序结果：{no_threshold_values_reranker}")

        for item in unique_results_reranker[:10]:
            final_match2phrase[item['value']] = item['phrases']

    return all_match_word, no_threshold_values_reranker, final_match2phrase


def llm_choose_metric(user_object, sentense, metric_list):

    sql_word = []
    if "WHERE" in user_object.test2sql_information.keys():
        sql_where = user_object.test2sql_information["WHERE"]
        sql_word = sql_word + sql_where
    if "GROUP" in user_object.test2sql_information.keys():
        sql_group = user_object.test2sql_information["GROUP"]
        sql_word = sql_word + sql_group

    prompt = f'''基于用户的输入从全部指标中选择出对应的指标，如所有指标都不能明确反应用户意图，则不选择任何指标

用户的输入为：{sentense.replace(' ', '')}\n'''
    if len(sql_word) > 0:
        prompt += f'''其中{sql_word}是筛选条件，可能不是判断指标的依据\n'''
#     prompt += f'''全部指标列表为：{metric_list}
# 规则：
# 1、你选择的指标可以是0个，可以是1个，也可以是多个，你选择的指标必须和用户问题明显相关
# 2、如果有多个相关指标，以列表 ['x1', 'x2'....] 形式按照相关性从大到小排列输出，不要输出任何其他或分析的内容
# 3、结果输出仅是一个列表，无法判断或没有合适的指标则返回空列表 []'''
    prompt += f'''全部指标列表为：{metric_list}
规则：
1.你选择的指标可以是0个、1个或多个，但必须与用户问题直接相关。不要选择间接或模糊相关的指标。
2.如果用户输入的信息可能被多个指标包含，则返回多个指标。
3.当用户问题意图明确指向多个指标时，返回一个按相关性从高到低排序的列表['x1', 'x2', ...]。
4.如果用户问题意图仅指向单一指标，则仅返回该指标['x']。
5.无法判断或没有合适的指标时，返回空列表[]。

要求：不要给出任何解释说明，直接以列表的形式返回最终结果。'''

    # print(prompt)
    result = send_llm(prompt)
    result = find_max_list(result)
    final_list = [metric for metric in result if metric in metric_list]
    return final_list




def get_metric_word_vector_match(metric_word, collection_name):
    threshold = 0.81
    # 获取所有窗口子串

    embedding_results = send_embedding_message(metric_word, collection_name, scores_threshold=0)

    # 筛选和排序结果
    filtered_results = [item for item in embedding_results if item['score'] >= threshold]
    origin_filtered_results = [item for item in embedding_results if item['score'] >= 0]
    sorted_results = sorted(filtered_results, key=lambda x: x['score'], reverse=True)
    origin_sorted_results = sorted(origin_filtered_results, key=lambda x: x['score'], reverse=True)
    # 处理数据
    processed_results = [{'value': item['payload']['value'], 'score': item['score']} for item in sorted_results]
    origin_processed_results = [{'value': item['payload']['value'], 'score': item['score']} for item in origin_sorted_results]
    # 去除重复项
    unique_results = []
    seen_values = set()
    for item in processed_results:
        if item['value'] not in seen_values:
            unique_results.append(item)
            seen_values.add(item['value'])

    selected_values = [result['value'] for result in unique_results[:6]]

    unique_results = []
    seen_values = set()
    for item in origin_processed_results:
        if item['value'] not in seen_values:
            unique_results.append(item)
            seen_values.add(item['value'])

    # 选取前6个唯一值
    origin_selected_values = [result['value'] for result in unique_results[:6]]

    # 选取前6个唯一值

    return selected_values, origin_selected_values
