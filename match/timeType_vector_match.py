from utils.request_util import batch_send_embedding_message
from utils.window_phrases import get_window_phrases


def get_timeType_vector_match(user_object, collection_name):

    user_object.jieba_window_phrases2positionList = get_window_phrases(user_object)  # 剔除字形匹配的指标
    sentence_jieba_position_use = []
    threshold = 0.81
    phrase_recall_info = []
    all_match_word = []
    match2phrase = {}

    # 获取所有窗口子串
    result_words = list(user_object.jieba_window_phrases2positionList.keys())
    if len(result_words) == 0:
        return []
    embedding_results = batch_send_embedding_message(result_words, 1, [collection_name] * len(result_words), scores_threshold=0)

    num = 0
    for item in embedding_results:
        phrases = result_words[num]
        num += 1
        position_list = user_object.jieba_window_phrases2positionList[phrases]
        recall_list = [info['payload']['value'] for info in item if info['score'] >= threshold]
        if len(recall_list) > 0:
            max_score = item[0]['score']
            new_dict = {"phrases": phrases, "position_list": position_list, "recall_list": recall_list, "max_score": max_score}
            phrase_recall_info.append(new_dict)

    phrase_recall_info = sorted(phrase_recall_info, key=lambda x: (x["max_score"], len(x["position_list"])), reverse=True)

    for info in phrase_recall_info:
        intersection_list = [value for value in info["position_list"] if value in sentence_jieba_position_use]
        if len(intersection_list) == 0:
            sentence_jieba_position_use = sentence_jieba_position_use + info["position_list"]
            for recall_word in info["recall_list"]:
                all_match_word.append(recall_word)
                match2phrase[recall_word] = info["phrases"]

    if len(all_match_word) == 1:
        if all_match_word[0] not in user_object.slot_dict["timeType"]:
            user_object.metric_recognize_by_phrase.append(match2phrase[all_match_word[0]])

    return all_match_word
