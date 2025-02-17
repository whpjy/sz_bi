from config import CURRENT_SCENE
from utils.json_util import find_max_list
from utils.request_util import send_llm, batch_send_embedding_message, get_bge_reranker


def get_first_question(user_object, metric_name):
    '''根据当前指标，推荐相关的组合或者派生指标，其他条件不变'''

    user_input = user_object.history[0]['user']
    time = user_object.slot_dict["time"]
    group = user_object.slot_dict["group"]
    where = user_object.slot_dict["where"]

    print("原始输入：", user_input)
    print("指标名称：", metric_name)
    print("时间条件：", time)
    print("分组条件：", group)
    print("where条件：", where)

    user_input = user_input + "附加信息：("
    if len(metric_name) > 0:
        user_input = user_input + '指标名称：' + str(metric_name) + ' '
    # if len(time) > 0:
    #     user_input = user_input + '时间条件：' + str(time) + ' '
    if len(group) > 0:
        user_input = user_input + '分组条件：' + str([info['columnName'] for info in group]) + ' '
    if len(where) > 0:
        user_input = user_input + 'where条件：' + str(where) + ' '
    user_input = user_input + " )"

    embedding_results = batch_send_embedding_message([metric_name], 1, [CURRENT_SCENE + "_zhibiao"], scores_threshold=0)
    recall_list = []
    for item in embedding_results:
        for info in item:
            recall_list.append(info['payload']['value'])

    rerank_result = get_bge_reranker(metric_name, recall_list)
    relevant_metric = list(rerank_result.keys())[:10]
    print(relevant_metric)
    prompt = f'''基于用户的原始问题和提供的信息再推荐3个问题，不要输出其他分析内容，只给出最终结果即可。
    用户原始问题{user_input}
    
    全部指标列表：{relevant_metric}
    要求如下：
    1、不要输出“附加信息”，将附加信息融合到问题里面去
    2、推荐的第一个问题的指标必须是{metric_name}，且整体意思不能和用户相同
    3、推荐的第二个和第三个问题的指标不能是{metric_name}，且互不相同
    4、推荐的三个问题以列表的形式输出，例如 ["question1", "question2", "question3"]
    '''
    answer = send_llm(prompt)
    print("推荐问题如下：")
    print(answer)

    problemRecommendation = find_max_list(answer)
    if len(problemRecommendation) > 0:
        return problemRecommendation

    return []


def get_second_question():
    '''根据当前指标，推荐相关的组合或者派生指标，其他条件不变'''
    return ''


def get_third_question():
    '''根据当前指标，推荐相关的组合或者派生指标，其他条件不变'''
    return ''


def recommend_question(user_object):

    return []

    if len(user_object.slot_dict["metric"]) > 0:
        metric_name = user_object.slot_dict["metric"][0]
        problemRecommendation = get_first_question(user_object, metric_name)
        return problemRecommendation

    return []