import json
import os
import time
import traceback

from config import CURRENT_SCENE, write_lock, LANGUAGE_MODE
from config.log_config import clear_log_file, setup_logger, logger, clear_log_file_chart_data, logger_chart_data, \
    logger_record_uer_problem
from core.main_logic import main_logic_deal
from core.public_data import PublicMetricData
from core.user_class import user_class
from fastapi import FastAPI
import uvicorn
from fastapi.responses import StreamingResponse

from extract_information.extract_question_type import extract_question_type
from bi_agent.function_intent import function_intention_analysis, function_intention_analysis_stream
from interface.recommended_indicators import get_metric_vector_match_for_interface
from utils.json_util import convert_json_to_simplified, convert_json_to_traditional, convert_json_to_traditional_final, \
    generate_fake_stream_response, get_targetName_describe
from utils.reply import unrelated_intent_reply, function_intent_reply
from utils.util import continue_multiple_rounds_of_judgment, get_data_fenxi, refactoring_history, get_zhibiao_list, get_data_fenxi_stream

app = FastAPI(reload=True)
# 存放已创建的用户实例
user_dict = {}

public_data_class = PublicMetricData()

@app.post("/v1/chat/completions")
async def receive_and_forward(request_data: dict):
    time1 = time.time()
    print("\n收到请求时间：", time1)
    messages = request_data.get("messages", [])
    logger.info(f"\n收到请求: {messages}")
    if not messages:
        logger.error("No messages found in the request.")
        return {"error": "No messages found in the request."}

    first_message = messages[0] if messages else {}
    user_input = first_message.get('content', "")
    user_id = first_message.get('user_id')
    history = first_message.get('history')
    stream = first_message.get('stream')

    if stream is None:
        stream = False

    if public_data_class.metric_data_status:
        print("*****", "用户输入: ", user_input)
        if LANGUAGE_MODE == "traditional":  # 如果配置是繁体，向量库和中间处理过程都先转换简体，输出时候再换繁体
            user_input = convert_json_to_simplified(user_input)
            history = convert_json_to_simplified(history)

        if not user_input:
            logger.error("No question provided")
            return {"error": "No question provided"}

        if user_id not in user_dict.keys():
            new_example = user_class()
            user_dict[user_id] = new_example

        user_object = user_dict[user_id]

        logger.info(f"检查user_object.history：{user_object.history}")

        user_object = refactoring_history(user_object, user_input, history)
        user_input = list(user_object.history[-1].values())[0]
        # if len(history)>0 and not continue_multiple_rounds_of_judgment(user_input,history):
        #     user_object.history = []
        print("更新user_object.history：", user_object.history)
        logger.info(f"更新user_object.history：{user_object.history}")

        if len(user_object.history) == 1:
            # clear_log_file()
            logger_record_uer_problem.info(f"user_id: {user_id}, question: {user_input}")
            question_type = extract_question_type(user_input, public_data_class.all_metric_data)
            logger.info(f"问题类型: {question_type}")
            print("问题类型：", question_type)
        else:
            question_type = "数据查询或统计分析"

        # question_type = "其他"

        logger.info(f"用户: {user_id}")
        logger.info(f"输入: {user_input}")
        logger.info(f"当前user_object.history: {user_object.history}")
        all_metric_data = public_data_class.all_metric_data
        user_object.all_metric_data = all_metric_data
        if question_type != "数据查询或统计分析":
            user_object.history = []
            if stream:
                result = StreamingResponse(function_intention_analysis_stream(public_data_class.targetDefine_dict, user_input, user_object),
                                     media_type="application/json; charset=utf-8")
                return result
            else:
                answer = function_intention_analysis(public_data_class.targetDefine_dict, user_input, user_object)
                return function_intent_reply(answer)
                # if LANGUAGE_MODE == "simplified":
                #     return function_intent_reply(answer)
                # else:
                #     return convert_json_to_traditional(function_intent_reply(answer))
        else:
            try:
                response = main_logic_deal(user_object, user_input, public_data_class.table_describe)
            except Exception as e:
                user_object.history = []
                error_traceback = traceback.format_exc()
                logger.info(error_traceback)
                response = {"error": str(e) }

            # 尽管分类到这里，在识别指标过程中没有找到任何合适的指标，再返回到知识库问答的分类中去
            if "need_knowledge_qa" in response.keys():
                user_object.history = []
                if stream:
                    result = StreamingResponse(
                        function_intention_analysis_stream(public_data_class.targetDefine_dict, user_input,
                                                           user_object),
                        media_type="application/json; charset=utf-8")
                    return result
                else:
                    answer = function_intention_analysis(public_data_class.targetDefine_dict, user_input, user_object)
                    return function_intent_reply(answer)
            else:
                time2 = time.time()
                print("总计耗时：", time2 - time1)
                print("response", response)
                logger.info(f"最终回复: {str(response)}")
                logger.info(f"总计耗时: {time2 - time1}\n")
                if LANGUAGE_MODE == "simplified":
                    if stream:
                        return StreamingResponse(generate_fake_stream_response(response),
                                         media_type="application/json; charset=utf-8")
                    else:
                        return response
                else:
                    origin_targetId2Name = public_data_class.origin_targetId2Name
                    response = convert_json_to_traditional_final(response, origin_targetId2Name)
                    if stream:
                        return StreamingResponse(generate_fake_stream_response(response),
                                         media_type="application/json; charset=utf-8")
                    else:
                        return response

    else:
        response = unrelated_intent_reply("获取指标数据异常！")
        logger.info(f"获取指标数据异常！")
        print("获取指标数据异常！")
        if LANGUAGE_MODE == "simplified":
            return response
        else:
            return convert_json_to_traditional(response)


@app.post("/v1/chat/atlas")
async def get_knoeledge_graph_by_metric():
    # 前端展示根据指标获得的知识图谱
    data = {}
    return data



@app.post("/v1/chat/chart_interpretation")
async def chart_interpretation(request_data: dict):
    # print(request_data)
    # with open('data_fenxi.json', 'w', encoding='utf-8') as f:
    #     json.dump(request_data, f, ensure_ascii=False, indent=4)
    messages = request_data.get("messages", [])
    history = []
    if not messages:
        logger.error("No messages found in the request.")
        data_content = request_data
        return {"error": " No messages found in the request."}
    else:
        clear_log_file_chart_data()
        first_message = messages[0] if messages else {}
        data_content = first_message.get('data')
        user_id = first_message.get('user_id')
        stream = first_message.get('stream')
        if stream is None:
            stream = False

        if user_id not in user_dict.keys():
            new_example = user_class()
            user_dict[user_id] = new_example

        user_object = user_dict[user_id]
        history = user_object.history
        logger_chart_data.info(f"user_id：{user_id}")
        logger_chart_data.info(f"user_history：{history}")
        logger_chart_data.info(f"data：\n {json.dumps(data_content, indent=4, ensure_ascii=False)}")

        if stream:
            return StreamingResponse(get_data_fenxi_stream(history, data_content),
                                     media_type="application/json")
        else:
            fenxi = get_data_fenxi(history, data_content)
            result = {"result": fenxi}
            return result


@app.post("/v1/chat/targetName_describe")
async def targetName_describe(request_data: dict):
    # print(request_data)
    # with open('data_fenxi.json', 'w', encoding='utf-8') as f:
    #     json.dump(request_data, f, ensure_ascii=False, indent=4)
    messages = request_data.get("messages", [])
    if not messages:
        data_content = request_data
        return {"error": " No messages found in the request."}
    else:

        first_message = messages[0] if messages else {}
        targetName = first_message.get('targetName')
        targetDefine = first_message.get('targetDefine')

        describe = get_targetName_describe(targetName, [], targetDefine)

        result = {"describe": describe}
        return result

@app.get("/get_guidance_information")
async def get_guidance_information():
    path = 'indicator_frequency/' + CURRENT_SCENE + '.json'
    guidance1 = []
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as fr:
                indicator_frequency_dict = json.load(fr)
                indicator_frequency_dict = dict(sorted(indicator_frequency_dict.items(), key=lambda x: x[1], reverse=True))
                guidance1 = list(indicator_frequency_dict.keys())[:5]
        except Exception as e:
            print(e)

        if len(guidance1) == 0:
            all_metric_data = public_data_class.all_metric_data
            zhibiao_list = get_zhibiao_list(all_metric_data)
            indicator_frequency_dict = {zhibiao: 0 for zhibiao in zhibiao_list}
            guidance1 = list(indicator_frequency_dict.keys())[:5]
    else:
        all_metric_data = public_data_class.all_metric_data
        zhibiao_list = get_zhibiao_list(all_metric_data)
        indicator_frequency_dict = {zhibiao: 0 for zhibiao in zhibiao_list}
        guidance1 = list(indicator_frequency_dict.keys())[:5]
        async with write_lock:
            with open(path, 'w', encoding='utf-8') as fw:
                json.dump(indicator_frequency_dict, fw, ensure_ascii=False, indent=4)

    guidance2 = ['如何使用？', '有多少指标？', '指标有哪些？', 'xx相关的指标有什么？', '测试']

    result = {"guidance1": guidance1, "guidance2": guidance2}
    return result


@app.post("/get_relevant_indicators")
async def get_relevant_indicators(request_data: dict):
    time1 = time.time()
    print("收到请求时间：", time1)
    messages = request_data.get("messages", [])
    if not messages:
        return {"error": "No messages found in the request."}

    first_message = messages[0] if messages else {}
    question = first_message.get('content', "")

    if not question:
        return {"error": "No question provided"}

    collection_name = CURRENT_SCENE + "_zhibiao"
    response = get_metric_vector_match_for_interface(question, collection_name)

    time2 = time.time()
    print("总计耗时：", time2 - time1)
    print("response", response)

    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7074)