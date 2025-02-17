import json

import requests
import streamlit as st

st.set_page_config(
    page_title="ChatApp",
    page_icon=" ",
    layout="wide",
)
st.title("测试")

# 给对话增加history属性，将历史对话信息储存下来
if "history" not in st.session_state:
    st.session_state.history = []
    st.session_state.post_history = []
    st.session_state.rerun_flag = False

with st.sidebar:
    st.markdown('当前栏目')
    st.text('')

# 显示历史信息
for message in st.session_state.history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

url = 'http://127.0.0.1:7074/v1/chat/completions'


def get_para_data(user_input, history):
    para_data = {
        "messages": [
            {
                "content": user_input,
                "user_id": "admin",
                "analyzeType": "",
                "history": history,
                "stream": True
            }
        ]
    }
    return para_data


def get_mult_dict(mult):
    mult_dict = {}
    for info in mult:
        if "prompt" in info.keys():
            mult_dict["prompt"] = info["prompt"]
        if "expenditures" in info.keys():
            mult_dict["expenditures"] = [name_info["name"] for name_info in info["expenditures"]]

    return mult_dict


if user_input := st.chat_input("请输入您想咨询的信息") or ("submit" in st.session_state and st.session_state.get("submit")):
    if "submit" in st.session_state and st.session_state.get("submit"):
        if st.session_state.rerun_flag:
            st.session_state.rerun_flag = False
            st.rerun()

        user_input = st.session_state["user_input"]
        del st.session_state["submit"]
        del st.session_state["user_input"]

    with st.chat_message("user"):
        st.markdown(user_input)

    h_user = {"role": "user", "content": user_input}
    st.session_state.history.append(h_user)

    with st.chat_message("assistant"):
        output_container = st.empty()

    para_data = get_para_data(user_input, st.session_state.post_history)

    response = requests.post(url, json=para_data, stream=True)
    if response.status_code == 200:
        for line in response.iter_lines(decode_unicode=True):
            if line:
                result = json.loads(line)
                context = result["context"]
                if "DONE" not in result.keys():
                    output_container.markdown(context)

                else:
                    # all_context += "[DONE]"
                    # output_container.markdown(all_context)
                    h_assistant = {"role": "assistant", "content": context}
                    st.session_state.history.append(h_assistant)

                    relevantIndicator = []
                    if "relevantIndicator" in result.keys():
                        relevantIndicator = result["relevantIndicator"]
                        if len(relevantIndicator) > 0:
                            output_container.markdown(context)
                            # output_container.markdown(relevantIndicator)
                            indicator_name_list = [indicator['targetName'] for indicator in relevantIndicator]
                            indicator_targetDefine_list = []
                            for indicator in relevantIndicator:
                                if len(indicator['targetDefine']) > 50:
                                    indicator_targetDefine_list.append(indicator['targetDefine'][:50] + '...')
                                else:
                                    indicator_targetDefine_list.append(indicator['targetDefine'])

                            with st.chat_message("assistant"):
                                for index, word in enumerate(indicator_name_list):
                                    def on_click(word=word):
                                        st.session_state["user_input"] = word
                                        st.session_state["submit"] = True
                                        st.session_state.rerun_flag = True
                                    st.button(f"{word}（*{indicator_targetDefine_list[index]}*）", on_click=on_click, key=f"button_{index}")


                        else:
                            output_container.markdown(context)

                    mult = []
                    if "mult" in result.keys():
                        mult = result["mult"]

                    if len(mult) > 0:
                        mult_dict = get_mult_dict(mult)
                        prompt = mult_dict["prompt"]
                        expenditure = mult_dict["expenditures"]

                        with st.chat_message("assistant"):
                            for word in expenditure:
                                def on_click(word=word):
                                    st.session_state["user_input"] = word
                                    st.session_state["submit"] = True
                                    st.session_state.rerun_flag = True
                                # 添加按钮
                                st.button(word, on_click=on_click)

                    else:

                        # chart_url = 'http://127.0.0.1:7074/v1/chat/chart_interpretation'
                        # fr = open('streamlit/chart_data.json', 'r', encoding='utf-8')
                        # chart_data = json.load(fr)
                        # chart_response = requests.post(chart_url, json=chart_data, stream=True)
                        # if chart_response.status_code == 200:
                        #     for line in chart_response.iter_lines(decode_unicode=True):
                        #         if line:
                        #             result = json.loads(line)
                        #             print(result)
                        #             if isinstance(result["context"], str):
                        #                 output_container.markdown(result["context"])
                        #                 h_assistant = {"role": "assistant", "content": result["context"]}
                        #                 st.session_state.history.append(h_assistant)

                        if "value" in result.keys():
                            if "problemRecommendation" in result["value"].keys():
                                if len(result["value"]["problemRecommendation"]) > 0:
                                    with st.chat_message("assistant"):
                                        st.markdown("您还可以这样问")
                                        for word in result["value"]["problemRecommendation"]:
                                            def on_click(word=word):
                                                st.session_state["user_input"] = word
                                                st.session_state["submit"] = True
                                                st.session_state.rerun_flag = True

                                            # 添加按钮
                                            st.button(word, on_click=on_click)

                    st.session_state.post_history = result["history"]

                    if len(result["history"]) == 0:
                        st.session_state.history = []

                    if len(st.session_state.history) > 200:
                        st.session_state.messages = st.session_state.messages[-200:]

    else:
        result = None
        print(f"Request failed with status {response.status_code}")
