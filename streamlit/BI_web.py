import requests
import streamlit as st

st.set_page_config(
    page_title="ChatApp",
    page_icon=" ",
    layout="wide",
)
st.title("ChatBI")

if 'show_container' not in st.session_state:
    st.session_state.show_container = True

# 数据准备
guidance1 = []
guidance2 = []

if st.session_state.show_container:
    response = requests.get("http://127.0.0.1:7074/get_guidance_information")
    if response.status_code == 200:
        # 解析响应的JSON数据
        response_data = response.json()
        if "guidance1" in response_data.keys():
            guidance1 = response_data["guidance1"]
        if "guidance2" in response_data.keys():
            guidance2 = response_data["guidance2"]

    with st.container():
        # 创建左右两栏
        col0, col1, col2, col4 = st.columns(4)

        # 左侧栏
        with col1:
            st.markdown("#### 常用指标")
            for item in guidance1:
                if st.button(item, key=item):
                    st.session_state["user_input"] = item
                    st.session_state["submit"] = True
                    st.session_state.show_container = False
                    st.rerun()

        # 右侧栏
        with col2:
            st.markdown("#### 其他咨询")
            for item in guidance2:
                if st.button(item, key=f"button_{item}"):  # 使用不同的key以避免冲突
                    st.session_state["user_input"] = item
                    st.session_state["submit"] = True
                    st.session_state.show_container = False
                    st.rerun()


# 给对话增加history属性，将历史对话信息储存下来
if "history" not in st.session_state:
    st.session_state.history = []
    st.session_state.post_history = []
    st.session_state.rerun_flag = False

with st.sidebar:
    st.markdown('当前槽位状态')
    st.text('')

# 显示历史信息
for message in st.session_state.history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 这里是你的大模型生成的回复
def get_response_material(user_input, history):

    # 目标URL
    url = 'http://127.0.0.1:7074/v1/chat/completions'

    # 构造请求的JSON数据
    data = {
        "messages":[
            {
                "content": user_input,
                "user_id": "admin",
                "analyzeType": "",
                "history": history,
                "stream": False
            }
        ]
    }

    # 发送POST请求
    response = requests.post(url, json=data)
    print(response)
    # 检查请求是否成功
    if response.status_code == 200:
        # 解析响应的JSON数据
        response_data = response.json()
        return response_data

    else:
        print(f"Request failed with status {response.status_code}")
        return None


# user_input接收用户的输入
if user_input := st.chat_input("请输入您想咨询的信息") or ("submit" in st.session_state and st.session_state.get("submit")):

    if user_input and st.session_state.show_container:
        st.session_state.show_container = False
        st.session_state["user_input"] = user_input
        st.session_state["submit"] = True
        st.rerun()

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

    result = get_response_material(user_input, st.session_state.post_history)
    # st.session_state.slot_status = result["slot_status"]
    st.session_state.slot_status = {}

    # 使用一个左侧框，展示检索到的信息，如果不需要显示检索信息删掉即可
    with st.sidebar:
        # st.markdown('当前槽位状态')
        for k, v in st.session_state.slot_status.items():
            output = str(k) + ": " + str(v)
            st.text(output)

    if isinstance(result["context"], str):
        colon_pos = result["context"].find(':')
        if colon_pos < 0:
            with st.chat_message("assistant"):
                st.markdown(result["context"])
            h_assistant = {"role": "assistant", "content": result["context"]}
            st.session_state.history.append(h_assistant)
        else:
            intro_text = result["context"][:colon_pos].strip()
            expenditures_str = result["context"][colon_pos + 1:].strip()
            expenditure = expenditures_str.split(',')
            h_assistant = {"role": "assistant", "content": intro_text}
            st.session_state.history.append(h_assistant)

            with st.chat_message("assistant"):
                st.markdown(intro_text)

            with st.chat_message("assistant"):
                for word in expenditure:
                    def on_click(word=word):
                        st.session_state["user_input"] = word
                        st.session_state["submit"] = True
                        st.session_state.rerun_flag = True
                    # 添加按钮
                    st.button(word, on_click=on_click)

    else:
        with st.chat_message("assistant"):
            st.markdown(result["context"])
        h_assistant = {"role": "assistant", "content": result["context"]}
        st.session_state.history.append(h_assistant)
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