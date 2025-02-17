import json

import requests

import config


def send_llm(message):
    # return send_proxy_deepseek_message(message)


    data = {
        "model": config.MODEL_NAME,
        "messages": [
            {"role": "user", "content": message}
        ],
        "temperature": 0.0
    }

    try:
        response = requests.post(config.LLM_MODEL_URL, json=data, verify=False)
        if response.status_code == 200:
            answer = response.json()["choices"][0]["message"]['content']
            # print('----LLM输出如下')
            # print(answer)
            return answer
        else:
            print(f"Error: {response.status_code}", response)
            return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None


def send_llm_stream(message):
    # print("deepseek")
    # yield send_proxy_deepseek_message_stream(message)

    data = {
        "model": config.MODEL_NAME,
        "messages": [
            {"role": "user", "content": message}
        ],
        "stream": True,
        "temperature": 0.0
    }

    response_content = ''
    try:
        with requests.post(config.LLM_MODEL_URL, json=data, stream=True, verify=False) as response:
            if response.status_code == 200:
                for chunk in response.iter_content(chunk_size=None):  # 流式读取每个数据块
                    if chunk:  # 过滤掉保持连接的空行
                        try:
                            # print(chunk)
                            chunk_text = chunk.decode('utf-8', errors='ignore').replace('data: ', '').strip()
                            # 过滤掉不需要的内容
                            if "[DONE]" in chunk_text:
                                yield response_content
                                break
                            chunk_json = json.loads(chunk_text)
                            text = chunk_json['choices'][0]['delta'].get('content', '')
                            response_content += text
                            yield response_content

                        except Exception as e:
                            print(e)
                            yield response_content
            else:
                print(f"Error: {response.status_code}", response)
                yield response_content
    except requests.RequestException as e:
        print(f"Request error: {e}")
        yield response_content


def send_llm_system(system_prompt, message):

    # return send_llm_system_deepseek(system_prompt, message)
    data = {
        "model": config.MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ],
        "temperature": 0.0
    }

    try:
        response = requests.post(config.LLM_MODEL_URL, json=data, verify=False)
        if response.status_code == 200:
            answer = response.json()["choices"][0]["message"]['content']
            return answer
        else:
            print(f"Error: {response.status_code}", response)
            return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None



def send_llm_system_stream(system_prompt, message):

    # yield send_llm_system_stream_deepseek(system_prompt, message)

    data = {
        "model": config.MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ],
        "stream": True,
        "temperature": 0.0
    }

    response_content = ''
    try:
        with requests.post(config.LLM_MODEL_URL, json=data, stream=True, verify=False) as response:
            if response.status_code == 200:
                for chunk in response.iter_content(chunk_size=None):  # 流式读取每个数据块
                    if chunk:  # 过滤掉保持连接的空行
                        try:
                            chunk_text = chunk.decode('utf-8', errors='ignore').replace('data: ', '').strip()
                            # 过滤掉不需要的内容
                            if "[DONE]" in chunk_text:
                                # yield response_content
                                break
                            chunk_json = json.loads(chunk_text)
                            text = chunk_json['choices'][0]['delta'].get('content', '')
                            if len(text.strip()) == 0:
                                continue
                            response_content += text
                            yield response_content

                        except Exception as e:
                            print(e)
                            yield response_content
            else:
                print(f"Error: {response.status_code}", response)
                yield response_content
    except requests.RequestException as e:
        print(f"Request error: {e}")
        yield response_content


def send_embedding_message(query_word, name, scores_threshold):
    url = config.Embedding_Service_URL
    data = {
        "messages": [
            {
                "content": query_word,
                "collection_name": name,
                "scores_threshold": scores_threshold
            }
        ]
    }
    try:
        response = requests.post(url, json=data)
        if response.status_code == 200:
            answer = response.json()
            # print(answer)
            return answer
        else:
            print(f"Error: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None


def batch_send_embedding_message(query_list, column_count, collection_name_list, scores_threshold):
    url = config.Embedding_Service_URL_BATCH
    data = {
        "messages": [
            {
                "content": query_list,
                "column_count": column_count,
                "collection_name": collection_name_list,
                "scores_threshold": scores_threshold
            }
        ]
    }
    try:
        response = requests.post(url, json=data)
        if response.status_code == 200:
            answer = response.json()
            # print(answer)
            return answer
        else:
            print(f"Error: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None


def get_bge_reranker(query, sentences_list):
    """
    同步地获取句子的嵌入向量。
    """
    url = config.BGE_RANKER_URL
    data = {
        "query": query,
        "sentences": sentences_list
    }
    default_response = {}

    try:
        # 使用 requests 发送 POST 请求
        response = requests.post(url, json=data)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            print(f"向量化失败, 请求失败: 状态码 {response.status_code}")
            return default_response
    except Exception as e:
        print(f"向量化失败, 无法请求: {e}")
        return default_response


def send_proxy_deepseek_message(message):

    deepseek_URL = 'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation'
    deepseek_API_KEY = 'sk-d8a225949e8641a58f38c4482d598ba7'

    headers = {
        "Authorization": f"Bearer {deepseek_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": "deepseek-v3",
        "input": {
            "messages": [
                        {"role": "user", "content": f"{message}"}
                    ]
            },
        "parameters": {
            "temperature": 0
        }
    }

    try:
        response = requests.post(deepseek_URL, headers=headers, json=data, verify=False)
        if response.status_code == 200:
            answer = response.json()["output"]["choices"][0]["message"]['content']
            return answer
        else:
            print(f"Error: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None


def send_proxy_deepseek_message_stream(message):
    deepseek_URL = 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions'
    deepseek_API_KEY = 'sk-d8a225949e8641a58f38c4482d598ba7'

    headers = {
        "Authorization": f"Bearer {deepseek_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": "deepseek-v3",
        "messages": [
            {
                "role": "user",
                "content": message
            }
        ],
        "stream": True,
        "stream_options": {
            "include_usage": True
        },
        "temperature": 0
    }
    response_content = ''
    try:
        with requests.post(deepseek_URL, headers=headers, json=data, stream=True) as response:
            if response.status_code == 200:
                for chunk in response.iter_content(chunk_size=None):  # 流式读取每个数据块
                    if chunk:  # 过滤掉保持连接的空行
                        try:
                            chunk_text = chunk.decode('utf-8').strip()
                            # 过滤掉不需要的内容
                            if "[DONE]" in chunk_text:
                                # yield response_content
                                break
                            json_objects = chunk_text.split('data: ')[1:][0]  # 去除"data: "前缀
                            chunk_json = json.loads(json_objects)
                            choices = chunk_json['choices']
                            if len(choices) == 0:
                                continue
                            text = choices[0]['delta'].get('content', '')
                            if len(text.strip()) == 0:
                                continue
                            response_content += text
                            yield response_content

                        except Exception as e:
                            print(e)
                            yield response_content
            else:
                print(f"Error: {response.status_code}", response)
                yield response_content
    except requests.RequestException as e:
        print(f"Request error: {e}")
        yield response_content




def send_llm_system_deepseek(system_prompt, message):

    deepseek_URL = 'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation'
    deepseek_API_KEY = 'sk-d8a225949e8641a58f38c4482d598ba7'

    headers = {
        "Authorization": f"Bearer {deepseek_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": "deepseek-v3",
        "input": {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message}
                    ]
            },
        "parameters": {
            "temperature": 0
        }
    }

    try:
        response = requests.post(deepseek_URL, headers=headers, json=data, verify=False)
        if response.status_code == 200:
            answer = response.json()["output"]["choices"][0]["message"]['content']
            return answer
        else:
            print(f"Error: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None


def send_llm_system_stream_deepseek(system_prompt, message):
    deepseek_URL = 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions'
    deepseek_API_KEY = 'sk-d8a225949e8641a58f38c4482d598ba7'

    headers = {
        "Authorization": f"Bearer {deepseek_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": "deepseek-v3",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ],
        "stream": True,
        "stream_options": {
            "include_usage": True
        },
        "temperature": 0
    }
    response_content = ''
    try:
        with requests.post(deepseek_URL, headers=headers, json=data, stream=True) as response:
            if response.status_code == 200:
                for chunk in response.iter_content(chunk_size=None):  # 流式读取每个数据块
                    if chunk:  # 过滤掉保持连接的空行
                        try:
                            chunk_text = chunk.decode('utf-8').strip()
                            # 过滤掉不需要的内容
                            if "[DONE]" in chunk_text:
                                # yield response_content
                                break
                            json_objects = chunk_text.split('data: ')[1:][0]  # 去除"data: "前缀
                            chunk_json = json.loads(json_objects)
                            choices = chunk_json['choices']
                            if len(choices) == 0:
                                continue
                            text = choices[0]['delta'].get('content', '')
                            if len(text.strip()) == 0:
                                continue
                            response_content += text
                            yield response_content

                        except Exception as e:
                            print(e)
                            yield response_content
            else:
                print(f"Error: {response.status_code}", response)
                yield response_content
    except requests.RequestException as e:
        print(f"Request error: {e}")
        yield response_content