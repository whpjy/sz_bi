import asyncio

from opencc import OpenCC

print("Config package initialized.")

current_ip = '192.168.110.123'

Embedding_Service_URL = 'http://' + current_ip + ':7079/embedding/match'  # 服务器部署接口
Embedding_Service_URL_BATCH = 'http://' + current_ip + ':7079/embedding/match_batch'

BGE_RANKER_URL = 'http://' + current_ip + ':7077/bge_reranker'

# LLM 服务器接口
LLM_MODEL_URL = 'http://' + current_ip + ':9080/v1/chat/completions'
MODEL_NAME = "Qwen2.5-14B-Instruct-GPTQ-Int4"

LLM_MODEL_CFG_URL = 'http://' + current_ip +':9080/v1'

# MODEL_NAME = "Qwen-14B-Chat"
write_lock = asyncio.Lock()

# simplified: 中文简体，traditional：中文繁体
LANGUAGE_MODE = "simplified"  # 数据库的语言模式，默认为中文简体
t2s = OpenCC('t2s')  # 繁体转简体工具
s2t = OpenCC('s2t')  # 简体转繁体工具

# 场景对应的数据获取接口
SCENE_URL_DICT = {
    "fuyiyuan": "http://192.168.110.160:1088/chatAi/createModelJsonV2/6",
    "fuyiyuan_8": "http://192.168.110.160:1088/chatAi/createModelJsonV2/8",
    "fuyiyuan_11": "http://192.168.110.160:1088/chatAi/createModelJsonV2/11"
}

CURRENT_SCENE = "fuyiyuan_8"

