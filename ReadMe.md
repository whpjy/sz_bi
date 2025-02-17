## 如何部署 
    1、选择2022的服务器
    2、进入docker：  docker exec -it 8c099fcd5ebc /bin/bash
    3、切换conda环境： conda activate Langchain
    4、切换目录： cd /home/model-server/model-store
    5、执行脚本： sh start.sh   (此脚本会同时启动三个项目，lianyungang、sale、lianyungang_embedding)

##  各个目录说明

### config 
    配置请求已经部署的大模型API

### data 
    data_process 将提供的原始指标、字段转换成新格式，其中包括知识图谱需要的格式
    historical_question_records 用户问题历史记录
    knowledge_graph  构建知识图谱
    pretrain 放置预训练模型

### scene_config 
    放置prompt和槽位模版

### models 
    放置项目最核心的一个类 chatbot_model, 先场景识别再进入场景细节

### scene_processor_detail 
    具体每个场景的不同细节处理，调用信息抽取，后续处理，返回结果

### scene_processor 
    进行信息抽取和槽位更新

### utils 
    项目所有的工具处理函数都写在helpers，比较混乱，send_llm放调用大模型的函数

### main_api_new 
    部署api
