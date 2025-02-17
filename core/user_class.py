# encoding=utf-8

from utils.util import get_clear_slot_dict


class user_class:
    def __init__(self):
        self.slot_dict = get_clear_slot_dict()
        self.extract_plan = "意图"
        self.history = []  # 存储历史信息
        # self.history_for_chart = []  # 每一次意图达成，history_for_chart记录history，然后history被清空
        self.all_metric_data = {}  # 存储全部的实时数据
        self.jieba_window_phrases2positionList = {}  # 分词后的词语组合和词语位置对应的信息
        self.test2sql_information = {}  # 通过text2sql的方式抽取关键信息，后续词语语义计算可基于此做阈值调整
        self.rewrite_input = ''  # 分词前会剔除输入中已识别的信息，记录最新的剔除信息的输入

        self.metric2id = {}  # 指标转id字典
        self.metric_type = {}  # 指标类型，如 原子指标、派生指标、组合指标
        self.metric_table = {}  # 指标下的所有表，用于多轮推荐表，以及最终“表-指标”的整合
        self.metric_aggregation = {}  # 指标下的所有聚合条件，存在多个有模型判断不进多轮，用于最终“表-指标聚合”的整合
        self.metric_knowledge_graph = {}  # 指标生成的图谱
        self.metric_recognize_by_phrase = []  # 记录确定指标的是哪些文字片段
        self.metric_mul_turn_match2phrase = {}  # 当指标进行多轮推荐后，这个参数记录推荐的词对应的文字片段，方便多轮中识别

        self.table_ask_list = []  # 多指标都有多表需要推荐，先准备好所有推荐信息，逐个推荐
        self.table_name2id = {}  # 表名转id字典
        self.table_id2name = {}  # id转表名字典
        self.table_relation = []  # 表关联关系
        self.table_attribute_list = []  # 根据图谱找到指标->表后，查看表是否有 time、group、type...等条件，根据条件逐个抽取

        self.time_name2id_dict = {}  # 时间口径转id字典

        self.group_recognize_by_phrase = []  # 记录确定group的是哪些文字片段
        self.group_phrase_start_pos = {}  # 记录确定group的片段的起始位置，方便group按出现顺序排序
        self.group_ask_list = []  # 多个片段，每个片段都召回了多个确定不了的group词进行推荐，每次删除一个逐渐推荐

        self.where_recognize_by_phrase_pos = []  # 记录用于确定where的片段，有些片段需要多轮尽管没有最终确定，也被记录，同时记录位置
        self.phrase_recall_info = {}  # where某个维度召回多个难区分的进入多轮，如果多个维度都召回多个，每次删除一个逐渐推荐
        self.relation_slot_dict = []  # 从当前指标的所有关联表维度识别的完整结果，临时存放，自身维度的多轮进行完之后再使用这个
        self.where_relation_mul_turn = []  # 从当前指标的所有关联表维度识别重复值，预先构造好所有问答对，逐个进入多轮
        self.relation_table_id = []  # 当前指标的关联表id
        self.where_fuzzy_dict = []  # 每个where的模糊值同时存在多个列表，每次删除一个逐渐推荐
        self.where_weidu_list = []  # 记录当前的全部维度
        self.self_where_weidu_list = []  # 记录当前的全部维度

        self.multi_recommendation_recognition = {}  # 一次意图达成之后新输入，这个保留达成过程中推荐的指标、表的用户的最终选择，同时有值也说明在处在达成后的新改写多轮状态
