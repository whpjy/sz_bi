# log_config.py
import logging
import time as time_module


class BeijingTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        try:
            utc_timestamp = record.created
            local_timestamp = utc_timestamp + 8 * 3600  # 添加8小时的秒数
            time_tuple = time_module.localtime(local_timestamp)
            if datefmt:
                s = time_module.strftime(datefmt, time_tuple)
            else:
                s = time_module.strftime("%Y-%m-%d %H:%M:%S", time_tuple)
                s += ".%03d" % (record.msecs % 1000)
            return s
        except Exception as e:
            print(f"Failed to format time: {e}")
            return super().formatTime(record, datefmt)


def setup_logger(log_path):
    # 创建日志记录器
    logger = logging.getLogger("logger1")
    logger.setLevel(logging.DEBUG)  # 设置最低记录级别为DEBUG

    # 创建文件处理器
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # 设置日志格式
    file_handler.setFormatter(BeijingTimeFormatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 添加处理器到记录器
    if not logger.hasHandlers():  # 确保不会重复添加处理器
        logger.addHandler(file_handler)

    return logger

def setup_logger_chart_data(log_path):
    # 创建日志记录器
    logger = logging.getLogger("logger2")
    logger.setLevel(logging.DEBUG)  # 设置最低记录级别为DEBUG

    # 创建文件处理器
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # 设置日志格式
    file_handler.setFormatter(BeijingTimeFormatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 添加处理器到记录器
    if not logger.hasHandlers():  # 确保不会重复添加处理器
        logger.addHandler(file_handler)

    return logger


def setup_logger_record_uer_problem(log_path):
    # 创建日志记录器
    logger = logging.getLogger("logger3")
    logger.setLevel(logging.DEBUG)  # 设置最低记录级别为DEBUG

    # 创建文件处理器
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # 设置日志格式

    file_handler.setFormatter(BeijingTimeFormatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 添加处理器到记录器
    if not logger.hasHandlers():  # 确保不会重复添加处理器
        logger.addHandler(file_handler)

    return logger

def clear_log_file(log_path = "./log/problem_portrait.log"):
    with open(log_path, "w", encoding="utf-8") as log_file:
        log_file.write("")


def clear_log_file_chart_data(log_path = "./log/chart_data.log"):
    with open(log_path, "w", encoding="utf-8") as log_file:
        log_file.write("")

# 初始化日志记录器
log_path = "./log/problem_portrait.log"
logger = setup_logger(log_path)


log_path_chart_data = "./log/chart_data.log"
logger_chart_data = setup_logger_chart_data(log_path_chart_data)

log_path_record_uer_problem = "./log/record_uer_problem.log"
logger_record_uer_problem = setup_logger_record_uer_problem(log_path_record_uer_problem)
