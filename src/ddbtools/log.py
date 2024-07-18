# 文件名: mylogger.py
import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)   # 设置总日志等级

    format = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y年%m月%d日 %H:%M:%S %z')  # 日志格式

    # cli_handler = logging.StreamHandler()  # 输出到屏幕的日志处理器
    file_handler = logging.FileHandler(filename='run.log', mode='a', encoding='utf-8')  # 输出到文件的日志处理器

    # cli_handler.setFormatter(format)  # 设置屏幕日志格式
    file_handler.setFormatter(format)  # 设置文件日志格式

    # cli_handler.setLevel(logging.INFO)  # 设置屏幕日志等级, 可以大于日志记录器设置的总日志等级
    # file_hander.setLevel(logging.DEBUG)  # 不设置默认使用logger的等级

    logger.handlers.clear()  # 清空已有处理器, 避免继承了其他logger的已有处理器
    # logger.addHandler(cli_handler)  # 将屏幕日志处理器添加到logger
    logger.addHandler(file_handler)  # 将文件日志处理器添加到logger
    return logger

logger = get_logger("ddbtool_logger")