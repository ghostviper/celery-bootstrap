# -*- coding:utf-8 -*-

__author__ = 'ghostviper'

import os
dirs = os.path.join( os.path.dirname(__file__), '../')
os.sys.path.append(os.path.join( os.path.dirname(__file__), '../'))
from celery import platforms
from celery import Celery
from config import CELERY_CONFIG
from celery.utils.log import get_task_logger


logger = get_task_logger(__name__)


result_backend = CELERY_CONFIG['RESULT_BACKEND']
app = Celery('tasks', backend=result_backend)
app.conf.broker_url = CELERY_CONFIG['BROKER_URL']
app.conf.broker_pool_limit = 10

# 任务过期时间
app.conf.update(
    result_expires=7200
)

# 每个worker最多同时可以取走多少个任务
app.conf.worker_prefetch_multiplier = 20

# 定义任务模块结构
app.conf.include = ["module1.task", "canvas_module.task"]
# 每个worker 的子进程数
app.conf.worker_max_tasks_per_child = 20
# 不采用UTC时间
app.conf.enable_utc = False
# 设置时区
app.conf.timezone = 'Asia/Shanghai'
# worker并发量
app.conf.worker_concurrency = 8
# 允许root启动
platforms.C_FORCE_ROOT = True
# 数据序列化方式
CELERY_TASK_SERIALIZER = 'pickle'


app.conf.task_routes = {
    "base.*": {
        "queue": "base",
        "routing_key": "base",
        'delivery_mode': 1
    },
    "canvas_module.task.*": {
        "queue": "task_queue_1",
        "routing_key": "task_queue_1",
        'delivery_mode': 1
    },
    "common_tasks.task.*": {
        "queue": "task_queue_2",
        "routing_key": "task_queue_2",
        'delivery_mode': 1
    }
}
