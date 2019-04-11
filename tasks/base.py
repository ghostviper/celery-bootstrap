# -*- coding:utf-8 -*-
# canvas测试

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
dirs = os.path.join( os.path.dirname(__file__), '../')
os.sys.path.append(os.path.join( os.path.dirname(__file__), '../'))
import json
import time
import requests
from celery import platforms
from celery import Celery
import traceback
from celery import chord, chain
from config import CELERY_CONFIG
from text.clean import url_duplication, area_filter
from celery.utils.log import get_task_logger


log = get_task_logger(__name__)

__author__ = 'ghostviper'
result_backend = CELERY_CONFIG['RESULT_BACKEND']
app = Celery('tasks', backend=result_backend)
app.conf.broker_url = CELERY_CONFIG['BROKER_URL']
app.conf.broker_pool_limit = 10

app.conf.update(
    result_expires=7200
)

# 每个worker最多同时可以取走多少个任务
app.conf.worker_prefetch_multiplier = 20


app.conf.include = ["text.clean", "text.nlp", "publish", "video.clean"]
app.conf.worker_max_tasks_per_child = 20
app.conf.enable_utc = False
app.conf.timezone = 'Asia/Shanghai'
app.conf.worker_concurrency = 8
platforms.C_FORCE_ROOT = True
CELERY_TASK_SERIALIZER = 'pickle'

# ###############TASK ROUTE########################

app.conf.task_routes = {
    "base.*": {
        "queue": "base",
        "routing_key": "base",
        'delivery_mode': 1
    },
    "publish.*": {
        "queue": "publish",
        "routing_key": "publish",
        'delivery_mode': 1
    },
    "text.clean.*": {
        "queue": "clean",
        "routing_key": "clean",
        'delivery_mode': 1
    },
    "text.nlp.*": {
        "queue": "nlp",
        "routing_key": "nlp",
        'delivery_mode': 1
    },
    "video.*": {
        "queue": "video",
        "routing_key": "video",
        'delivery_mode': 1
    }
}

# ##################COMMON PROCESS################################
@app.task
def combine_parameters(result_tuple):
    """
    合并参数
    :param result_tuple:
    :return:
    """

    result_dict = {}
    try:
        for tp in result_tuple:
            if isinstance(tp, dict):
                result_dict = dict(tp.items() + result_dict.items())
    except Exception as e:
        log.error("base.combine_parameters:"+str(Exception)+str(e)+str(traceback.print_exc()))
    return result_dict


@app.task
def send_request(url, data):
    """
    发送请求
    :param url:
    :param data:
    :return:
    """
    try:
        req = requests.post(url, json=json.loads(data))
        if req.status_code == 200:
            log.info(url+"post data success")
        else:
            log.error(url+"["+str(req.status_code)+"]post data failed!")
    except requests.ConnectionError as e:
        log.error(url+"application cgi connection error!")
    except Exception as e:
        log.error(url+"push data error")


# ##################TEXT CLEAN PROCESS################################
@app.task
def clean_on_success(sub_result, data, next_step):
    """
    清洗流程确认步骤
    :param sub_result:
    :param data:
    :param next_step:
    :return:
    """

    try:
        data['clean_content'] = sub_result['content']
        app.send_task(next_step, kwargs=data)
        log.info("base.clean_on_success:["+data['url']+"]send to clean")
    except Exception as e:
        log.error("base.clean_on_success:" + str(Exception) + str(e) + str(traceback.print_exc()))

# TODO:: 接着完善数据的日志
@app.task
def clean_canvas(**data):
    """
    清洗流程
    :param data:
    :return:
    """
    url = data.get('url', None)
    # 去重是否有效
    #  and area_filter(content)
    if url_duplication(url):
        # 数据清洗工作流
        next_step = "base.nlp_canvas"
        chain(
            [
                app.tasks["text.clean.completion_media_links"].s(data['url'], data['content']),
                app.tasks["text.clean.html_remove"].s(),
                app.tasks["text.clean.tag_remove"].s(),
                clean_on_success.s(data, next_step)
            ]
        )()
    else:
        log.error("url {url} exist!".format(url=url))


# ##################NLP PROCESS################################
@app.task(ignore_result=True)
def nlp_on_success(nlp_result, data, next_step, topic):
    """
    nlp挖掘确认
    :param nlp_result:
    :param data:
    :param next_step:
    :param topic:
    :return:
    """
    data['nlp_result'] = nlp_result
    if "T" in data['publish_time']:
        data['publish_time'] = data['publish_time'].replace("T", " ")
    app.send_task(next_step, args=(topic, data))


@app.task(ignore_result=True)
def nlp_canvas(**data):
    """
    nlp挖掘阶段
    :param data:
    :return:
    """
    next_step = "publish.push_to_kafka"
    topic = "news"
    # 2018-04-13 剔除不在省内数据源的 街道办事处 地区识别  nlp_area_detect流程增加source_site_id字段
    a = chord(
        [
            app.tasks["text.nlp.nlp_category"].s(data['clean_content']),
            app.tasks["text.nlp.nlp_entities"].s(data['clean_content']),
            app.tasks["text.nlp.nlp_sentiment"].s(data['clean_content']),
            app.tasks["text.nlp.nlp_segment"].s(data['clean_content']),
            app.tasks["text.nlp.nlp_area_detect"].s(data['source_site_id'], data['clean_content']),
            app.tasks["text.nlp.nlp_related_org"].s(data['clean_content'])
        ],
        combine_parameters.s()
    )
    #app.tasks["text.nlp.nlp_duplicates"].s(data['title'], data['clean_content'], data['url'])
    chain([a, nlp_on_success.s(data, next_step, topic)])()


# #####################IMAGE PROCESS###################################
@app.task(ignore_result=True)
def image_on_success(data, next_step, topic):
    app.send_task(next_step, args=(topic, data))



@app.task(ignore_result=True)
def image_canvas(**data):
    next_step = "publish.push_to_kafka"
    topic = "image"
    chain(app.tasks["image.clean.preprocess"].s(data), image_on_success.s(next_step, topic))


# #####################VIDEO PROCESS###################################
@app.task(ignore_result=True)
def video_on_success(data, next_step, topic):
    app.send_task(next_step, args=(topic, data))


@app.task(ignore_result=True)
def video_canvas(**data):
    next_step = "publish.push_to_kafka"
    topic = "video"
    #chain(app.tasks["video.clean.preprocess"].s(data), video_on_success.s(next_step, topic))()
    app.send_task("video.clean.preprocess", args=(data, ))
