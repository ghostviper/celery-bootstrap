# -*- coding:utf-8 -*-

from __future__ import absolute_import

import celery

__author__ = 'ghostviper'



@celery.task
def task_func1():
    pass


@celery.task
def task_func2():
    pass


@celery.task
def task_func3():
    pass


