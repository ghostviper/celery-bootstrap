# celery-bootstrap
celery bootstrap &amp; utils

---

### 项目结构

```text
root:
+--tasks
|      +--base.py                 --celery主任务
|      +--canvas_module           --工作流任务模块
|      |      +--task.py          --工作流任务定义
|      |      +--__init__.py
|      +--celery_app.py           --celery app定义和配置
|      +--common_tasks            --子任务模块
|      |      +--task.py          --子任务定义
|      |      +--__init__.py
|      +--__init__.py
|--tests                          --工具函数目录
+--tests                          --测试目录
|      +--print_dir_map.py
+--config.py                      --全局配置
+--README.md
```
