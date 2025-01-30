# encoding:utf-8

import json
import os
import time
import logging
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from common.log import logger
from plugins import Plugin
import plugins
from config import conf
from plugins import Event, EventContext, EventAction
import threading
from datetime import datetime, timedelta
from croniter import croniter  # 需要先 pip install croniter
import sqlite3
from typing import Optional, Dict, Any
from bridge.bridge import Bridge
from channel.chat_message import ChatMessage
from bridge.context import Context
from lib.gewechat import GewechatClient
from plugins.plugin_manager import PluginManager
import re
import requests
import queue

# 常量定义
MAX_RETRIES = 3
RETRY_DELAY = 1  # 初始重试延迟(秒)
API_TIMEOUT = 10
CONTACTS_CACHE_TIMEOUT = 600  # 联系人缓存超时时间(秒)
TASK_CAPACITY_DEFAULT = 100
TIME_ADJUST_INTERVAL_DEFAULT = 1
TASK_LIST_PASSWORD_DEFAULT = "123456"
DB_CONNECTION_TIMEOUT = 5    # 数据库连接超时时间(秒)
DB_POOL_SIZE = 5            # 数据库连接池大小
CONFIG_CHECK_INTERVAL = 300  # 配置文件检查间隔(秒)

# 错误消息
ERROR_MESSAGES = {
    "TASK_NOT_FOUND": "任务不存在: {}",
    "TASK_CREATE_FAILED": "创建任务失败: {}",
    "TASK_DELETE_FAILED": "删除任务失败: {}",
    "API_ERROR": "API调用失败: {}",
    "DB_ERROR": "数据库操作失败: {}",
    "INVALID_TIME_FORMAT": "无效的时间格式: {}",
    "INVALID_CYCLE_FORMAT": "无效的周期格式: {}",
    "GROUP_NOT_FOUND": "未找到群组: {}"
}

class GeweChatTaskError(Exception):
    """GeweChatTask插件的自定义异常类"""
    pass

@plugins.register(
    name="GeweChatTask",
    desire_priority=986,
    hidden=False,
    desc="基于gewechat和dify-on-wechat的定时任务插件，自定义扩展各类功能的集成基座",
    version="1.0.0",
    author="golovin",
)
class GeweChatTask(Plugin):
    def __init__(self):
        # 清理Python缓存文件
        try:
            # 清理插件目录的缓存
            plugin_dir = os.path.dirname(__file__)
            dirs_to_clean = [
                plugin_dir,
                os.path.join(plugin_dir, 'lib')  # 添加lib目录
            ]

            for base_dir in dirs_to_clean:
                for root, dirs, files in os.walk(base_dir):
                    # 删除__pycache__目录
                    if '__pycache__' in dirs:
                        pycache_path = os.path.join(root, '__pycache__')
                        try:
                            import shutil
                            shutil.rmtree(pycache_path)
                            logger.info(f"[GeweChatTask] 已清理缓存目录: {pycache_path}")
                        except Exception as e:
                            logger.error(f"[GeweChatTask] 清理缓存目录失败: {pycache_path}, 错误: {e}")

                    # 删除.pyc文件
                    for file in files:
                        if file.endswith('.pyc'):
                            pyc_path = os.path.join(root, file)
                            try:
                                os.remove(pyc_path)
                                logger.info(f"[GeweChatTask] 已删除缓存文件: {pyc_path}")
                            except Exception as e:
                                logger.error(f"[GeweChatTask] 删除缓存文件失败: {pyc_path}, 错误: {e}")
        except Exception as e:
            logger.error(f"[GeweChatTask] 清理缓存失败: {e}")

        super(Plugin, self).__init__()  # 修改这里，明确指定父类
        self.command_prefix = "$time"  # 添加命令前缀
        self._instance = None
        self._initialized = False
        self._scheduler = None
        self._scheduler_lock = threading.Lock()
        self._running = False
        self._message_lock = threading.Lock()
        self._task_locks = {}
        self._thread_local = threading.local()
        self._config_last_modified = 0
        self.contacts_cache = {
            "last_update": 0,
            "contacts": []
        }
        
        try:
            # 初始化 handlers
            self.handlers = {}
            self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
            
            # 初始化数据目录和数据库
            self._init_data_directory()
            self._init_db()
            
            # 初始化数据库连接池
            self._init_db_pool()
            
            # 初始化客户端和配置
            self._init_client()
            
            # 加载插件配置
            self._load_plugin_config()
            
            # 更新群组信息
            self._update_groups()
            
            # 初始化定时器
            self._init_scheduler()
            
            logger.info("[GeweChatTask] plugin initialized")
            self._initialized = True
        except Exception as e:
            logger.error(f"[GeweChatTask] 初始化失败: {e}")
            raise GeweChatTaskError(f"初始化失败: {e}")

    def _init_data_directory(self):
        """初始化数据目录"""
        try:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
            self.db_path = os.path.join(self.data_dir, "tasks.db")
        except Exception as e:
            logger.error(f"[GeweChatTask] 初始化数据目录失败: {e}")
            raise GeweChatTaskError(f"初始化数据目录失败: {e}")

    def _init_db(self):
        """初始化数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建任务表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    time TEXT NOT NULL,
                    circle TEXT NOT NULL,
                    cron TEXT NOT NULL,
                    event TEXT NOT NULL,
                    context TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_executed_at INTEGER DEFAULT 0,
                    executing_at INTEGER DEFAULT 0
                )
            ''')
            
            # 创建群组表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    wxid TEXT PRIMARY KEY,
                    nickname TEXT NOT NULL,
                    owner_wxid TEXT,
                    member_count INTEGER DEFAULT 0,
                    updated_at INTEGER NOT NULL
                )
            ''')
            
            # 创建联系人表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
                    wxid TEXT PRIMARY KEY,
                    nickname TEXT NOT NULL,
                    remark TEXT,
                    updated_at INTEGER NOT NULL
                )
            ''')
            
            # 检查executing_at列是否存在
            cursor.execute("PRAGMA table_info(tasks)")
            columns = cursor.fetchall()
            if not any(col[1] == 'executing_at' for col in columns):
                cursor.execute('ALTER TABLE tasks ADD COLUMN executing_at INTEGER DEFAULT 0')
            
            conn.commit()
            conn.close()
            logger.info("[GeweChatTask] 数据库初始化完成")
        except Exception as e:
            logger.error(f"[GeweChatTask] 初始化数据库失败: {e}")
            raise GeweChatTaskError(f"初始化数据库失败: {e}")

    def _init_client(self):
        """初始化客户端"""
        try:
            base_url = conf().get("gewechat_base_url")
            token = conf().get("gewechat_token")
            self.app_id = conf().get("gewechat_app_id")
            
            if not base_url or not token or not self.app_id:
                raise GeweChatTaskError("缺少必要的配置: gewechat_base_url, gewechat_token, gewechat_app_id")
            
            self.device_id = self._get_online_device_id()
            logger.info(f"[GeweChatTask] 使用设备 ID: {self.device_id}")
            
            # 1. 测试获取联系人列表
            try:
                contacts_response = self._fetch_contacts_list(self.app_id)
                if contacts_response and contacts_response.get('ret') == 200:
                    logger.info("[GeweChatTask] 获取联系人列表测试成功")
                    # 如果获取联系人成功，使用第一个联系人作为测试对象
                    contacts = contacts_response.get('data', {}).get('friends', [])
                    if contacts:
                        test_wxid = contacts[0]
                        response = self._post_text(
                            self.app_id,
                            test_wxid,
                            "GeweChatTask 插件初始化成功"
                        )
                        if response and response.get('ret') == 200:
                            logger.info("[GeweChatTask] 发送消息测试成功")
                        else:
                            logger.warning(f"[GeweChatTask] 发送消息测试失败: {response}")
                    else:
                        logger.warning("[GeweChatTask] 未找到可用的联系人进行测试")
                else:
                    logger.warning(f"[GeweChatTask] 获取联系人列表测试失败: {contacts_response}")
            except Exception as e:
                logger.warning(f"[GeweChatTask] 初始化测试失败: {e}")
                
        except Exception as e:
            logger.error(f"[GeweChatTask] 初始化客户端失败: {e}")
            raise GeweChatTaskError(f"初始化客户端失败: {e}")

    def _get_online_device_id(self):
        """获取在线设备ID"""
        try:
            # 先尝试使用 app_id 作为设备ID进行验证
            if self._verify_device_online(self.app_id):
                logger.info(f"[GeweChatTask] 使用 app_id 作为设备 ID: {self.app_id}")
                return self.app_id

            # 如果 app_id 不在线，尝试获取设备列表
            response = self._get_self_info(self.app_id)
            
            if response and response.get('ret') == 200:
                data = response.get('data', {})
                if data.get('list'):
                    devices = sorted(data['list'], 
                                  key=lambda x: x.get('lastTime', 0), 
                                  reverse=True)
                    
                    for device in devices:
                        device_id = device.get('uuid')
                        if device_id and self._verify_device_online(device_id):
                            logger.info(f"[GeweChatTask] 找到在线设备 - ID: {device_id}, "
                                      f"名称: {device.get('deviceName')}, "
                                      f"类型: {device.get('deviceType')}, "
                                      f"最后登录: {datetime.fromtimestamp(device.get('lastTime', 0)).strftime('%Y-%m-%d %H:%M:%S')}")
                            return device_id
            
            # 如果没有找到在线设备，使用 app_id
            logger.debug("[GeweChatTask] 未找到其他在线设备，使用 app_id")
            return self.app_id
            
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取设备列表失败: {e}")
            return self.app_id

    def _verify_device_online(self, device_id):
        """验证设备是否在线"""
        try:
            if not device_id:
                return False

            data = {
                "appId": self.app_id,
                "uuid": device_id
            }
            response = self._make_request("POST", "personal/getSafetyInfo", data, timeout=API_TIMEOUT)
            
            if response:
                is_online = response.get('ret') == 200 and response.get('data', {}).get('online')
                if is_online:
                    logger.debug(f"[GeweChatTask] 设备 {device_id} 在线")
                else:
                    logger.debug(f"[GeweChatTask] 设备 {device_id} 离线")
                return is_online

            logger.info(f"[GeweChatTask] 获取设备状态失败: HTTP {response.status_code}")
            return False
            
        except Exception as e:
            logger.debug(f"[GeweChatTask] 验证设备状态失败: {e}")
            return False

    def _load_plugin_config(self):
        """加载插件配置"""
        try:
            config_path = os.path.join(os.path.dirname(__file__), "config.json")
            self.plugin_config = {}
            
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    self.plugin_config = json.load(f)
            
            # 设置默认配置
            self.plugin_config.setdefault("task_list_password", TASK_LIST_PASSWORD_DEFAULT)
            self.plugin_config.setdefault("task_capacity", TASK_CAPACITY_DEFAULT)
            self.plugin_config.setdefault("time_adjust_interval", TIME_ADJUST_INTERVAL_DEFAULT)
            
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(self.plugin_config, f, indent=4)
                
        except Exception as e:
            logger.error(f"[GeweChatTask] 加载配置失败: {e}")
            raise GeweChatTaskError(f"加载配置失败: {e}")

    def _init_scheduler(self):
        """初始化定时器"""
        try:
            with self._scheduler_lock:
                if self._scheduler and self._scheduler.is_alive():
                    self._running = False
                    self._scheduler.join(timeout=1)
                
                self._running = True
                self._scheduler = threading.Thread(target=self._timer_loop)
                self._scheduler.daemon = True
                self._scheduler.start()
                
        except Exception as e:
            logger.error(f"[GeweChatTask] 初始化定时器失败: {e}")
            raise GeweChatTaskError(f"初始化定时器失败: {e}")

    def reload(self):
        """重载时停止旧线程，返回 (success, message)"""
        try:
            with self._scheduler_lock:
                # 停止旧线程
                self._running = False
                if self._scheduler and self._scheduler.is_alive():
                    try:
                        self._scheduler.join(timeout=30)
                        if self._scheduler.is_alive():
                            return False, "Failed to stop timer thread"
                    except Exception as e:
                        return False, f"Error stopping timer thread: {e}"
                
                # 重新初始化 handlers
                self.handlers = {}
                self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
                
                # 重新初始化线程
                self._running = True
                self._scheduler = threading.Thread(target=self._timer_loop)
                self._scheduler.daemon = True
                self._scheduler.start()
                
                logger.info("[GeweChatTask] Plugin reloaded successfully")
                return True, "Timer thread restarted successfully"
        except Exception as e:
            logger.error(f"[GeweChatTask] Reload failed: {e}")
            return False, f"Error reloading plugin: {e}"

    def __del__(self):
        """析构函数，确保线程正确退出"""
        if hasattr(self, 'running'):
            self.running = False
        if hasattr(self, 'timer_thread') and self.timer_thread and self.timer_thread.is_alive():
            self.timer_thread.join(timeout=1)
            logger.info("[GeweChatTask] timer thread stopped")

    def _timer_loop(self):
        """定时器循环（添加配置检查）"""
        last_group_update = 0
        last_check_time = 0
        last_config_check = 0
        last_lock_clean = 0

        while self._running:
            try:
                now = datetime.now()
                current_time = int(time.time())

                # 避免在同一秒内多次检查任务
                if current_time == last_check_time:
                    time.sleep(0.1)
                    continue

                last_check_time = current_time

                # 检查配置文件更新
                if current_time - last_config_check >= CONFIG_CHECK_INTERVAL:
                    self._check_config_update()
                    last_config_check = current_time

                # 清理过期的任务锁
                if current_time - last_lock_clean >= 3600:  # 每小时清理一次
                    self._clean_task_locks()
                    last_lock_clean = current_time

                # 每天凌晨3点清理过期任务
                if now.hour == 3 and now.minute == 0:
                    self._clean_expired_tasks()
                
                # 每小时更新一次群组信息
                if current_time - last_group_update >= 3600:
                    self._update_groups()
                    last_group_update = current_time

                # 从数据库获取所有任务
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                try:
                    # 使用 IMMEDIATE 事务级别确保并发安全
                    cursor.execute('BEGIN IMMEDIATE')
                    
                    # 获取需要执行的任务
                    cursor.execute(
                        'SELECT id, cron, context, last_executed_at FROM tasks WHERE last_executed_at < ? AND (executing_at = 0 OR executing_at < ?)',
                        (current_time - 60, current_time - 30)  # 30秒超时保护
                    )
                    
                    tasks = cursor.fetchall()
                    tasks_to_execute = []
                    
                    for task_id, cron_exp, context_json, last_executed_at in tasks:
                        try:
                            cron = croniter(cron_exp, now)
                            prev_time = cron.get_prev(datetime)
                            time_diff = (now - prev_time).total_seconds()
                            
                            # 检查是否在执行时间窗口内
                            if 0 <= time_diff < 60:
                                # 立即尝试更新执行状态，使用 IMMEDIATE 事务确保原子性
                                cursor.execute('''
                                    UPDATE tasks 
                                    SET executing_at = ?,
                                        last_executed_at = ?
                                    WHERE id = ? 
                                    AND (executing_at = 0 OR executing_at < ?)
                                    AND last_executed_at < ?
                                ''', (current_time, current_time, task_id, current_time - 30, current_time - 60))
                                
                                # 只有成功更新状态的任务才会被执行
                                if cursor.rowcount > 0:
                                    tasks_to_execute.append((task_id, context_json))
                                    logger.info(f"[GeweChatTask] 任务 {task_id} 标记为执行中")
                        
                        except Exception as e:
                            logger.error(f"[GeweChatTask] 检查任务异常: {task_id} {str(e)}")
                            continue
                    
                    # 提交状态更新
                    conn.commit()
                    
                    # 执行任务
                    for task_id, context_json in tasks_to_execute:
                        try:
                            context_info = json.loads(context_json)
                            self._execute_task(task_id, context_info)
                            
                            # 任务执行完成后，开始新事务更新状态
                            cursor.execute('BEGIN IMMEDIATE')
                            
                            # 检查是否是一次性任务并处理
                            cursor.execute('SELECT circle FROM tasks WHERE id = ?', (task_id,))
                            result = cursor.fetchone()
                            
                            if result and (len(result[0]) == 10 or result[0] in ["今天", "明天", "后天"]):
                                cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
                                logger.info(f"[GeweChatTask] 已删除一次性任务: {task_id}")
                            else:
                                cursor.execute('UPDATE tasks SET executing_at = 0 WHERE id = ?', (task_id,))
                            
                            conn.commit()
                            
                        except Exception as e:
                            logger.error(f"[GeweChatTask] 执行任务异常: {task_id} {str(e)}")
                            # 发生错误时也要清除执行状态
                            try:
                                cursor.execute('BEGIN IMMEDIATE')
                                cursor.execute('UPDATE tasks SET executing_at = 0 WHERE id = ?', (task_id,))
                                conn.commit()
                            except Exception as e2:
                                logger.error(f"[GeweChatTask] 清除执行状态失败: {task_id} {str(e2)}")
                    
                except Exception as e:
                    # 如果发生异常，回滚事务
                    conn.rollback()
                    logger.error(f"[GeweChatTask] 数据库操作异常: {str(e)}")
                
                finally:
                    # 确保关闭连接
                    conn.close()

                # 优化休眠时间
                next_second = (now + timedelta(seconds=1)).replace(microsecond=0)
                sleep_time = (next_second - now).total_seconds()
                time.sleep(max(0.1, sleep_time))

            except Exception as e:
                logger.error(f"[GeweChatTask] 定时器异常: {e}")
                time.sleep(60)

    def _update_groups(self):
        """更新群组和联系人信息"""
        try:
            # 检查设备 ID
            if not self.device_id:
                logger.error("[GeweChatTask] 无法更新群组信息：设备 ID 未设置")
                return
                
            # 获取所有群聊和联系人列表
            response = self._fetch_contacts_list(self.app_id)
            logger.debug(f"[GeweChatTask] fetch_contacts_list response: {response}")

            if response.get('ret') == 200:
                current_time = int(time.time())
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # 处理群组信息
                data = response.get('data')
                if not isinstance(data, dict):
                    logger.error(f"[GeweChatTask] 响应数据格式错误，期望字典类型，实际类型: {type(data)}")
                    data = {}
                
                chatrooms = data.get('chatrooms', [])
                if not isinstance(chatrooms, list):
                    logger.error(f"[GeweChatTask] 群组数据格式错误，期望列表类型，实际类型: {type(chatrooms)}")
                    chatrooms = []
                
                logger.info(f"[GeweChatTask] Total chatrooms found: {len(chatrooms)}")
                for chatroom_id in chatrooms:
                    try:
                        # 获取群信息
                        group_info = self._get_chatroom_info(self.device_id, chatroom_id)
                        logger.debug(f"[GeweChatTask] Group info for {chatroom_id}: {group_info}")
                        
                        if group_info.get('ret') == 200:
                            data = group_info.get('data', {})
                            nickname = data.get('nickName', '')
                            
                            # 如果昵称为空，使用群ID作为昵称
                            if not nickname:
                                nickname = f"群聊_{chatroom_id}"
                                logger.warning(f"[GeweChatTask] Group {chatroom_id} has no nickname, using default name")
                            
                            # 更新数据库
                            cursor.execute('''
                                INSERT OR REPLACE INTO groups (wxid, nickname, updated_at)
                                VALUES (?, ?, ?)
                            ''', (chatroom_id, nickname, current_time))
                            
                            logger.debug(f"[GeweChatTask] Updated group info: {chatroom_id} - {nickname}")
                        elif group_info.get('ret') == 500 and "设备已离线" in str(group_info.get('msg', '')):
                            # 如果是设备离线错误，尝试使用缓存的群信息
                            cursor.execute('SELECT nickname FROM groups WHERE wxid = ?', (chatroom_id,))
                            result = cursor.fetchone()
                            if result:
                                logger.debug(f"[GeweChatTask] 设备离线，使用缓存的群信息: {chatroom_id} - {result[0]}")
                                # 更新时间戳以避免被清理
                                cursor.execute('UPDATE groups SET updated_at = ? WHERE wxid = ?', 
                                             (current_time, chatroom_id))
                            else:
                                logger.debug(f"[GeweChatTask] 设备离线且无缓存信息: {chatroom_id}")
                        else:
                            # API 调用成功但返回错误
                            logger.warning(f"[GeweChatTask] Failed to get group info for {chatroom_id}, ret: {group_info.get('ret')}")
                            # 使用默认名称
                            cursor.execute('''
                                INSERT OR REPLACE INTO groups (wxid, nickname, updated_at)
                                VALUES (?, ?, ?)
                            ''', (chatroom_id, f"群聊_{chatroom_id}", current_time))
                            
                    except Exception as e:
                        logger.debug(f"[GeweChatTask] Failed to update group {chatroom_id}: {e}")
                        # 尝试使用缓存的群信息
                        cursor.execute('SELECT nickname FROM groups WHERE wxid = ?', (chatroom_id,))
                        result = cursor.fetchone()
                        if result:
                            logger.debug(f"[GeweChatTask] 使用缓存的群信息: {chatroom_id} - {result[0]}")
                            # 更新时间戳以避免被清理
                            cursor.execute('UPDATE groups SET updated_at = ? WHERE wxid = ?', 
                                         (current_time, chatroom_id))
                            logger.error(f"[GeweChatTask] Exception while updating group {chatroom_id}: {e}")
                        else:
                            # 发生异常时也使用默认名称
                            try:
                                cursor.execute('''
                                    INSERT OR REPLACE INTO groups (wxid, nickname, updated_at)
                                    VALUES (?, ?, ?)
                                ''', (chatroom_id, f"群聊_{chatroom_id}", current_time))
                            except Exception as db_e:
                                logger.error(f"[GeweChatTask] Failed to insert default group name: {db_e}")
                        continue
                
                # 处理联系人信息
                friends = data.get('friends', [])
                if not isinstance(friends, list):
                    logger.error(f"[GeweChatTask] 联系人数据格式错误，期望列表类型，实际类型: {type(friends)}")
                    friends = []
                
                logger.info(f"[GeweChatTask] Total friends found: {len(friends)}")
                if friends:
                    try:
                        # 批量获取联系人信息
                        friend_info_response = self._get_contact_brief_info(friends)
                        
                        if friend_info_response and friend_info_response.get('ret') == 200:
                            data = friend_info_response.get('data', [])
                            if isinstance(data, list):
                                for contact in data:
                                    wxid = contact.get('userName')
                                    nickname = contact.get('nickName', '')
                                    if not nickname:
                                        nickname = f"用户_{wxid}"
                                        logger.warning(f"[GeweChatTask] Contact {wxid} has no nickname, using default name")
                                    if wxid:
                                        cursor.execute('''
                                            INSERT OR REPLACE INTO contacts (wxid, nickname, updated_at)
                                            VALUES (?, ?, ?)
                                        ''', (wxid, nickname, current_time))
                            else:
                                logger.error(f"[GeweChatTask] Failed to get contacts info: {data}")
                        else:
                            logger.error(f"[GeweChatTask] Failed to get contacts info, status: {friend_info_response.get('ret')}")
                    except Exception as e:
                        logger.error(f"[GeweChatTask] Failed to update contacts: {e}")
                
                # 清理过期数据
                week_ago = current_time - 7 * 24 * 3600
                cursor.execute('DELETE FROM groups WHERE updated_at < ?', (week_ago,))
                cursor.execute('DELETE FROM contacts WHERE updated_at < ?', (week_ago,))
                
                conn.commit()
                conn.close()
                logger.info("[GeweChatTask] Groups and contacts info updated")
            else:
                logger.debug(f"[GeweChatTask] Failed to fetch contacts list: {response}")
                
        except Exception as e:
            logger.debug(f"[GeweChatTask] Failed to update groups and contacts: {e}")
            # 异常不影响主要功能的执行

    def get_help_text(self, **kwargs):
        return f"""定时任务插件使用说明:
命令前缀: {self.command_prefix}

1. 创建定时任务
基础格式：
{self.command_prefix} 周期 时间 [u[用户]] 事件内容

支持的周期格式：
- 每天 09:30 早安
- 工作日 18:00 下班提醒
- 每周一 10:00 周报时间
- 今天 23:30 睡觉提醒
- 明天 12:00 午饭提醒
- 后天 09:00 周末快乐
- 2024-01-01 12:00 新年快乐

用户提醒格式：
{self.command_prefix} 今天 13:16 u[张三] 早报
{self.command_prefix} 每天 09:30 u[李四] 喝水提醒

群聊任务格式：
{self.command_prefix} 每天 09:30 g[测试群]早安

插件任务格式：
- 老黄历
{self.command_prefix} 每天 08:00 p[老黄历]  # 每天早上查看老黄历
{self.command_prefix} 每天 09:30 g[测试群] p[老黄历]  # 每天早上给测试群推送老黄历
{self.command_prefix} 工作日 09:30 u[张三] p[老黄历]  # 工作日给张三推送老黄历

- 星座运势
{self.command_prefix} 每天 08:00 p[星座-巨蟹座]  # 每天早上查看巨蟹座运势
{self.command_prefix} 每天 09:30 g[测试群] p[星座-白羊座]  # 每天早上给测试群推送白羊座运势
{self.command_prefix} 工作日 09:30 u[张三] p[星座-天秤座]  # 工作日给张三推送天秤座运势

支持的星座：白羊座、金牛座、双子座、巨蟹座、狮子座、处女座、天秤座、天蝎座、射手座、魔羯座、水瓶座、双鱼座

- API余额查询
{self.command_prefix} 今天 10:00 p[余额查询]  # 查询当前API余额
{self.command_prefix} 每周一 09:00 u[张三] p[余额查询]  # 每周一给张三推送API余额信息

Cron表达式格式（高级）：
{self.command_prefix} cron[分 时 日 月 周] 事件内容
例如：
{self.command_prefix} cron[0 9 * * 1-5] 该起床了
{self.command_prefix} cron[*/30 * * * *] 喝水提醒
{self.command_prefix} cron[0 */2 * * *] 休息一下

2. 查看任务列表
{self.command_prefix} 任务列表 密码  # 需要提供管理员密码才能查看任务列表

3. 取消任务
支持以下几种方式：
- 取消指定任务：{self.command_prefix} 取消 任务ID  # 支持多个任务ID，可用逗号、空格、顿号分隔，如：TYZE,XL52 或 TYZE、XL52
- 取消用户任务：{self.command_prefix} 取消 u[用户名]  # 删除指定用户的所有任务
- 取消群聊任务：{self.command_prefix} 取消 g[群名]  # 删除指定群的所有任务
- 取消所有任务：{self.command_prefix} 取消 all 密码  # 需要提供管理员密码才能删除所有任务

注意：查看任务列表和删除所有任务都需要提供管理员密码，密码必须与配置文件中的task_list_password匹配。"""

    def _get_contact_brief_info(self, wxids: list) -> Optional[Dict[str, Any]]:
        """
        获取联系人简要信息
        
        Args:
            wxids: 微信ID列表,最多100个
            
        Returns:
            Dict[str, Any]: 包含联系人信息的响应数据,失败返回None
            
        API文档:
            - 请求方式: POST
            - 接口: contacts/getBriefInfo
            - 参数: 
                - appId: 设备ID
                - wxids: 好友的wxid列表
        """
        try:
            if not wxids:
                logger.error("[GeweChatTask] wxids不能为空")
                return None
                
            if len(wxids) > 100:
                logger.error("[GeweChatTask] wxids数量超过100个限制")
                return None
                
            data = {
                "appId": self.app_id,
                "wxids": wxids
            }
            
            response = self._make_request("POST", "contacts/getBriefInfo", data)
            logger.debug(f"[GeweChatTask] getBriefInfo raw response: {response}")
            
            # 直接返回原始响应结构
            return response
            
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取联系人简要信息失败: {e}")
            return None

    def _get_user_nickname(self, user_id: str) -> str:
        """
        获取用户昵称
        
        Args:
            user_id: 用户ID
            
        Returns:
            str: 用户昵称，如果获取失败则返回用户ID
        """
        try:
            response = self._get_contact_brief_info([user_id])
            if response and response.get('ret') == 200:
                data = response.get('data', [])
                if isinstance(data, list) and len(data) > 0:
                    return data[0].get('nickName', user_id)
            return user_id
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取用户昵称失败: {e}")
            return user_id

    def _set_user_nickname(self, chat_msg):
        """
        设置用户昵称
        
        Args:
            chat_msg: 聊天消息对象
        """
        try:
            response = self._get_contact_brief_info([chat_msg.from_user_id])
            if response and response.get('ret') == 200:
                data = response.get('data', [])
                if isinstance(data, list) and len(data) > 0:
                    chat_msg.other_user_nickname = data[0].get('nickName', chat_msg.from_user_id)
                    chat_msg.actual_user_nickname = data[0].get('nickName', chat_msg.from_user_id)
                else:
                    chat_msg.other_user_nickname = chat_msg.from_user_id
                    chat_msg.actual_user_nickname = chat_msg.from_user_id
            else:
                chat_msg.other_user_nickname = chat_msg.from_user_id
                chat_msg.actual_user_nickname = chat_msg.from_user_id
        except Exception as e:
            logger.error(f"[GeweChatTask] 设置用户昵称失败: {e}")
            chat_msg.other_user_nickname = chat_msg.from_user_id
            chat_msg.actual_user_nickname = chat_msg.from_user_id

    def _make_request(self, method: str, endpoint: str, data: Dict[str, Any] = None, timeout: int = 10) -> Optional[Dict[str, Any]]:
        """
        发送 HTTP 请求
        
        Args:
            method: HTTP 方法 (GET, POST 等)
            endpoint: API 端点
            data: 请求数据
            timeout: 超时时间(秒)
            
        Returns:
            Dict[str, Any]: API 响应数据
        """
        try:
            base_url = conf().get("gewechat_base_url").rstrip('/')
            token = conf().get("gewechat_token")
            headers = {
                "X-GEWE-TOKEN": token,
                "Content-Type": "application/json"
            }
            
            # 移除endpoint开头和结尾的斜杠
            endpoint = endpoint.strip('/')
            
            # 直接使用endpoint，不添加v2/api前缀
            url = f"{base_url}/{endpoint}"
                
            logger.debug(f"[GeweChatTask] Making request to: {url}")
            
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                timeout=timeout
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"API 请求失败: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"API 请求异常: {str(e)}")
            return None

    def _fetch_contacts_list(self, app_id: str) -> Dict[str, Any]:
        """
        获取联系人列表
        
        Args:
            app_id: 应用 ID
            
        Returns:
            Dict[str, Any]: 包含联系人列表的响应数据
        """
        data = {"appId": app_id}
        response = self._make_request("POST", "contacts/fetchContactsList", data)
        return response or {"ret": 500, "msg": "获取联系人列表失败"}
    
    def _fetch_contacts_list_cache(self, app_id: str) -> Dict[str, Any]:
        """
        获取联系人列表缓存
        通讯录列表数据缓存10分钟，超时则需要重新调用获取通讯录列表接口
        
        Args:
            app_id: 应用 ID
            
        Returns:
            Dict[str, Any]: 包含联系人列表的响应数据
        """
        data = {"appId": app_id}
        response = self._make_request("POST", "contacts/fetchContactsListCache", data)
        return response or {"ret": 500, "msg": "获取联系人列表缓存失败"}

    def _get_chatroom_info(self, device_id: str, chatroom_id: str) -> Dict[str, Any]:
        """
        获取群聊信息
        
        Args:
            device_id: 设备 ID
            chatroom_id: 群聊 ID
            
        Returns:
            Dict[str, Any]: 包含群聊信息的响应数据
        """
        data = {
            "appId": device_id,
            "chatroomId": chatroom_id
        }
        response = self._make_request("POST", "group/getChatroomInfo", data)
        return response or {"ret": 500, "msg": "获取群聊信息失败"}

    def _post_text(self, app_id: str, to_wxid: str, content: str) -> Dict[str, Any]:
        """
        发送文本消息
        
        Args:
            app_id: 应用 ID
            to_wxid: 接收者 ID
            content: 消息内容
            
        Returns:
            Dict[str, Any]: API 响应数据
        """
        data = {
            "appId": app_id,
            "toWxid": to_wxid,
            "content": content
        }
        response = self._make_request("POST", "message/postText", data)
        return response or {"ret": 500, "msg": "发送消息失败"}

    def _get_self_info(self, app_id: str) -> Dict[str, Any]:
        """
        获取个人资料信息
        
        Args:
            app_id: 应用 ID
            
        Returns:
            Dict[str, Any]: 包含个人资料的响应数据
        """
        data = {"appId": app_id}
        response = self._make_request("POST", "personal/getProfile", data)
        return response or {"ret": 500, "msg": "获取个人资料失败"}

    def _get_task_list(self):
        """获取任务列表"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有任务，并关联群组信息
            cursor.execute('''
            SELECT 
                t.id, 
                t.time, 
                t.circle, 
                t.event, 
                t.context,
                g.nickname
            FROM tasks t
            LEFT JOIN groups g ON g.wxid = json_extract(t.context, '$.msg.from_user_id')
            ORDER BY json_extract(t.context, '$.isgroup') DESC,  -- 先按是否群消息排序
                     COALESCE(g.nickname, json_extract(t.context, '$.msg.from_user_id'))  -- 再按群名/用户名排序
            ''')
            tasks = cursor.fetchall()
            
            conn.close()
            
            if not tasks:
                return "当前没有任务"
            
            # 按群/用户分组整理任务
            grouped_tasks = {}
            for task_id, time, circle, event, context_str, group_name in tasks:
                try:
                    # 解析上下文信息
                    context = json.loads(context_str)
                    msg_info = context.get('msg', {})
                    is_group = context.get('isgroup', False)
                    
                    # 获取显示名称
                    if is_group:
                        display_name = f"群：{group_name or '未知群组'}"
                    else:
                        user_id = msg_info.get('from_user_id', '')
                        nickname = self._get_user_nickname(user_id)
                        display_name = f"用户：{nickname}"
                    
                    # 添加到分组中
                    if display_name not in grouped_tasks:
                        grouped_tasks[display_name] = []
                    
                    # 添加任务信息
                    grouped_tasks[display_name].append({
                        'id': task_id,
                        'time': time,
                        'circle': circle,
                        'event': event
                    })
                    
                except Exception as e:
                    logger.error(f"[GeweChatTask] 解析任务信息失败: {e}")
                    continue
            
            # 生成显示文本
            result = "任务列表:\n"
            for group_name, tasks in grouped_tasks.items():
                result += f"\n{group_name}\n"
                result += "-" * 30 + "\n"
                for task in tasks:
                    result += f"[{task['id']}] {task['circle']} {task['time']} {task['event']}\n"
                result += "\n"
            
            return result.strip()
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取任务列表失败: {e}")
            return "获取任务列表失败"

    def _validate_time_format(self, time_str):
        """验证时间格式 HH:mm"""
        try:
            # 检查格式
            if ':' not in time_str and '：' not in time_str:
                return False, "时间格式错误，请使用 HH:mm 格式"
            
            # 统一处理中文冒号
            time_str = time_str.replace('：', ':')
            
            # 解析时间
            hour, minute = time_str.split(':')
            hour = int(hour)
            minute = int(minute)
            
            # 验证范围
            if hour < 0 or hour > 23:
                return False, "小时必须在0-23之间"
            if minute < 0 or minute > 59:
                return False, "分钟必须在0-59之间"
            
            # 检查时间是否过期
            now = datetime.now()
            if hour < now.hour or (hour == now.hour and minute < now.minute):
                # 仅当指定"今天"时才提示过期
                return True, "today_expired"
            
            logger.debug("[GeweChatTask] 时间格式验证通过")
            return True, None
            
        except ValueError:
            return False, "时间格式错误，请使用 HH:mm 格式，例如：09:30"
        except Exception as e:
            logger.error(f"[GeweChatTask] 时间格式验证失败: {e}")
            return False, "时间格式验证失败"

    def _validate_circle_format(self, circle_str):
        """验证周期格式"""
        try:
            logger.debug(f"[GeweChatTask] 验证周期格式: {circle_str}")
            
            # 每天
            if circle_str == "每天":
                return True
                
            # 工作日
            if circle_str == "工作日":
                return True
                
            # 每周几
            week_days = ["一", "二", "三", "四", "五", "六", "日"]
            if circle_str.startswith("每周"):
                day = circle_str[2:]
                if day in week_days:
                    return True
                return False
                
            # 今天、明天、后天
            if circle_str in ["今天", "明天", "后天"]:
                return True
                
            # 具体日期 YYYY-MM-DD
            if len(circle_str) == 10:
                try:
                    year = int(circle_str[0:4])
                    month = int(circle_str[5:7])
                    day = int(circle_str[8:10])
                    if circle_str[4] != '-' or circle_str[7] != '-':
                        return False
                    if year < 2024 or year > 2100:
                        return False
                    if month < 1 or month > 12:
                        return False
                    if day < 1 or day > 31:
                        return False
                    return True
                except:
                    return False
                    
            # cron表达式
            if circle_str.startswith("cron[") and circle_str.endswith("]"):
                cron_exp = circle_str[5:-1].strip()  # 移除前后空格
                try:
                    # 检查格式：必须是5个部分
                    parts = cron_exp.split()
                    if len(parts) != 5:
                        logger.debug(f"[GeweChatTask] cron表达式格式错误，应该有5个部分: {cron_exp}")
                        return False
                    croniter(cron_exp)
                    return True
                except Exception as e:
                    logger.debug(f"[GeweChatTask] cron表达式验证失败: {e}")
                    return False
                    
            return False
        except Exception as e:
            logger.error(f"[GeweChatTask] 周期格式验证异常: {e}")
            return False

    def _convert_to_cron(self, circle_str, time_str):
        """转换为cron表达式"""
        try:
            # 如果已经是cron表达式，直接返回表达式内容
            if circle_str.startswith("cron[") and circle_str.endswith("]"):
                return circle_str[5:-1].strip()
            
            # 处理普通时间格式
            hour, minute = time_str.split(':')
            
            if circle_str == "每天":
                return f"{minute} {hour} * * *"
                
            if circle_str == "工作日":
                return f"{minute} {hour} * * 1-5"
                
            if circle_str.startswith("每周"):
                week_map = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5", "六": "6", "日": "0"}
                day = circle_str[2:]
                if day in week_map:
                    return f"{minute} {hour} * * {week_map[day]}"
                    
            # 处理今天、明天、后天
            if circle_str in ["今天", "明天", "后天"]:
                today = datetime.now()
                days_map = {"今天": 0, "明天": 1, "后天": 2}
                target_date = today + timedelta(days=days_map[circle_str])
                return f"{minute} {hour} {target_date.day} {target_date.month} *"
                
            if len(circle_str) == 10:  # YYYY-MM-DD
                date = datetime.strptime(circle_str, "%Y-%m-%d")
                return f"{minute} {hour} {date.day} {date.month} *"
                
            return None
        except Exception as e:
            logger.error(f"[GeweChatTask] 转换cron表达式失败: {e}")
            return None

    def _get_last_task_id(self):
        """获取数据库中最后一个数字类型的任务ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # 简单地获取所有ID，在Python中过滤
            cursor.execute('SELECT id FROM tasks')
            ids = cursor.fetchall()
            conn.close()
            
            # 过滤出纯数字ID并找出最大值
            numeric_ids = [int(id[0]) for id in ids if id[0].isdigit()]
            return max(numeric_ids) if numeric_ids else 1000
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取最后任务ID失败: {e}")
            return 1000  # 出错时从1000开始

    def _generate_task_id(self):
        """生成新的任务ID"""
        alphabet = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'
        
        while True:
            # 生成4位ID
            result = ''
            seed = int(str(time.time_ns())[-6:])
            for _ in range(4):
                seed = (seed * 1103515245 + 12345) & 0x7fffffff
                result += alphabet[seed % len(alphabet)]
            
            # 检查是否已存在
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM tasks WHERE id = ?', (result,))
            exists = cursor.fetchone()
            conn.close()
            
            # 如果ID不存在，则使用它
            if not exists:
                return result

    def _get_contacts(self):
        """获取联系人列表,增加重试和缓存机制"""
        try:
            # 1. 先尝试获取缓存的联系人列表
            cache_response = self._fetch_contacts_list_cache(self.app_id)
            if cache_response and cache_response.get('ret') == 200:
                logger.info("[GeweChatTask] 成功获取联系人列表缓存")
                return cache_response.get('data', {}).get('friends', [])

            # 2. 如果缓存失效,重新获取完整列表
            response = self._fetch_contacts_list(self.app_id)
            if response and response.get('ret') == 200:
                logger.info("[GeweChatTask] 成功获取完整联系人列表")
                return response.get('data', {}).get('friends', [])
                
            logger.error(f"[GeweChatTask] 获取联系人列表失败: {response}")
            return []
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取联系人列表异常: {e}")
            return []

    def _search_contact(self, user_name: str) -> Optional[str]:
        """搜索联系人
        Args:
            user_name: 用户名/昵称
        Returns:
            Optional[str]: 找到的用户wxid,未找到返回None
        """
        try:
            # 构造搜索请求
            data = {
                "appId": self.app_id,
                "contactsInfo": user_name
            }
            response = self._make_request("POST", "contacts/search", data)
            
            if response and response.get('ret') == 200:
                search_result = response.get('data', {})
                if search_result:
                    logger.info(f"[GeweChatTask] 搜索到用户: {user_name} -> {search_result.get('v3')}")
                    return search_result.get('v3')
            return None
        except Exception as e:
            logger.error(f"[GeweChatTask] 搜索用户失败: {e}")
            return None

    def _create_task(self, time_str, circle_str, event_str, context):
        """创建任务"""
        try:
            # 验证事件内容不能为空
            if not event_str or event_str.strip() == "":
                return "事件内容不能为空"

            # 1. 基本格式验证
            if not circle_str.startswith("cron["):  # 非 cron 表达式才需要验证时间
                is_valid, error_msg = self._validate_time_format(time_str)
                if not is_valid:
                    return error_msg
                
                # 验证时间值
                hour, minute = time_str.replace('：', ':').split(':')
                hour = int(hour)
                minute = int(minute)
                
                if hour > 23 or minute > 59:
                    return "时间格式错误：小时必须在0-23之间，分钟必须在0-59之间"
                
                # 检查是否过期（只在"今天"的情况下检查）
                if circle_str == "今天":
                    if hour < datetime.now().hour or (hour == datetime.now().hour and minute < datetime.now().minute):
                        return "指定的时间已过期，请设置未来的时间"
                elif len(circle_str) == 10:  # YYYY-MM-DD 格式
                    try:
                        target_date = datetime.strptime(f"{circle_str} {time_str}", "%Y-%m-%d %H:%M")
                        if target_date <= datetime.now():
                            return "指定的时间已过期，请设置未来的时间"
                    except ValueError:
                        return "日期格式错误"
            
            # 2. 验证周期格式
            if not self._validate_circle_format(circle_str):
                return "周期格式错误，支持：每天、每周x、工作日、YYYY-MM-DD、今天、明天、后天"

            # 3. 转换cron表达式
            cron_exp = self._convert_to_cron(circle_str, time_str)
            if not cron_exp:
                return "转换cron表达式失败"

            # 4. 获取基本消息信息
            cmsg: ChatMessage = context.get("msg", None)
            logger.debug(f"[GeweChatTask] 原始消息对象: {cmsg}")
            msg_info = {}
            
            # 5. 检查是否在私聊中指定了群名
            is_group = context.get("isgroup", False)
            group_name = None

            # 场景1：群聊场景 - 检查是否指定了群组
            if not is_group and event_str.startswith("g["):
                match = re.match(r'g\[([^\]]+)\](.*)', event_str)
                if match:
                    group_name = match.group(1)
                    event_str = match.group(2).strip()  # 移除群名标记，保留实际内容
                    logger.debug(f"[GeweChatTask] 从消息中提取群名: {group_name}, 实际内容: {event_str}")
                    
                    # 查询群组信息
                    conn = self._get_db_connection()  # 使用连接池
                    try:
                        cursor = conn.cursor()
                        cursor.execute('SELECT wxid FROM groups WHERE nickname = ?', (group_name,))
                        result = cursor.fetchone()
                        
                        if result:
                            group_wxid = result[0]
                            is_group = True
                            # 更新消息信息，将群ID设置为from_user_id
                            if cmsg:
                                msg_info = {
                                    "from_user_id": group_wxid,  # 使用群ID
                                    "actual_user_id": cmsg.from_user_id,  # 保留原始发送者ID
                                    "to_user_id": cmsg.to_user_id,
                                    "create_time": cmsg.create_time,
                                    "is_group": True
                                }
                            logger.debug(f"[GeweChatTask] 找到群组: {group_name}, wxid: {group_wxid}")
                        else:
                            return f"未找到群组: {group_name}"
                    finally:
                        self._return_db_connection(conn)  # 确保归还连接

            # 场景2：用户提醒场景 - 检查是否指定了用户
            elif context.get('user_mention'):
                target_user = context.get('user_mention')
                # 验证用户是否存在
                try:
                    response = self._get_contact_brief_info([target_user])
                    if response.get('ret') != 200 or not response.get('data'):
                        return f"无法验证用户信息，请确保用户存在"
                except Exception as e:
                    logger.error(f"[GeweChatTask] 验证用户信息失败: {e}")
                    return f"验证用户信息失败: {str(e)}"

                # 设置用户提醒的消息信息
                msg_info = {
                    "from_user_id": target_user,  # 使用目标用户ID
                    "actual_user_id": cmsg.from_user_id if cmsg else None,  # 保留原始发送者ID
                    "to_user_id": cmsg.to_user_id if cmsg else None,
                    "create_time": cmsg.create_time if cmsg else int(time.time()),
                    "is_group": False,  # 用户提醒始终是私聊
                    "user_mention": target_user  # 保存用户标记
                }
                is_group = False

            # 常规消息处理（非指定群组或用户）
            else:
                if cmsg:
                    msg_info = {
                        "from_user_id": cmsg.from_user_id,
                        "actual_user_id": getattr(cmsg, "actual_user_id", cmsg.from_user_id),
                        "to_user_id": cmsg.to_user_id,
                        "create_time": cmsg.create_time,
                        "is_group": is_group
                    }

            # 6. 生成任务ID并创建任务
            task_id = self._generate_task_id()
            logger.debug(f"[GeweChatTask] 生成任务ID: {task_id}")

            # 构建上下文信息
            context_info = {
                "type": context.type.name,
                "content": event_str,
                "isgroup": is_group,
                "msg": msg_info
            }
            logger.debug(f"[GeweChatTask] 上下文信息: {context_info}")
            
            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO tasks (id, time, circle, cron, event, context, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                task_id,
                time_str,
                circle_str,
                cron_exp,
                event_str,
                json.dumps(context_info),
                int(time.time())
            ))
            
            conn.commit()
            conn.close()
            
            logger.debug(f"[GeweChatTask] 任务创建成功: {task_id}")
            return f"已创建任务: [{task_id}] {time_str} {circle_str} {event_str}"

        except Exception as e:
            logger.error(f"[GeweChatTask] 创建任务失败: {e}")
            return f"创建任务失败: {str(e)}"

    def _delete_task(self, task_id_or_target):
        """删除任务
        Args:
            task_id_or_target: 可以是以下几种格式：
                1. 任务ID：单个任务ID或多个任务ID（用逗号、空格、顿号分隔，如：TYZE,XL52 或 TYZE、XL52）
                2. 用户标识：u[用户名] - 删除指定用户的所有任务
                3. 群聊标识：g[群名] - 删除指定群的所有任务
                4. "all 密码" - 删除所有任务（需要提供管理员密码，密码必须与配置文件中的task_list_password匹配）
        Returns:
            str: 删除结果消息
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            deleted_tasks = []

            # 处理不同的删除模式
            if task_id_or_target.lower().startswith("all"):
                # 检查是否提供了密码
                parts = task_id_or_target.split()
                if len(parts) < 2:
                    return "删除所有任务需要提供管理员密码，格式：$time 取消 all 密码"

                # 验证密码
                password = parts[1]
                config_password = self.plugin_config.get('task_list_password')
                if not config_password:
                    return "管理员未设置访问密码，无法删除所有任务"

                if password != config_password:
                    return "管理员密码错误，无法删除所有任务"

                # 密码正确，删除所有任务
                cursor.execute('SELECT id, time, circle, event FROM tasks')
                tasks = cursor.fetchall()
                cursor.execute('DELETE FROM tasks')
                deleted_tasks = tasks

            elif task_id_or_target.startswith("u[") and task_id_or_target.endswith("]"):
                # 删除指定用户的任务
                user_name = task_id_or_target[2:-1]
                logger.info(f"[GeweChatTask] 开始查找用户 {user_name} 的任务")

                # 1. 先从联系人列表查找
                contacts = self._get_contacts()
                target_wxid = None
                
                logger.info(f"[GeweChatTask] 获取到联系人列表: {len(contacts) if contacts else 0} 个联系人")
                
                if contacts:
                    # 获取每个联系人的详细信息
                    for wxid in contacts:
                        try:
                            # 跳过系统内置联系人
                            if wxid in ['fmessage', 'medianote', 'floatbottle', 'weixin']:
                                continue
                                
                            logger.info(f"[GeweChatTask] 正在检查联系人: {wxid}")
                            response = self._get_contact_brief_info([wxid])
                            
                            if response and response.get('ret') == 200:
                                data = response.get('data', [])
                                if isinstance(data, list) and len(data) > 0:
                                    contact_info = data[0]
                                    logger.info(f"[GeweChatTask] 联系人详情: "
                                              f"wxid={wxid}, "
                                              f"昵称={contact_info.get('nickName')}, "
                                              f"备注={contact_info.get('remark')}")
                                    
                                    if contact_info.get('nickName', '').strip() == user_name.strip():
                                        target_wxid = wxid
                                        logger.info(f"[GeweChatTask] 找到目标用户: {user_name} -> {target_wxid}")
                                        break
                        except Exception as e:
                            logger.error(f"[GeweChatTask] 获取用户详情失败: {e}")
                            continue

                # 2. 如果联系人列表中未找到,尝试搜索
                if not target_wxid:
                    logger.info(f"[GeweChatTask] 联系人列表未找到用户,尝试搜索: {user_name}")
                    target_wxid = self._search_contact(user_name)

                if target_wxid:
                    # 修改查询语句，只查找指定用户作为执行对象的任务
                    query = '''
                        SELECT t.id, t.time, t.circle, t.event, t.context
                        FROM tasks t
                        WHERE (
                            -- 用户是发送者
                            json_extract(t.context, '$.msg.from_user_id') = ?
                            -- 或者内容中包含用户标记
                            OR json_extract(t.context, '$.content') LIKE ?
                        )
                    '''
                    query_params = (
                        target_wxid,                # from_user_id
                        f"%u[{user_name}]%"        # content LIKE
                    )
                    
                    logger.info(f"[GeweChatTask] 执行查询: {query}")
                    logger.info(f"[GeweChatTask] 查询参数: {query_params}")

                    try:
                        cursor.execute(query, query_params)
                        tasks = cursor.fetchall()

                        # 打印找到的任务详情
                        logger.info(f"[GeweChatTask] 找到 {len(tasks) if tasks else 0} 个任务")
                        for task in tasks:
                            try:
                                context = json.loads(task[4])
                                logger.info(f"[GeweChatTask] 任务详情: \n"
                                          f"ID={task[0]}\n"
                                          f"时间={task[1]}\n"
                                          f"周期={task[2]}\n"
                                          f"事件={task[3]}\n"
                                          f"上下文={json.dumps(context, ensure_ascii=False, indent=2)}")
                            except Exception as e:
                                logger.error(f"[GeweChatTask] 解析任务详情失败: {e}")

                        if tasks:
                            # 删除找到的任务
                            task_ids = [task[0] for task in tasks]
                            delete_query = 'DELETE FROM tasks WHERE id IN ({})'.format(
                                ','.join(['?'] * len(task_ids))
                            )
                            logger.info(f"[GeweChatTask] 执行删除: {delete_query} 参数: {task_ids}")
                            
                            try:
                                cursor.execute(delete_query, task_ids)
                                conn.commit()  # 确保提交事务
                                logger.info(f"[GeweChatTask] 成功删除 {len(task_ids)} 个任务")
                                return f"已删除 {len(task_ids)} 个任务"
                            except Exception as e:
                                conn.rollback()  # 发生错误时回滚
                                logger.error(f"[GeweChatTask] 删除任务失败: {e}")
                                return f"删除任务失败: {str(e)}"
                        else:
                            logger.info(f"[GeweChatTask] 未找到用户 {user_name} 的任务")
                            return f"未找到用户 {user_name} 的任务"

                    except Exception as e:
                        conn.rollback()  # 发生错误时回滚
                        logger.error(f"[GeweChatTask] 查询任务失败: {e}")
                        return f"查询任务失败: {str(e)}"
                else:
                    error_msg = f"未找到用户: {user_name} (已尝试联系人列表和搜索)"
                    logger.info(f"[GeweChatTask] {error_msg}")
                    return error_msg

            elif task_id_or_target.startswith("g[") and task_id_or_target.endswith("]"):
                # 删除指定群聊的任务
                group_name = task_id_or_target[2:-1]
                # 先获取群聊的所有任务
                cursor.execute('''
                    SELECT t.id, t.time, t.circle, t.event
                    FROM tasks t
                    JOIN groups g ON g.wxid = json_extract(t.context, '$.msg.from_user_id')
                    WHERE g.nickname = ?
                ''', (group_name,))
                tasks = cursor.fetchall()
                if tasks:
                    # 删除找到的任务
                    task_ids = [task[0] for task in tasks]
                    cursor.execute('DELETE FROM tasks WHERE id IN ({})'.format(
                        ','.join(['?'] * len(task_ids))
                    ), task_ids)
                    deleted_tasks = tasks

            else:
                # 处理多个任务ID的情况
                # 支持的分隔符：逗号、中文逗号、顿号、空格
                task_ids = re.split(r'[,，、\s]+', task_id_or_target)
                task_ids = [tid.strip().upper() for tid in task_ids if tid.strip()]  # 去除空白并转大写

                if not task_ids:
                    return "请指定要删除的任务ID"

                # 使用 IN 子句一次性查询所有任务
                placeholders = ','.join(['?' for _ in task_ids])
                cursor.execute(f'''
                    SELECT id, time, circle, event 
                    FROM tasks 
                    WHERE UPPER(id) IN ({placeholders})
                ''', task_ids)
                tasks = cursor.fetchall()

                if tasks:
                    # 删除找到的任务
                    task_ids_found = [task[0] for task in tasks]
                    cursor.execute(f'DELETE FROM tasks WHERE UPPER(id) IN ({placeholders})', task_ids)
                    deleted_tasks = tasks

                    # 检查是否有未找到的任务ID
                    not_found_ids = set(task_ids) - set(tid.upper() for tid in task_ids_found)
                    if not_found_ids:
                        logger.warning(f"[GeweChatTask] 以下任务未找到: {', '.join(not_found_ids)}")

            conn.commit()
            conn.close()
            
            # 生成删除结果消息
            if not deleted_tasks:
                if task_id_or_target.lower().startswith("all"):
                    return "没有找到任何任务可以删除"
                elif task_id_or_target.startswith("u["):
                    return f"未找到用户 {task_id_or_target[2:-1]} 的任务"
                elif task_id_or_target.startswith("g["):
                    return f"未找到群聊 {task_id_or_target[2:-1]} 的任务"
                else:
                    return f"任务不存在: {task_id_or_target.upper()}"

            # 格式化删除结果
            result = "已删除以下任务:\n" + "-" * 30 + "\n"
            for task in deleted_tasks:
                result += f"[{task[0]}] {task[2]} {task[1]} {task[3]}\n"
            result += f"\n共删除 {len(deleted_tasks)} 个任务"

            return result

        except Exception as e:
            logger.error(f"[GeweChatTask] 删除任务失败: {e}")
            return f"删除任务失败: {str(e)}"

    def _clean_expired_tasks(self):
        """清理过期任务"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有任务
            cursor.execute('''
                SELECT id, circle, time, cron, last_executed_at, created_at 
                FROM tasks
            ''')
            tasks = cursor.fetchall()
            now = datetime.now()
            deleted_count = 0
            
            for task_id, circle_str, time_str, cron_str, last_executed_at, created_at in tasks:
                try:
                    # 只处理一次性任务（具体日期或今天明天后天）
                    if len(circle_str) == 10 or circle_str in ["今天", "明天", "后天"]:
                        # 解析任务的目标执行时间
                        target_time = None
                        
                        if len(circle_str) == 10:  # YYYY-MM-DD格式
                            try:
                                target_time = datetime.strptime(f"{circle_str} {time_str}", "%Y-%m-%d %H:%M")
                            except ValueError:
                                logger.error(f"[GeweChatTask] 无效的日期格式: {circle_str} {time_str}")
                                continue
                        else:  # 今天、明天、后天
                            days_offset = {"今天": 0, "明天": 1, "后天": 2}
                            base_date = datetime.fromtimestamp(created_at)
                            hour, minute = map(int, time_str.split(':'))
                            target_time = base_date.replace(
                                hour=hour,
                                minute=minute,
                                second=0,
                                microsecond=0
                            ) + timedelta(days=days_offset[circle_str])
                        
                        if not target_time:
                            continue
                            
                        # 检查任务是否已过期
                        # 1. 任务目标时间已过
                        # 2. 任务已执行过（last_executed_at > 0）或创建超过48小时
                        is_expired = (
                            now > target_time and (
                                last_executed_at > 0 or  # 已执行过
                                (now - target_time).total_seconds() > 48 * 3600  # 超过48小时未执行
                            )
                        )
                        
                        if is_expired:
                            cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
                            deleted_count += 1
                            logger.info(f"[GeweChatTask] 删除过期任务: {task_id} {circle_str} {time_str} "
                                      f"(目标时间: {target_time}, 最后执行: {datetime.fromtimestamp(last_executed_at) if last_executed_at else '从未执行'})")
                            
                except Exception as e:
                    logger.error(f"[GeweChatTask] 检查任务过期失败: {task_id} {e}")
                    continue
            
            conn.commit()
            logger.info(f"[GeweChatTask] 清理过期任务完成，共删除 {deleted_count} 个任务")
        except Exception as e:
            logger.error(f"[GeweChatTask] 清理过期任务失败: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def _execute_task(self, task_id: str, context_info: dict):
        """执行任务（添加任务锁保护）"""
        task_lock = self._get_task_lock(task_id)
        if not task_lock.acquire(blocking=False):
            logger.warning(f"[GeweChatTask] 任务 {task_id} 正在执行中，跳过本次执行")
            return

        try:
            if not isinstance(task_id, str) or not isinstance(context_info, dict):
                logger.error(f"[GeweChatTask] 参数类型错误: task_id={type(task_id)}, context_info={type(context_info)}")
                raise ValueError("参数类型错误")

            msg_info = context_info.get('msg', {})
            content = context_info.get('content', '')
            is_group = context_info.get('isgroup', False)
            
            if not content:
                logger.error(f"[GeweChatTask] 任务内容为空: {task_id}")
                raise ValueError("任务内容不能为空")
            
            logger.info(f"[GeweChatTask] 开始执行任务: {task_id}")
            
            # 检查是否是插件调用
            plugin_match = re.match(r'p\[([^\]]+)\]', content)

            if plugin_match:
                # 插件调用
                plugin_name = plugin_match.group(1)
                if not plugin_name:
                    raise ValueError("插件名称不能为空")

                # 获取接收者ID
                to_wxid = msg_info.get('from_user_id')
                if not to_wxid:
                    raise ValueError("无法获取接收者ID")

                # 检查插件配置并执行
                if plugin_name.lower() == "老黄历":
                    result = self._execute_laohuangli_plugin(context_info)
                    if not result:
                        raise ValueError("获取老黄历信息失败")

                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        result
                    ):
                        raise GeweChatTaskError("发送老黄历信息失败")

                    # 在新线程中检查API余额
                    threading.Thread(target=self._check_and_notify_balance, daemon=True).start()

                elif plugin_name.startswith("星座-"):
                    result = self._execute_xingzuo_plugin(plugin_name[3:], context_info)
                    if not result:
                        raise ValueError("获取星座运势失败")

                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        result
                    ):
                        raise GeweChatTaskError("发送星座运势失败")
                elif plugin_name.lower() == "余额查询":
                    # 检查API余额
                    remaining_times = self._check_api_balance()

                    # 构建消息内容
                    if remaining_times is None:
                        message = "📊 API余额查询\n{'='*22}\n当前账户类型：包年会员\n无调用次数限制"
                    else:
                        message = f"📊 API余额查询\n{'='*22}\n当前剩余调用次数：{remaining_times}"

                    # 发送消息
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        message
                    ):
                        raise GeweChatTaskError("发送余额查询信息失败")
                else:
                    error_msg = f"⚠️ 插件执行失败\n{'='*22}\n不支持的插件类型: {plugin_name}\n支持的插件类型:\n- 老黄历\n- 星座-xx座（如：星座-白羊座）\n- 余额查询"
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        error_msg
                    ):
                        logger.error(f"[GeweChatTask] 发送错误提示失败: {error_msg}")
                    raise ValueError(f"不支持的插件类型: {plugin_name}")
            else:
                # 检查是否包含用户或群组标记，并提取实际内容
                actual_content = content
                target_wxid = msg_info.get('from_user_id')  # 默认发送者
                user_match = re.search(r'u\[([^\]]+)\]\s*(.*)', content)
                group_match = re.search(r'g\[([^\]]+)\]\s*(.*)', content)
                
                if user_match or group_match:
                    # 提取目标用户/群组和实际内容
                    if user_match:
                        target_name = user_match.group(1)
                        actual_content = user_match.group(2)
                        # 获取用户wxid
                        logger.info(f"[GeweChatTask] 尝试获取用户wxid，昵称: {target_name}")
                        target_wxid = self._get_user_by_nickname(target_name)
                        logger.info(f"[GeweChatTask] 获取用户wxid结果: {target_wxid}")
                        if not target_wxid:
                            raise ValueError(f"未找到用户: {target_name}")
                        logger.info(f"[GeweChatTask] 找到用户: {target_name} -> {target_wxid}")
                    else:
                        target_name = group_match.group(1)
                        actual_content = group_match.group(2)
                        # 获取群组wxid
                        logger.info(f"[GeweChatTask] 尝试获取群组wxid，群名: {target_name}")
                        conn = self._get_db_connection()
                        try:
                            cursor = conn.cursor()
                            cursor.execute('SELECT wxid FROM groups WHERE nickname = ?', (target_name,))
                            result = cursor.fetchone()
                            if result:
                                target_wxid = result[0]
                                logger.info(f"[GeweChatTask] 找到群组: {target_name} -> {target_wxid}")
                            else:
                                raise ValueError(f"未找到群组: {target_name}")
                        finally:
                            self._return_db_connection(conn)
                    
                    # 更新msg_info中的接收者信息
                    if isinstance(msg_info, dict):
                        msg_info['to_user_id'] = target_wxid
                        msg_info['content'] = actual_content
                
                # 去除所有前导和尾随空格
                actual_content = actual_content.strip()
                logger.info(f"[GeweChatTask] 处理后的内容: {actual_content}, 目标接收者: {target_wxid}")
                
                if actual_content.startswith('提醒'):
                    # 使用更新后的msg_info（包含正确的接收者信息）
                    self._handle_reminder_task(task_id, actual_content, msg_info)
                else:
                    # 其他消息交给系统其他插件处理，使用更新后的msg_info
                    msg_info['from_user_id'] = target_wxid  # 更新接收者信息
                    self._create_and_send_chat_message(actual_content, msg_info, is_group)
                    logger.info(f"[GeweChatTask] 已转发消息到插件处理: {task_id}")
            
            logger.info(f"[GeweChatTask] 任务执行完成: {task_id}")

        except Exception as e:
            logger.error(f"[GeweChatTask] 执行任务异常: {task_id} {str(e)}")
            self._update_task_status(task_id, failed=True)
            raise GeweChatTaskError(f"执行任务失败: {str(e)}")
        finally:
            task_lock.release()

    def _update_task_status(self, task_id: str, failed: bool = False):
        """更新任务状态"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = int(time.time())
            if failed:
                cursor.execute('''
                    UPDATE tasks 
                    SET executing_at = 0,
                        last_executed_at = ?
                    WHERE id = ?
                ''', (current_time, task_id))
            else:
                cursor.execute('''
                    UPDATE tasks 
                    SET executing_at = 0,
                        last_executed_at = ?
                    WHERE id = ?
                ''', (current_time, task_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"[GeweChatTask] 更新任务状态失败: {task_id} {str(e)}")

    def _handle_reminder_task(self, task_id: str, content: str, msg_info: dict):
        """处理提醒任务"""
        try:
            with self._message_lock:
                # 优先使用指定的接收者ID，如果没有则使用发送者ID
                to_wxid = msg_info.get('to_user_id') or msg_info.get('from_user_id')
                if not to_wxid:
                    raise ValueError("无法获取接收者ID")
                
                # 去掉"提醒"前缀并格式化消息
                reminder_content = content[2:].strip()  # 移除"提醒"两个字
                if not reminder_content:
                    raise ValueError("提醒内容不能为空")
                    
                # 记录日志
                logger.info(f"[GeweChatTask] 处理提醒任务: {task_id}, 内容: {reminder_content}, 接收者: {to_wxid}")
                    
                reminder_message = f"⏰ 定时提醒\n{'-' * 20}\n{reminder_content}\n{'-' * 20}\n发送时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                
                # 使用重试机制发送消息
                if not self._send_message_with_retry(
                    self.app_id,
                    to_wxid,
                    reminder_message
                ):
                    raise GeweChatTaskError("发送提醒消息失败")
                    
                logger.info(f"[GeweChatTask] 已发送提醒消息: {task_id}")
                
        except Exception as e:
            logger.error(f"[GeweChatTask] 发送提醒消息失败: {e}")
            raise

    def _handle_plugin_task(self, task_id: str, plugin_name: str, context_info: dict):
        """处理插件任务"""
        try:
            with self._message_lock:
                # 获取接收者ID
                msg_info = context_info.get('msg', {})
                to_wxid = msg_info.get('from_user_id')
                if not to_wxid:
                    raise ValueError("无法获取接收者ID")

                # 检查插件配置并执行
                if plugin_name.lower() == "老黄历":
                    result = self._execute_laohuangli_plugin(context_info)
                    if not result:
                        raise ValueError("获取老黄历信息失败")
                        
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        result
                    ):
                        raise GeweChatTaskError("发送老黄历信息失败")
                        
                    # 在新线程中检查API余额
                    threading.Thread(target=self._check_and_notify_balance, daemon=True).start()
                    
                elif plugin_name.startswith("星座-"):
                    result = self._execute_xingzuo_plugin(plugin_name[3:], context_info)
                    if not result:
                        raise ValueError("获取星座运势失败")
                        
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        result
                    ):
                        raise GeweChatTaskError("发送星座运势失败")
                elif plugin_name.lower() == "余额查询":
                    # 检查API余额
                    remaining_times = self._check_api_balance()
                    
                    # 构建消息内容
                    if remaining_times is None:
                        message = "📊 API余额查询\n{'='*22}\n当前账户类型：包年会员\n无调用次数限制"
                    else:
                        message = f"📊 API余额查询\n{'='*22}\n当前剩余调用次数：{remaining_times}"
                    
                    # 发送消息
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        message
                    ):
                        raise GeweChatTaskError("发送余额查询信息失败")
                else:
                    error_msg = f"⚠️ 插件执行失败\n{'='*22}\n不支持的插件类型: {plugin_name}\n支持的插件类型:\n- 老黄历\n- 星座-xx座（如：星座-白羊座）\n- 余额查询"
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        error_msg
                    ):
                        logger.error(f"[GeweChatTask] 发送错误提示失败: {error_msg}")
                    raise ValueError(f"不支持的插件类型: {plugin_name}")
                
        except Exception as e:
            # 发送错误提示给用户
            error_msg = f"⚠️ 插件执行失败\n{'='*22}\n{str(e)}"
            if not self._send_message_with_retry(
                self.app_id,
                to_wxid,
                error_msg
            ):
                logger.error(f"[GeweChatTask] 发送错误提示失败: {error_msg}")
            raise

    def _handle_normal_task(self, task_id: str, content: str, msg_info: dict, is_group: bool):
        """处理普通消息任务"""
        try:
            with self._message_lock:
                to_wxid = msg_info.get('from_user_id')
                if not to_wxid:
                    raise ValueError("无法获取接收者ID")
                
                # 创建聊天消息对象并发送
                chat_msg = ChatMessage({})
                chat_msg.content = content
                chat_msg.from_user_id = msg_info.get('from_user_id')
                chat_msg.to_user_id = msg_info.get('to_user_id')
                chat_msg.actual_user_id = msg_info.get('actual_user_id', msg_info.get('from_user_id'))
                chat_msg.create_time = msg_info.get("create_time", int(time.time()))
                chat_msg.is_group = is_group
                chat_msg._prepared = True
                chat_msg.other_user_id = chat_msg.from_user_id

                # 设置昵称信息
                if is_group and self.device_id:
                    self._set_group_nickname(chat_msg)
                else:
                    self._set_user_nickname(chat_msg)

                # 构建Context
                context = Context(ContextType.TEXT, content)
                context["session_id"] = msg_info.get('from_user_id')
                context["receiver"] = msg_info.get('from_user_id')
                context["msg"] = chat_msg
                context["isgroup"] = is_group
                context["group_name"] = chat_msg.other_user_nickname if is_group else None
                context["is_shared_session_group"] = True if is_group else False
                context["origin_ctype"] = ContextType.TEXT
                context["openai_api_key"] = None
                context["gpt_model"] = None
                context["no_need_at"] = True

                # 创建事件上下文
                e_context = EventContext(Event.ON_HANDLE_CONTEXT)
                e_context["context"] = context
                e_context["reply"] = None
                e_context["channel"] = "gewechat"
                e_context["action"] = EventAction.CONTINUE

                # 触发事件，让其他插件处理
                self.emit_event(Event.ON_HANDLE_CONTEXT, e_context)

                # 如果没有插件处理，则直接发送原始消息
                if e_context.action == EventAction.CONTINUE:
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        content
                    ):
                        raise GeweChatTaskError("发送消息失败")
                
                logger.info(f"[GeweChatTask] 已发送普通消息: {task_id}")
                
        except Exception as e:
            logger.error(f"[GeweChatTask] 发送普通消息失败: {e}")
            raise

    def _send_message_with_retry(self, app_id: str, to_wxid: str, content: str, max_retries: int = MAX_RETRIES) -> bool:
        """带重试机制的消息发送

        Args:
            app_id: 应用ID
            to_wxid: 接收者ID
            content: 消息内容
            max_retries: 最大重试次数，默认3次

        Returns:
            bool: 是否发送成功
        """
        retry_count = 0
        delay = RETRY_DELAY
        
        while retry_count < max_retries:
            try:
                response = self._post_text(app_id, to_wxid, content)
                if response and response.get('ret') == 200:
                    logger.info(f"[GeweChatTask] 消息发送成功: {to_wxid}")
                    return True
                else:
                    logger.warning(f"[GeweChatTask] 发送消息失败，状态码: {response.get('ret')}, 响应: {response}")
            
            except Exception as e:
                logger.error(f"[GeweChatTask] 发送消息异常: {str(e)}, 重试中...")
            
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(delay)
                delay *= 2  # 指数退避
        
        return False

    def _create_and_send_chat_message(self, content: str, msg_info: dict, is_group: bool):
        """
        创建并发送聊天消息
        
        Args:
            content: 消息内容
            msg_info: 消息信息字典
            is_group: 是否为群消息
        """
        if not content or not isinstance(msg_info, dict):
            raise ValueError("无效的消息参数")
            
        with self._message_lock:
            try:
                # 创建基础消息对象
                chat_msg = ChatMessage({})
                chat_msg.content = content
                chat_msg.from_user_id = msg_info.get('from_user_id')
                if not chat_msg.from_user_id:
                    raise ValueError("无法获取发送者ID")
                    
                chat_msg.to_user_id = msg_info.get('to_user_id')
                chat_msg.actual_user_id = msg_info.get('actual_user_id', msg_info.get('from_user_id'))
                chat_msg.create_time = msg_info.get("create_time", int(time.time()))
                chat_msg.is_group = is_group
                chat_msg._prepared = True
                chat_msg.other_user_id = chat_msg.from_user_id

                # 设置昵称信息
                if is_group and self.device_id:  # 只在是群消息且有设备ID时获取群信息
                    self._set_group_nickname(chat_msg)
                else:
                    # 非群消息或没有设备ID时，直接设置用户昵称
                    self._set_user_nickname(chat_msg)

                # 构建Context
                context = self._build_context(content, msg_info, chat_msg, is_group)
                
                # 发送消息
                from channel.gewechat.gewechat_channel import GeWeChatChannel
                channel = GeWeChatChannel()
                if not channel.client:
                    channel.client = self.client
                channel.produce(context)
            except Exception as e:
                logger.error(f"[GeweChatTask] 创建或发送消息失败: {str(e)}")
                raise

    def _set_group_nickname(self, chat_msg):
        """设置群聊昵称"""
        try:
            # 检查设备 ID
            if not self.device_id:
                logger.info("[GeweChatTask] 设备 ID 未设置，使用默认昵称")  # 改为 debug 级别，因为这是预期的情况
                chat_msg.other_user_nickname = chat_msg.from_user_id
                chat_msg.actual_user_nickname = chat_msg.from_user_id
                return
                
            group_info = self._get_chatroom_info(self.device_id, chat_msg.from_user_id)
            if group_info.get('ret') == 200:
                data = group_info.get('data', {})
                chat_msg.other_user_nickname = data.get('nickName', chat_msg.from_user_id)
                chat_msg.actual_user_nickname = data.get('nickName', chat_msg.from_user_id)
                logger.info(f"[GeweChatTask] 成功获取群名称: {chat_msg.other_user_nickname}")
            else:
                logger.info(f"[GeweChatTask] 获取群信息返回非200状态: {group_info}")
                chat_msg.other_user_nickname = chat_msg.from_user_id
                chat_msg.actual_user_nickname = chat_msg.from_user_id
        except Exception as e:
            logger.debug(f"[GeweChatTask] 获取群名称失败: {e}")  # 改为 debug 级别
            chat_msg.other_user_nickname = chat_msg.from_user_id
            chat_msg.actual_user_nickname = chat_msg.from_user_id

    def _set_user_nickname(self, chat_msg):
        """设置用户昵称"""
        try:
            response = self._get_contact_brief_info([chat_msg.from_user_id])
            if response and response.get('ret') == 200:
                data = response.get('data', [])
                if isinstance(data, list) and len(data) > 0:
                    chat_msg.other_user_nickname = data[0].get('nickName', chat_msg.from_user_id)
                    chat_msg.actual_user_nickname = data[0].get('nickName', chat_msg.from_user_id)
                else:
                    chat_msg.other_user_nickname = chat_msg.from_user_id
                    chat_msg.actual_user_nickname = chat_msg.from_user_id
            else:
                chat_msg.other_user_nickname = chat_msg.from_user_id
                chat_msg.actual_user_nickname = chat_msg.from_user_id
        except Exception as e:
            logger.error(f"[GeweChatTask] 设置用户昵称失败: {e}")
            chat_msg.other_user_nickname = chat_msg.from_user_id
            chat_msg.actual_user_nickname = chat_msg.from_user_id

    def _get_user_by_nickname(self, nickname: str) -> Optional[str]:
        """
        根据昵称获取用户wxid
        
        Args:
            nickname: 用户昵称
            
        Returns:
            Optional[str]: 用户wxid，如果未找到则返回None
        """
        try:
            # 获取联系人列表
            response = self._fetch_contacts_list(self.app_id)
            if response.get('ret') != 200 or not response.get('data', {}).get('friends'):
                return None
                
            contacts = response['data']['friends']
            
            for wxid in contacts:
                try:
                    # 跳过系统内置联系人
                    if wxid in ['fmessage', 'medianote', 'floatbottle', 'weixin']:
                        continue
                        
                    # 获取联系人信息（直接使用data字段）
                    response = self._get_contact_brief_info([wxid])
                    if response and response.get('ret') == 200:
                        contact_list = response.get('data', [])  # 直接获取data列表
                        if isinstance(contact_list, list) and len(contact_list) > 0:
                            contact_info = contact_list[0]  # 直接取第一个元素
                            current_nickname = contact_info.get('nickName', '')
                            if current_nickname.strip() == nickname.strip():
                                return wxid
                except Exception as e:
                    logger.debug(f"[GeweChatTask] 查询用户信息异常: {e}")
                    
            return None
            
        except Exception as e:
            logger.error(f"[GeweChatTask] 根据昵称获取用户失败: {e}")
            return None

    def _build_context(self, content, msg_info, chat_msg, is_group):
        """构建Context对象"""
        context = Context(ContextType.TEXT, content)
        context["session_id"] = msg_info.get('from_user_id')
        context["receiver"] = msg_info.get('from_user_id')
        context["msg"] = chat_msg
        context["isgroup"] = is_group
        context["group_name"] = chat_msg.other_user_nickname if is_group else None
        context["is_shared_session_group"] = True if is_group else False
        context["origin_ctype"] = ContextType.TEXT
        context["openai_api_key"] = None
        context["gpt_model"] = None
        context["no_need_at"] = True
        return context

    def emit_event(self, event: Event, e_context: EventContext = None):
        """触发事件"""
        try:
            # 获取插件管理器
            plugin_manager = PluginManager()
            # 触发事件
            plugin_manager.emit_event(event, e_context)
        except Exception as e:
            logger.error(f"[GeweChatTask] 触发事件失败: {e}")
            # 如果事件触发失败，继续执行默认行为
            if e_context and e_context.action == EventAction.CONTINUE:
                try:
                    # 获取原始消息内容和接收者
                    content = e_context['context'].content
                    to_wxid = e_context['context']['msg'].from_user_id
                    
                    # 直接发送消息
                    if not self._send_message_with_retry(
                        self.app_id,
                        to_wxid,
                        content
                    ):
                        logger.error("[GeweChatTask] 发送默认消息失败")
                except Exception as send_error:
                    logger.error(f"[GeweChatTask] 发送默认消息异常: {send_error}")

    def on_handle_context(self, e_context: EventContext):
        """处理消息"""
        if e_context['context'].type != ContextType.TEXT:
            return

        content = e_context['context'].content.strip()
        if not content:
            return
        
        # 获取是否群聊
        is_group = e_context['context'].get('isgroup', False)
        
        # 群聊时移除用户id前缀
        if is_group and ":" in content:
            parts = content.split(":", 1)
            if not self.command_prefix in parts[0]:
                content = parts[1].strip()
        
        # 处理命令
        logger.debug(f"[GeweChatTask] 收到命令: {content}")
        if content.startswith(self.command_prefix):
            command = content[len(self.command_prefix):].strip()
            
            # 空命令显示帮助
            if not command:
                e_context['reply'] = Reply(ReplyType.TEXT, self.get_help_text())
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 插件直接调用机制 - 检查是否是直接的插件调用
            plugin_match = re.match(r'^p\[([^\]]+)\]', command)
            if plugin_match:
                plugin_name = plugin_match.group(1)
                # 创建一个当前时间的任务
                now = datetime.now()
                time_str = now.strftime("%H:%M")
                circle_str = "今天"
                
                # 构建任务内容
                event_str = command  # 使用完整的命令作为事件内容
                
                # 创建并执行任务
                result = self._create_task(time_str, circle_str, event_str, e_context['context'])
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 1. 先处理特殊命令（任务列表和取消任务）
            if command.startswith("任务列表"):
                # 检查是否提供了密码
                parts = command.split()
                if len(parts) < 2:
                    e_context['reply'] = Reply(ReplyType.ERROR, "请提供访问密码")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                # 验证密码
                password = parts[1]
                config_password = self.plugin_config.get('task_list_password')
                if not config_password:
                    e_context['reply'] = Reply(ReplyType.ERROR, "管理员未设置访问密码，无法查看任务列表")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                if password != config_password:
                    e_context['reply'] = Reply(ReplyType.ERROR, "访问密码错误")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                # 密码正确，显示任务列表
                task_list = self._get_task_list()
                e_context['reply'] = Reply(ReplyType.TEXT, task_list)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 取消任务
            if command.startswith("取消"):
                # 移除"取消"并清理空格
                target = command[2:].strip()
                if not target:
                    e_context['reply'] = Reply(ReplyType.ERROR, "请指定要取消的任务ID、用户、群聊或 all")
                    e_context.action = EventAction.BREAK_PASS
                    return

                result = self._delete_task(target)
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 2. 处理 cron 表达式命令
            if "cron[" in command:
                # 使用正则表达式匹配 cron 表达式和事件内容
                match = re.match(r'cron\[(.*?)\]\s*(.*)', command)
                if match:
                    cron_exp = match.group(1).strip()
                    event_str = match.group(2).strip()
                    if not event_str:
                        e_context['reply'] = Reply(ReplyType.ERROR, "请输入事件内容")
                        e_context.action = EventAction.BREAK_PASS
                        return
                    result = self._create_task("cron", f"cron[{cron_exp}]", event_str, e_context['context'])
                    e_context['reply'] = Reply(ReplyType.TEXT, result)
                    e_context.action = EventAction.BREAK_PASS
                    return
                else:
                    e_context['reply'] = Reply(ReplyType.ERROR, "cron表达式格式错误，正确格式：$time cron[分 时 日 月 周] 事件内容")
                    e_context.action = EventAction.BREAK_PASS
                    return
            
            # 3. 处理普通定时任务
            parts = command.split(" ", 2)
            if len(parts) == 3:
                circle_str, time_str, event_str = parts
                
                # 检查是否包含用户标记
                user_name = None
                if 'u[' in event_str:
                    match = re.match(r'u\[([^\]]+)\]\s*(.*)', event_str)
                    if match:
                        user_name = match.group(1)
                        event_str = match.group(2).strip()
                        
                        # 获取联系人列表并查找目标用户
                        contacts = self._get_contacts()
                        target_wxid = None  # 修改变量名保持一致
                        
                        if contacts:
                            # 获取每个联系人的详细信息
                            for wxid in contacts:
                                try:
                                    response = self._get_contact_brief_info([wxid])
                                    
                                    if response and response.get('ret') == 200:
                                        data = response.get('data', [])
                                        if isinstance(data, list) and len(data) > 0:
                                            contact_info = data[0]  # 直接取第一个元素
                                            if contact_info.get('nickName') == user_name:
                                                target_wxid = wxid
                                                logger.info(f"[GeweChatTask] 找到目标用户: {user_name} -> {target_wxid}")
                                                break
                                except Exception as e:
                                    logger.error(f"[GeweChatTask] 获取用户详情失败: {e}, 响应内容: {response}")
                                    continue
                            
                            if not target_wxid:  # 修改判断条件
                                e_context['reply'] = Reply(ReplyType.ERROR, f"未找到用户: {user_name}，请确保昵称完全匹配")
                                e_context.action = EventAction.BREAK_PASS
                                return
                        else:
                            e_context['reply'] = Reply(ReplyType.ERROR, "获取联系人列表失败，请稍后重试")
                            e_context.action = EventAction.BREAK_PASS
                            return
                        
                        # 修改上下文信息，添加用户标记
                        e_context['context']['user_mention'] = target_wxid  # 使用 target_wxid
                
                result = self._create_task(time_str, circle_str, event_str, e_context['context'])
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 命令格式错误
            e_context['reply'] = Reply(ReplyType.ERROR, "命令格式错误，请查看帮助信息")
            e_context.action = EventAction.BREAK_PASS
            return

    def _execute_laohuangli_plugin(self, context_info: dict) -> str:
        """老黄历插件
        Args:
            context_info: 上下文信息
        Returns:
            str: 老黄历信息
        """
        try:
            # 检查是否配置了API密钥
            api_key = self.plugin_config.get('yuanfenju_api_key')
            if not api_key:
                return "老黄历插件未配置API密钥，请联系管理员配置"

            # 获取当前日期
            today = datetime.now().strftime('%Y-%m-%d')

            # 调用老黄历API
            url = "https://api.yuanfenju.com/index.php/v1/Gongju/laohuangli"
            params = {
                'api_key': api_key,
                'title_laohuangli': today
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            # 添加超时设置和重试机制
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    response = requests.post(url, data=params, headers=headers, timeout=10)
                    break
                except requests.Timeout:
                    retry_count += 1
                    if retry_count == max_retries:
                        return "调用老黄历API超时，请稍后重试"
                    logger.warning(f"[GeweChatTask] 老黄历API调用超时，第{retry_count}次重试")
                    time.sleep(1)
                except requests.RequestException as e:
                    return f"调用老黄历API异常: {str(e)}"
            
            if response.status_code != 200:
                error_msg = f"调用老黄历API失败: HTTP {response.status_code}"
                logger.error(f"[GeweChatTask] {error_msg}")
                return error_msg

            try:
                data = response.json()
            except ValueError:
                error_msg = "解析老黄历API响应失败: 返回数据格式错误"
                logger.error(f"[GeweChatTask] {error_msg}")
                return error_msg

            if data.get('errcode') != 0:
                error_msg = f"获取老黄历信息失败: {data.get('errmsg', '未知错误')}"
                logger.error(f"[GeweChatTask] {error_msg}")
                return error_msg

            # 格式化返回信息
            result = data.get("data", {})
            if not result:
                return "获取老黄历信息失败: 返回数据为空"
            
            return f"""📅 今日老黄历 ({result.get('yangli', today)})
{'='*22}
📆 农历：{result.get('yinli', '无')}
🎯 宜：{result.get('yi', '无')}
❌ 忌：{result.get('ji', '无')}
💰 财神方位：{result.get('caishenfangwei', '无')}
🌟 吉神宜趋：{result.get('jishen', '无')}
👻 凶神方位：{result.get('xiongshen', '无')}
🎨 五行：{result.get('wuxing', '无')}
⭐ 星宿：{result.get('xingxiu', '无')}
{'='*22}"""

        except Exception as e:
            error_msg = f"执行老黄历插件失败: {str(e)}"
            logger.error(f"[GeweChatTask] {error_msg}")
            return error_msg

    def _execute_xingzuo_plugin(self, xingzuo_name: str, context_info: dict) -> str:
        """星座运势插件
        Args:
            xingzuo_name: 星座名称
            context_info: 上下文信息
        Returns:
            str: 星座运势信息
        """
        try:
            # 检查是否配置了API密钥
            api_key = self.plugin_config.get('yuanfenju_api_key')
            if not api_key:
                return "星座运势插件未配置API密钥，请联系管理员配置"

            # 星座ID映射
            xingzuo_map = {
                "白羊座": "0", "金牛座": "1", "双子座": "2", "巨蟹座": "3",
                "狮子座": "4", "处女座": "5", "天秤座": "6", "天蝎座": "7",
                "射手座": "8", "魔羯座": "9", "水瓶座": "10", "双鱼座": "11"
            }
            
            # 获取星座ID
            xingzuo_id = xingzuo_map.get(xingzuo_name)
            if not xingzuo_id:
                return f"不支持的星座名称: {xingzuo_name}"

            # 调用星座运势API
            url = "https://api.yuanfenju.com/index.php/v1/Zhanbu/yunshi"
            params = {
                'api_key': api_key,
                'type': '0',  # 0代表星座
                'title_yunshi': xingzuo_id
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            # 添加超时设置和重试机制
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    response = requests.post(url, data=params, headers=headers, timeout=10)
                    break
                except requests.Timeout:
                    retry_count += 1
                    if retry_count == max_retries:
                        return "调用星座运势API超时，请稍后重试"
                    logger.warning(f"[GeweChatTask] 星座运势API调用超时，第{retry_count}次重试")
                    time.sleep(1)
                except requests.RequestException as e:
                    return f"调用星座运势API异常: {str(e)}"
            
            if response.status_code != 200:
                error_msg = f"调用星座运势API失败: HTTP {response.status_code}"
                logger.error(f"[GeweChatTask] {error_msg}")
                return error_msg

            try:
                data = response.json()
            except ValueError:
                error_msg = "解析星座运势API响应失败: 返回数据格式错误"
                logger.error(f"[GeweChatTask] {error_msg}")
                return error_msg

            if data.get('errcode') != 0:
                error_msg = f"获取星座运势失败: {data.get('errmsg', '未知错误')}"
                logger.error(f"[GeweChatTask] {error_msg}")
                return error_msg

            # 格式化返回信息
            result = data.get("data", {})
            if not result:
                return "获取星座运势失败: 返回数据为空"

            today_data = result.get("今日运势", {})
            
            # 获取当前日期
            current_date = datetime.now()

            # 转换分数为百分比
            scores = {
                "综合": int(float(today_data.get('综合分数', '0'))),
                "爱情": int(float(today_data.get('爱情分数', '0'))),
                "事业": int(float(today_data.get('事业分数', '0'))),
                "财富": int(float(today_data.get('财富分数', '0'))),
                "健康": int(float(today_data.get('健康分数', '0')))
            }

            return f"""🌟 {xingzuo_name}今日运势
{'='*22}
📅 日期：{current_date.day} / {current_date.month}月
💕 速配星座：{today_data.get('速配星座', '无')}
🔥 提防星座：{today_data.get('提防星座', '无')}
🎨 幸运颜色：{today_data.get('幸运颜色', '无')}
🔢 幸运数字：{today_data.get('幸运数字', '无')}
💎 幸运宝石：{today_data.get('幸运宝石', '无')}

✨ 运势评分：
📒 综合运势：{scores['综合']}分
💕 爱情运势：{scores['爱情']}分
💼 事业运势：{scores['事业']}分
💰 财富运势：{scores['财富']}分
🏃 健康运势：{scores['健康']}分

💫 运势解析：
❤️‍🔥 爱情运势：{today_data.get('爱情运势', '无')}

💼 事业运势：{today_data.get('事业运势', '无')}

💰 财富运势：{today_data.get('财富运势', '无')}

🏃 健康运势：{today_data.get('健康运势', '无')}
{'='*22}"""

        except Exception as e:
            error_msg = f"执行星座运势插件失败: {str(e)}"
            logger.error(f"[GeweChatTask] {error_msg}")
            return error_msg

    def _check_and_notify_balance(self):
        """检查API余额并在低于阈值时通知管理员"""
        try:
            # 检查是否配置了管理员昵称
            admin_nickname = self.plugin_config.get('admin_nickname')
            if not admin_nickname:
                logger.debug("[GeweChatTask] 未配置管理员昵称，无法发送余额不足提醒")
                return

            # 检查API余额
            remaining_times = self._check_api_balance()
            if remaining_times is None:  # 包年会员
                return
                
            # 获取阈值设置
            balance_threshold = self.plugin_config.get('balance_threshold', 20)
            
            # 如果余额低于阈值，发送提醒
            if remaining_times < balance_threshold:
                # 获取管理员的wxid
                admin_wxid = None
                
                # 先尝试从缓存中查找管理员
                if self.contacts_cache["contacts"]:
                    for wxid in self.contacts_cache["contacts"]:
                        try:
                            response = self._get_contact_brief_info([wxid])
                            if response and response.get('ret') == 200:
                                data = response.get('data', [])
                                if isinstance(data, list) and len(data) > 0:
                                    if data[0].get('nickName') == admin_nickname:
                                        admin_wxid = wxid
                                        break
                        except Exception as e:
                            logger.debug(f"[GeweChatTask] 从缓存中获取用户信息失败: {e}")
                            continue
                
                # 如果缓存中没找到，尝试实时获取
                if not admin_wxid:
                    contacts = self._get_contacts()
                    if contacts:
                        for wxid in contacts:
                            try:
                                response = self._get_contact_brief_info([wxid])
                                if response and response.get('ret') == 200:
                                    data = response.get('data', [])
                                    if isinstance(data, list) and len(data) > 0:
                                        if data[0].get('nickName') == admin_nickname:
                                            admin_wxid = wxid
                                            break
                            except Exception as e:
                                logger.debug(f"[GeweChatTask] 获取用户信息失败: {e}")
                                continue
                
                if not admin_wxid:
                    logger.debug(f"[GeweChatTask] 未找到管理员: {admin_nickname}")
                    return
                    
                # 发送提醒消息
                with self._message_lock:
                    try:
                        from lib.gewechat import GewechatClient
                    except ImportError as e:
                        logger.debug(f"[GeweChatTask] 导入GewechatClient失败: {e}")
                        return
                    
                    client = GewechatClient(
                        base_url=conf().get("gewechat_base_url"),
                        token=conf().get("gewechat_token")
                    )
                    
                    message = f"⚠️ API余额不足提醒\n{'='*22}\n当前剩余调用次数：{remaining_times}\n阈值设置：{balance_threshold}\n请及时充值以确保服务正常运行。"
                    
                    if not self._send_message_with_retry(
                        conf().get("gewechat_app_id"),
                        admin_wxid,
                        message
                    ):
                        logger.debug("[GeweChatTask] 发送API余额不足提醒失败")
                    else:
                        logger.debug("[GeweChatTask] 已发送API余额不足提醒")
                        
        except Exception as e:
            logger.debug(f"[GeweChatTask] 检查API余额并通知失败: {e}")
            # 异常不会影响主要功能的执行

    def _adjust_time_for_conflicts(self, time_str: str, circle_str: str, cursor) -> str:
        """调整时间避免冲突，返回调整后的时间"""
        try:
            # 如果是cron表达式，不需要调整
            if circle_str.startswith("cron["):
                return time_str
            
            # 从配置中获取时间调整间隔（分钟），默认为1分钟
            time_adjust_interval = self.plugin_config.get("time_adjust_interval", 1)
            logger.debug(f"[GeweChatTask] 时间调整间隔: {time_adjust_interval}分钟")
            
            # 解析原始时间
            hour, minute = time_str.replace('：', ':').split(':')
            hour = int(hour)
            minute = int(minute)
            
            # 获取所有任务的时间
            cursor.execute('SELECT time FROM tasks')
            existing_tasks = cursor.fetchall()
            logger.debug(f"[GeweChatTask] 当前所有任务时间: {existing_tasks}")
            
            # 检查是否存在冲突
            while True:
                current_time = f"{hour:02d}:{minute:02d}"
                if not any(task[0] == current_time for task in existing_tasks):
                    break
                
                # 时间冲突，按配置的间隔增加分钟数
                minute += time_adjust_interval
                if minute >= 60:
                    hour += (minute // 60)
                    minute = minute % 60
                if hour >= 24:
                    return None  # 无法调整
                
                logger.debug(f"[GeweChatTask] 尝试调整时间到: {hour:02d}:{minute:02d}")
            
            # 返回调整后的时间
            return f"{hour:02d}:{minute:02d}"
            
        except Exception as e:
            logger.error(f"[GeweChatTask] 调整时间失败: {e}")
            return None

    def _validate_cron_format(self, cron_exp):
        """验证 cron 表达式的基本格式"""
        try:
            parts = cron_exp.split()
            if len(parts) != 5:
                logger.debug(f"[GeweChatTask] cron表达式必须包含5个部分，当前: {len(parts)}")
                return False
            
            # 只验证格式，不验证具体值
            return True
        except Exception as e:
            logger.debug(f"[GeweChatTask] cron表达式格式验证失败: {e}")
            return False 

    def _validate_normal_format(self, circle_str, time_str):
        """验证普通定时任务的基本格式"""
        try:
            # 1. 验证时间格式 HH:mm
            if ':' not in time_str and '：' not in time_str:
                logger.debug(f"[GeweChatTask] 时间格式错误: {time_str}")
                return False, "时间格式错误，请使用 HH:mm 格式"
            
            # 2. 验证周期格式
            valid_circles = ["每天", "工作日", "今天", "明天", "后天"]
            if circle_str in valid_circles:
                return True, None
            
            # 每周几
            week_days = ["一", "二", "三", "四", "五", "六", "日"]
            if circle_str.startswith("每周"):
                day = circle_str[2:]
                if day in week_days:
                    return True, None
                return False, "每周后面必须是：一、二、三、四、五、六、日"
            
            # 具体日期 YYYY-MM-DD
            if len(circle_str) == 10:
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', circle_str):
                    return False, "日期格式错误，正确格式：YYYY-MM-DD"
                return True, None
            
            return False, "周期格式错误，支持：每天、每周x、工作日、YYYY-MM-DD、今天、明天、后天"
        except Exception as e:
            logger.debug(f"[GeweChatTask] 格式验证失败: {e}")
            return False, f"格式验证失败: {str(e)}" 

    def _is_valid_task_data(self, time_str, circle_str, event_str):
        """验证任务数据的有效性"""
        try:
            if circle_str.startswith("cron["):
                # 验证 cron 表达式
                cron_exp = circle_str[5:-1].strip()
                try:
                    croniter(cron_exp)
                    return True
                except:
                    return False
            else:
                # 验证时间格式
                is_valid, _ = self._validate_time_format(time_str)
                if not is_valid:
                    return False
                    
                # 验证周期格式
                if not self._validate_circle_format(circle_str):
                    return False
                    
            return True
        except:
            return False 

    def _check_api_balance(self) -> Optional[int]:
        """检查API余额
        Returns:
            Optional[int]: 剩余调用次数，如果是包年会员返回None
        """
        try:
            api_key = self.plugin_config.get('yuanfenju_api_key')
            if not api_key:
                logger.error("[GeweChatTask] 未配置缘分居API密钥")
                return None

            # 添加重试机制
            max_retries = MAX_RETRIES
            retry_count = 0
            retry_delay = RETRY_DELAY

            while retry_count < max_retries:
                try:
                    # 调用账户查询接口
                    response = requests.post(
                        "https://api.yuanfenju.com/index.php/v1/Free/querymerchant",
                        data={"api_key": api_key},
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=API_TIMEOUT
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('errcode') != 0:
                            logger.error(f"[GeweChatTask] API余额查询失败: {data.get('errmsg')}")
                            return None
                        
                        merchant_info = data.get('data', {})
                        remaining_times = merchant_info.get('merchant_remaining_call_times')
                        
                        # 如果是包年会员，返回None
                        if remaining_times == "--":
                            return None
                        
                        # 转换为整数
                        try:
                            return int(remaining_times)
                        except (ValueError, TypeError):
                            logger.error(f"[GeweChatTask] API余额格式错误: {remaining_times}")
                            return None
                    else:
                        logger.error(f"[GeweChatTask] API余额查询HTTP错误: {response.status_code}")
                        
                except requests.Timeout:
                    logger.warning(f"[GeweChatTask] API余额查询超时，第{retry_count + 1}次重试")
                except requests.RequestException as e:
                    logger.warning(f"[GeweChatTask] API余额查询请求异常: {e}，第{retry_count + 1}次重试")
                except Exception as e:
                    logger.error(f"[GeweChatTask] API余额查询异常: {e}")
                    return None

                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                
            logger.error("[GeweChatTask] API余额查询失败，已达到最大重试次数")
            return None
            
        except Exception as e:
            logger.error(f"[GeweChatTask] 检查API余额失败: {e}")
            return None

    def _init_db_pool(self):
        """初始化数据库连接池 - 使用线程本地存储"""
        try:
            self._thread_local.connection = None
            logger.info("[GeweChatTask] 数据库连接初始化完成，使用线程本地存储")
        except Exception as e:
            logger.error(f"[GeweChatTask] 初始化数据库连接失败: {e}")
            raise GeweChatTaskError(f"初始化数据库连接失败: {e}")

    def _get_db_connection(self):
        """获取当前线程的数据库连接"""
        try:
            if not hasattr(self._thread_local, 'connection') or self._thread_local.connection is None:
                self._thread_local.connection = sqlite3.connect(self.db_path, timeout=DB_CONNECTION_TIMEOUT)
                self._thread_local.connection.row_factory = sqlite3.Row
            return self._thread_local.connection
        except Exception as e:
            logger.error(f"[GeweChatTask] 获取数据库连接失败: {e}")
            raise GeweChatTaskError(f"获取数据库连接失败: {e}")

    def _return_db_connection(self, conn):
        """关闭数据库连接"""
        try:
            if conn and conn == self._thread_local.connection:
                # 不要真的关闭连接，保持线程本地连接
                pass
            elif conn:
                conn.close()
        except Exception as e:
            logger.error(f"[GeweChatTask] 关闭数据库连接失败: {e}")
            try:
                if conn:
                    conn.close()
            except:
                pass

    def _get_task_lock(self, task_id):
        """获取任务级别的锁"""
        with self._scheduler_lock:
            if task_id not in self._task_locks:
                self._task_locks[task_id] = threading.Lock()
            return self._task_locks[task_id]

    def _clean_task_locks(self):
        """清理不再使用的任务锁"""
        conn = None
        try:
            conn = self._get_db_connection()
            if not conn:
                return
                
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM tasks')
            active_tasks = {row[0] for row in cursor.fetchall()}
            
            with self._scheduler_lock:
                for task_id in list(self._task_locks.keys()):
                    if task_id not in active_tasks:
                        del self._task_locks[task_id]
            
            logger.debug(f"[GeweChatTask] 已清理任务锁，当前活动任务数: {len(self._task_locks)}")
        except Exception as e:
            logger.error(f"[GeweChatTask] 清理任务锁失败: {e}")
        finally:
            if conn:
                self._return_db_connection(conn)

    def _check_config_update(self):
        """检查配置文件是否更新"""
        try:
            config_path = os.path.join(os.path.dirname(__file__), "config.json")
            if os.path.exists(config_path):
                current_mtime = os.path.getmtime(config_path)
                if current_mtime > self._config_last_modified:
                    self._load_plugin_config()
                    self._config_last_modified = current_mtime
                    logger.debug("[GeweChatTask] 配置文件已更新")
        except Exception as e:
            logger.error(f"[GeweChatTask] 检查配置文件更新失败: {e}")
