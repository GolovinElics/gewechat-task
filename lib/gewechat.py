import requests
import logging
from typing import Dict, Any, Optional
from GeweChatTask import GeweChatTask  # 更新导入语句

logger = logging.getLogger(__name__)

class GewechatClient:
    """GeWechat API 客户端"""
    
    def __init__(self, base_url: str, token: str):
        """
        初始化 GeWechat 客户端
        
        Args:
            base_url: API 基础 URL
            token: API 访问令牌
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.headers = {
            "X-GEWE-TOKEN": token,
            "Content-Type": "application/json"
        }
        
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
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
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
            
    def fetch_contacts_list(self, app_id: str) -> Dict[str, Any]:
        """
        获取联系人列表
        
        Args:
            app_id: 应用 ID
            
        Returns:
            Dict[str, Any]: 包含联系人列表的响应数据
        """
        data = {"appId": app_id}
        response = self._make_request("POST", "/contacts/fetchContactsList", data)
        return response or {"ret": 500, "msg": "获取联系人列表失败"}
        
    def get_chatroom_info(self, device_id: str, chatroom_id: str) -> Dict[str, Any]:
        """
        获取群聊信息
        
        Args:
            device_id: 设备 ID
            chatroom_id: 群聊 ID
            
        Returns:
            Dict[str, Any]: 包含群聊信息的响应数据
        """
        data = {
            "uuid": device_id,
            "chatroomId": chatroom_id
        }
        response = self._make_request("POST", "/chatroom/getChatroomInfo", data)
        return response or {"ret": 500, "msg": "获取群聊信息失败"}
        
    def post_text(self, app_id: str, to_wxid: str, content: str) -> Dict[str, Any]:
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
        response = self._make_request("POST", "/message/sendText", data)
        return response or {"ret": 500, "msg": "发送消息失败"}

    def get_self_info(self, app_id: str) -> Dict[str, Any]:
        """
        获取个人资料信息
        
        Args:
            app_id: 应用 ID
            
        Returns:
            Dict[str, Any]: 包含个人资料的响应数据
        """
        data = {"appId": app_id}
        response = self._make_request("POST", "/personal/getProfile", data)
        return response or {"ret": 500, "msg": "获取个人资料失败"} 