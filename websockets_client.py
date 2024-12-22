"""
Author: MaJie
Date: 2024/12/22 17:56
Description:
编写了python版本的websockets客户端模板代码, 提供了如下功能:
   1. 日志记录.
   2. 连接超时判断
   3. 使用orjson替代json进行json与字符串的格式转换
   4. 心跳发送机制
   5. 显示ack响应机制
   6. 自动重连机制
"""
import asyncio
import copy
import uuid
from typing import Union, Optional
import websockets
import orjson
from loguru import logger
from websockets import ConnectionClosed
from websockets.frames import Close

# 创建日志记录客户端操作
__log_dir = f'./logs/WebsocketsClient'
__log_name = 'SocketClient'
logger.add(
    f'{__log_dir}/' + '{time:YYYY-MM-DD}.log',
    filter=lambda record: record["extra"]["name"] == f'{__log_name}',
    format='{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}',
    rotation='00:00',
    retention='30 days'
)
websockets_client_logger = logger.bind(name=f'{__log_name}')

class WebsocketClient:
    def __init__(self, ip, port):
        self.__ip_address = ip
        self.__port = port
        self.__websockets_connection = None
        self.__connect_error_sleep_time = 5  # 连接失败休眠时间
        self.__use_heart_thread = True  # 是否启动心跳线程
        self.__heartbeat_interval = 1  # 心跳发送间隔
        self.__heartbeat_task = None  # 心跳发送线程
        self.__use_ack_check = True  # 是否启动ack检查
        self.__ack_message_manage = {}  # ack消息管理字典
        self.__ack_message_max_len = 100000  # ack消息管理字典最多存储多少条消息
        self.__ack_lock = asyncio.Lock()  # 更新和删除消息字典内容时，必须要加锁
        self.__ack_check_interval = 1  # ack检查机制
        self.__ack_check_task = None  # ack检查线程
        self.__ack_timeout = 3  # ack消息的超时时间，默认3s
        self.client_id = ''  # 客户端唯一id
        self._is_reconnect_lock = asyncio.Lock()
        self._is_reconnect = False  # 连接断开标志

    async def set_heart_time_out(self, heart_interval: Union[int, float]) -> None:
        """
        设置心跳发送间隔，默认4秒
        :param heart_interval:
        :return: None
        """
        if not isinstance(heart_interval, (int, float)) or heart_interval <= 0:
            raise ValueError("timeout must be an int or float and greater than 0.")
        self.__heartbeat_interval = heart_interval

    async def set_use_heart_thread(self, use_heart_thread: bool) -> None:
        """
        设置是否启动心跳发送线程，默认启动
        :return: None
        """
        if isinstance(use_heart_thread, bool):
            self.__use_heart_thread = use_heart_thread
        else:
            raise ValueError("reuse_flag must be a boolean")

    async def set_use_ack_check(self, use_ack_check: bool) -> None:
        """
        设置是否启动心跳发送线程，默认不启动
        :return: None
        """
        if isinstance(use_ack_check, bool):
            self.__use_ack_check = use_ack_check
        else:
            raise ValueError("use ack check must be a boolean")

    async def set_ack_message_timeout(self, ack_timeout: Union[int, float]) -> None:
        """
        设置ack超时时间
        :return: None
        """
        if isinstance(ack_timeout, bool):
            self.__ack_timeout = ack_timeout
        else:
            raise ValueError("ack timeout must be int or float")

    async def _send_heart_task(self) -> None:
        """
        心跳发送任务，定期往所有客户端依次发送心跳，默认0.1秒发送一次
        :return: None
        """
        while True:
            try:
                if self.__websockets_connection is None:
                    await asyncio.sleep(self.__heartbeat_interval)
                    continue
                heartbeat_message = {"message": {'client_id': self.client_id, "type": "heartbeat", "params": {}},
                                     "message_id": '', 'ack': ''}
                await self.send_message(heartbeat_message)
                await asyncio.sleep(self.__heartbeat_interval)
            except Exception as e:
                async with self._is_reconnect_lock:
                    self._is_reconnect = True

    async def _check_ack_task(self) -> None:
        """
        ack检查任务, 定期检查ack字典, 对其中的消息进行超时机制处理
        :return: None
        """

        while True:
            try:
                need_del_key_list = []
                # 获取ack字典
                async with self.__ack_lock:
                    now_ack_dict = copy.deepcopy(self.__ack_message_manage)
                # 校验ack字典
                now_ack_dict_len = await self._dict_length_generator(now_ack_dict)
                if now_ack_dict_len == 0:
                    await asyncio.sleep(self.__ack_check_interval)
                    continue
                if now_ack_dict_len > self.__ack_message_max_len:
                    async with self.__ack_lock:
                        self.__ack_message_manage.clear()
                # 遍历ack字典进行超时处理
                for message_id, data_dict in now_ack_dict.items():
                    if data_dict['time_out'] <= 0:
                        send_message = data_dict['message']
                        try:
                            if self.__websockets_connection is not None:
                                await self.send_message(send_message)
                            data_dict['time_out'] = data_dict['original_time']
                        except Exception as e:
                            need_del_key_list.append(message_id)
                            async with self._is_reconnect_lock:
                                self._is_reconnect = True
                    else:
                        data_dict['time_out'] -= 1

                # 进行回收处理
                for del_key in need_del_key_list:
                    if now_ack_dict.get(del_key) is not None:
                        del now_ack_dict[del_key]

                async with self.__ack_lock:
                    for del_key in need_del_key_list:
                        if self.__ack_message_manage.get(del_key) is not None:
                            del self.__ack_message_manage[del_key]
                    self.__ack_message_manage.update(now_ack_dict)

                await asyncio.sleep(self.__ack_check_interval)
            except Exception as e:
                websockets_client_logger.error(f"Manage ack message occur error: {e}")
                await asyncio.sleep(self.__ack_check_interval)

    async def _generate_ack_message(self, message: dict, client_id: str) -> dict:
        """
        生成ack字典消息
        """
        temp_message = copy.deepcopy(message)
        ack_message = {'time_out': self.__ack_timeout, 'original_time': self.__ack_timeout, "message": temp_message, 'ack': ''}
        return ack_message


    async def _generate_send_ack_message(self, message: dict, message_id: str) -> dict:
        """
        生成ack发送消息
        """
        ack_message = {"client_id": self.client_id, "message": message, "message_id": message_id, 'ack': ''}
        return ack_message

    async def _generate_response_ack_message(self, message: dict, message_id: str) -> dict:
        """
        生成ack回复消息
        """
        ack_message = {"client_id": self.client_id, "message": message, "message_id": '', "ack": message_id}
        return ack_message

    async def _put_ack_message(self, message: dict, message_id: str, client_id: str) -> None:
        """
        将ack消息加入到ack字典中
        """
        ack_message = await self._generate_ack_message(message, client_id)
        async with self.__ack_lock:
            self.__ack_message_manage[message_id] = ack_message

    async def _del_ack_dict(self, message: dict) -> None:
        try:
            if message.get('ack') is None:
                return
            if message['ack'] == '':
                return
            async with self.__ack_lock:
                if self.__ack_message_manage.get(message['ack']) is not None:
                    del self.__ack_message_manage[message['ack']]
        except Exception as e:
            websockets_client_logger.warning(f"Delete ack message occur error: {e}")

    async def _send_ack_message(self, message: dict, message_id: str, client_id: str) -> None:
        """
        主动发送ack消息
        """
        ack_send_message = await self._generate_send_ack_message(message, message_id)
        await self.send_message(ack_send_message)

    async def _response_ack_message(self, message) -> None:
        """
        回应ack消息
        """
        try:
            if message.get('message_id') is None:
                return
            if message['message_id'] == '':
                return
            ack_res_message = await self._generate_response_ack_message(message, message['message_id'])
            await self.send_message(ack_res_message)
        except Exception as e:
            websockets_client_logger.warning(f"Response ack message occur error: {e}")

    @staticmethod
    async def _dict_length_generator(data_dict: dict) -> int:
        return sum(1 for _, _ in data_dict.items())

    async def _start_send_heart_task(self) -> None:
        """
        启动发送心跳的线程
        :return: None
        """
        if self.__use_heart_thread:
            self.__heartbeat_task = asyncio.create_task(self._send_heart_task())

    async def _start_ack_check_task(self) -> None:
        """
        启动ack检查
        :return: None
        """
        if self.__use_ack_check:
            self.__ack_check_task = asyncio.create_task(self._check_ack_task())

    async def _generate_message_id(self) -> str:
        """
        生成唯一的消息id
        """
        while True:
            message_id = str(uuid.uuid4())
            async with self.__ack_lock:
                if self.__ack_message_manage.get(message_id) is None:
                    return message_id

    async def _check_connection_status(self):
        async with self._is_reconnect_lock:
            if self._is_reconnect:
                self._is_reconnect = False
                close_info = Close(1006, "Connection closed unexpectedly.")
                raise websockets.exceptions.ConnectionClosed(close_info, sent=None)

    async def send_message(self, message: dict) -> None:
        if self.__websockets_connection is None:
            return None
        message_str = orjson.dumps(message)
        await self.__websockets_connection.send(message_str)


    async def recv_message(self) -> Optional[dict]:
        """接收消息"""
        try:
            message_str = await asyncio.wait_for(self.__websockets_connection.recv(), timeout=0.1)
            message = orjson.loads(message_str)
            return message
        except asyncio.TimeoutError:
            return None
        except websockets.exceptions.ConnectionClosed as e:
            close_info = Close(1006, "Connection closed unexpectedly.")
            raise ConnectionClosed(None, sent=close_info)
        except Exception as e:
            return None

    async def _handle_message(self, message: dict) -> None:
        if message['message']['type'] == 'connect':
            self.client_id = message['message']['params']['client_id']
        else:
            pass
    
    async def connect_websocket(self):
        uri = f"ws://{self.__ip_address}:{self.__port}"
        await self._start_send_heart_task()
        await self._start_ack_check_task()
        try:
            while True:
                try:
                    async with websockets.connect(uri) as websocket:
                        self.__websockets_connection = websocket
                        websockets_client_logger.info(f"Connect to websocket server successfully!")
                        while True:
                            recv_message = await self.recv_message()  # 设置超时
                            await self._check_connection_status()
                            if recv_message:
                                websockets_client_logger.info(f"Rec message: {recv_message}")
                                await self._del_ack_dict(recv_message)
                                await self._response_ack_message(recv_message)
                                await self._handle_message(recv_message)
                except Exception as e:
                    websockets_client_logger.error(f"Connection occur error: {e}")
                    self.__websockets_connection = None
                    await asyncio.sleep(self.__connect_error_sleep_time)
        except Exception as e:
            websockets_client_logger.error(f"Client occur error: {e}")
            if self.__heartbeat_task is not None:
                self.__heartbeat_task.cancel()
                self.__heartbeat_task = None
            if self.__ack_check_task is not None:
                self.__ack_check_task.cancel()
                self.__ack_check_task = None

if __name__ == "__main__":
    client_a = WebsocketClient('localhost', 8004)
    asyncio.run(client_a.connect_websocket())