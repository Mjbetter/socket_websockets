"""
Author: MaJie
Date: 2024/12/22 17:56
Description:
编写了python版本的websockets服务器模板代码, 提供了如下功能:
   1. 日志记录.
   2. 连接释放
   3. 使用orjson替代json进行json与字符串的格式转换
   4. 心跳发送机制
   5. 显示ack响应机制
   6. 一对多机制
"""
import asyncio
import copy
import json
import uuid
from typing import Union
import websockets
import orjson
from loguru import logger

__log_dir = f'./logs/SocketServer'
__log_name = 'SocketServer'
# 创建不同级别的日志
logger.add(
    f'{__log_dir}/' + '{time:YYYY-MM-DD}.log',
    filter=lambda record: record["extra"]["name"] == f'{__log_name}',
    format='{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}',
    rotation='00:00',
    retention='30 days'
)
websockets_server_logger = logger.bind(name=f'{__log_name}')

class WebsocketsServer:
    def __init__(self, ip: str, port: int):
        self.__ip_address = ip
        self.__ip_port = port
        self.__clients = {}
        self.__client_ids = {}  # 用于存储客户端id， 每个socket对应一个uuid
        self.__client_lock = asyncio.Lock()
        self.__use_heart_thread = True  # 是否启动心跳线程
        self.__heartbeat_interval = 2  # 心跳发送间隔
        self.__heartbeat_task = None
        self.__use_ack_check = True  # 是否启动ack检查
        self.__ack_message_manage = {}  # ack消息管理字典
        self.__ack_message_max_len = 100000  # ack消息管理字典最多存储多少条消息
        self.__ack_lock = asyncio.Lock()  # 更新和删除消息字典内容时，必须要加锁
        self.__ack_check_interval = 1  # ack检查机制
        self.__ack_check_task = None  # ack检查线程
        self.__ack_timeout = 3  # ack消息的超时时间，默认3s

    def set_heart_time_out(self, heart_interval: Union[int, float]) -> None:
        """
        设置心跳发送间隔，默认4秒
        :param heart_interval:
        :return: None
        """
        if not isinstance(heart_interval, (int, float)) or heart_interval <= 0:
            raise ValueError("heart timeout must be an int or float and greater than 0.")
        self.__heartbeat_interval = heart_interval

    def set_use_heart_thread(self, use_heart_thread: bool) -> None:
        """
        设置是否启动心跳发送线程，默认不启动
        :return: None
        """
        if isinstance(use_heart_thread, bool):
            self.__use_heart_thread = use_heart_thread
        else:
            raise ValueError("use heart thread must be a boolean")

    def set_use_ack_check(self, use_ack_check: bool) -> None:
        """
        设置是否启动心跳发送线程，默认不启动
        :return: None
        """
        if isinstance(use_ack_check, bool):
            self.__use_ack_check = use_ack_check
        else:
            raise ValueError("use ack check must be a boolean")

    def set_ack_message_timeout(self, ack_timeout: Union[int, float]) -> None:
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
        心跳发送任务，定期往所有客户端依次发送心跳，默认2秒发送一次
        :return: None
        """
        while True:
            try:
                need_delete_client_id = []
                async with self.__client_lock:
                    now_clients = dict(self.__clients)
                for client_id, client_websocket in now_clients.items():
                    try:
                        heartbeat_message = {"message": {"type": "heartbeat", "params": {}}, "message_id": '', 'ack': ''}
                        await self._send_message(client_websocket, heartbeat_message)
                    except Exception as e:
                        need_delete_client_id.append(client_id)
                if len(need_delete_client_id) > 0:
                    for client_id in need_delete_client_id:
                        async with self.__client_lock:
                            if self.__clients.get(client_id) is not None:
                                del self.__clients[client_id]
                await asyncio.sleep(self.__heartbeat_interval)
            except Exception as e:
                websockets_server_logger.warning(f"Send heart to client occur error: {e}")
                await asyncio.sleep(self.__heartbeat_interval)

    async def _check_ack_task(self) -> None:
        """
        ack检查任务, 定期检查ack字典, 对其中的消息进行超时机制处理
        :return: None
        """
        try:
            while True:
                need_del_client_id_list = []
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
                        client_id = data_dict['client_id']
                        send_message = data_dict['message']
                        try:
                            async with self.__client_lock:
                                if self.__clients.get(client_id) is not None:
                                    client_connection = self.__clients[client_id]
                                else:
                                    client_connection = None
                            if client_connection is None:
                                need_del_key_list.append(message_id)
                                continue
                            await self._send_message(client_connection, send_message)
                            data_dict['time_out'] = data_dict['original_time']
                        except Exception as e:
                            need_del_client_id_list.append(client_id)
                            need_del_key_list.append(message_id)
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

                async with self.__client_lock:
                    for client_id in need_del_client_id_list:
                        if self.__clients.get(client_id) is not None:
                            del self.__clients[client_id]

                await asyncio.sleep(self.__ack_check_interval)
        except Exception as e:
            websockets_server_logger.error(f"Manage ack message occur error: {e}")

    async def _generate_ack_message(self, message: dict, client_id: str) -> dict:
        """
        生成ack字典消息
        """
        temp_message = copy.deepcopy(message)
        ack_message = {'time_out': self.__ack_timeout, 'original_time': self.__ack_timeout, "message": temp_message,
                       'client_id': client_id, 'ack': ''}
        return ack_message

    @staticmethod
    async def _generate_send_ack_message(message: dict, message_id: str) -> dict:
        """
        生成ack发送消息
        """
        ack_message = {"message": message, "message_id": message_id, 'ack': ''}
        return ack_message

    @staticmethod
    async def _generate_response_ack_message(message: dict, message_id: str) -> dict:
        """
        生成ack回复消息
        """
        ack_message = {"message": message, "message_id": '', "ack": message_id}
        return ack_message

    async def _put_ack_message(self, message: dict, message_id: str, client_id: str) -> None:
        """
        将ack消息加入到ack字典中
        """
        ack_message = await self._generate_ack_message(message, client_id)
        async with self.__ack_lock:
            self.__ack_message_manage[message_id] = ack_message

    async def _del_ack_dict(self, message: dict) -> None:
        if message.get('ack') is None:
            return
        if message['ack'] == '':
            return
        async with self.__ack_lock:
            if self.__ack_message_manage.get(message['ack']) is not None:
                del self.__ack_message_manage[message['ack']]

    async def _send_ack_message(self, message: dict, message_id: str, client_id: str) -> None:
        """
        主动发送ack消息
        """
        ack_send_message = await self._generate_send_ack_message(message, message_id)
        async with self.__client_lock:
            client_socket = self.__clients[client_id]
        await self._send_message(client_socket, ack_send_message)

    async def _response_ack_message(self, message, client_id) -> None:
        """
        回应ack消息
        """
        if message.get('message_id') is None:
            return
        if message['message_id'] == '':
            return
        ack_res_message = await self._generate_response_ack_message(message, message['message_id'])
        with self.__client_lock:
            client_socket = self.__clients[client_id]
        await self._send_message(client_socket, ack_res_message)

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

    @staticmethod
    async def _recv_message(websocket: websockets.WebSocketServerProtocol) -> dict:
        recv_message_str = await websocket.recv()
        recv_message_dic = json.loads(recv_message_str)
        return recv_message_dic

    @staticmethod
    async def _send_message(websocket: websockets.WebSocketServerProtocol, message: dict) ->None:
        message_str = json.dumps(message)
        await websocket.send(message_str)

    async def _generate_client_id(self) -> str:
        """
        生成唯一的客户端id
        """
        while True:
            client_id = str(uuid.uuid4())
            if self.__clients.get(client_id) is None:
                return client_id

    async def _generate_message_id(self) -> str:
        """
        生成唯一的消息id
        """
        while True:
            message_id = str(uuid.uuid4())
            async with self.__ack_lock:
                if self.__ack_message_manage.get(message_id) is None:
                    return message_id

    async def _handle_client_message(self, websocket: websockets.WebSocketServerProtocol, message: dict) -> None:
        pass

    async def _handle_request(self, websocket: websockets.WebSocketServerProtocol):
        client_id = await self._generate_client_id()
        try:
            websockets_server_logger.info(f"Client {client_id} connect to server!")
            self.__client_ids[websocket] = client_id
            async with self.__client_lock:
                self.__clients[client_id] = websocket
            message_id = await self._generate_message_id()
            message = {'type': 'connect', 'params': {'client_id': client_id}}
            await self._send_ack_message(message, message_id, client_id)
            await self._put_ack_message(message, message_id, client_id)
            async for message in websocket:
                message_dic = orjson.loads(message)
                websockets_server_logger.info(f"Rec message: {message_dic}")
                await self._del_ack_dict(message_dic)
                await self._response_ack_message(message_dic, client_id)
                await self._handle_client_message(websocket, message_dic)
        except websockets.exceptions.ConnectionClosed as e:
            if e.code == 1000:
                websockets_server_logger.warning(f"Connection with {client_id} closed normally (1000).")
            elif e.code == 1001:
                websockets_server_logger.warning(f"Connection with {client_id} closed (Going Away, 1001).")
            elif e.code == 1006:
                websockets_server_logger.warning(f"Connection with {client_id} closed unexpectedly (1006).")
            elif e.code == 1011:
                websockets_server_logger.warning(f"Connection with {client_id} closed due to server error (1011).")
            else:
                websockets_server_logger.warning(f"Connection with {client_id} closed with code {e.code}.")

        except Exception as e:
            websockets_server_logger.warning(f"Connection of {client_id} occur error:{e}")
        finally:
            # 清除资源
            async with self.__client_lock:
                if self.__clients.get(client_id) is not None:
                    del self.__clients[client_id]

    async def start_server(self):
        try:
            websockets_server_logger.info("Start to run websockets server, waiting client to connect.")
            await self._start_send_heart_task()
            await self._start_ack_check_task()
            async with websockets.serve(self._handle_request, self.__ip_address, self.__ip_port):
                try:
                    await asyncio.Future()  # 永不结束，保持服务器运行
                except asyncio.CancelledError:
                    websockets_server_logger.error("WebSocket server stopped.")
        except Exception as e:
            websockets_server_logger.error(f"Start websockets server failed, occur error: {e}!")
        finally:
            if self.__ack_check_task is not None:
                self.__ack_check_task.cancel()
                self.__ack_check_task = None
            if self.__heartbeat_task is not None:
                self.__heartbeat_task.cancel()
                self.__heartbeat_task = None


if __name__ == "__main__":
    websockets_server = WebsocketsServer('0.0.0.0', 8004)
    asyncio.run(websockets_server.start_server())