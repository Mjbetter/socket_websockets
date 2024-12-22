"""
Author: MaJie
Date: 2024/12/18 16:00
Description:
编写了python版本的socket客户端模板代码, 提供了如下功能:
   1. 日志记录.
   2. 连接超时判断
   3. 使用orjson替代json进行json与字符串的格式转换
   4. 消息通过头部标识, 消息长度, 尾部标识来防止粘包
   5. 心跳发送机制
   6. 显示ack响应机制
   7. 自动重连机制
"""
import copy
import socket
import select
import threading
import time
from typing import List, Union, Optional
import errno
import orjson
from loguru import logger

# 创建日志记录客户端操作
__log_dir = f'./logs/SocketClient'
__log_name = 'SocketClient'
logger.add(
    f'{__log_dir}/' + '{time:YYYY-MM-DD}.log',
    filter=lambda record: record["extra"]["name"] == f'{__log_name}',
    format='{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}',
    rotation='00:00',
    retention='30 days'
)
socket_client_logger = logger.bind(name=f'{__log_name}')


class SocketClient:
    def __init__(self, server_address: tuple, timeout: float = 0.1):
        """
        初始化客户端，设置默认的服务器地址和超时时间
        :param server_address: 服务器地址(IP, 端口)
        :param timeout: 客户端连接超时时间, 默认为4秒
        """
        self.__server_address = server_address
        self.__transmit_type = socket.SOCK_STREAM  # 默认使用TCP协议
        self.__family = socket.AF_INET  # 默认使用IPv4协议族
        self.__timeout = timeout
        self.__client_socket = None
        self.__connect_error_sleep_time = 5  # 连接失败休眠时间
        self.__HEADER = b"HEADER"
        self.__FOOTER = b"FOOTER"
        self.__message_len = 7
        self.__use_heart_thread = True  # 是否启动心跳线程
        self.__heartbeat_interval = 1  # 心跳发送间隔
        self.__heartbeat_thread = threading.Thread(target=self._send_heart_task, daemon=True)  # 心跳发送线程
        self.__use_ack_check = True  # 是否启动ack检查
        self.__ack_message_manage = {}  # ack消息管理字典
        self.__ack_message_max_len = 100000  # ack消息管理字典最多存储多少条消息
        self.__ack_lock = threading.Lock()  # 更新和删除消息字典内容时，必须要加锁
        self.__ack_check_interval = 1  # ack检查机制
        self.__ack_check_thread = threading.Thread(target=self._check_ack_task, daemon=True)  # ack检查线程
        self.__ack_timeout = 3  # ack消息的超时时间，默认3s
        self.client_id = ''  # 客户端唯一id
        self._is_reconnect_lock = threading.Lock()
        self._is_reconnect = False  # 连接断开标志
    
    def set_transmit_type(self, transmit_type: int) -> None:
        """
        设置套接字类型, 默认soket.SOCK_STREAM(1)代表TCP, socket.SOCK_DGRAM(2)代表UDP
        :param transmit_type: 套接字类型
        :return: None
        """
        if isinstance(transmit_type, int) and transmit_type in [socket.SOCK_STREAM, socket.SOCK_DGRAM]:
            self.__transmit_type = transmit_type
        else:
            raise ValueError("transmit_type must be socket.SOCK_STREAM or socket.SOCK_DGRAM (int type)")

    def set_family(self, family: int) -> None:
        """
        设置协议族, 默认socket.AF_INET(IPV4)
        :param family: 协议类型
        :return: None
        """
        if isinstance(family, int) and family in [socket.AF_INET, socket.AF_INET6]:
            self.__family = family
        else:
            raise ValueError("family must be socket.AF_INET or socket.AF_INET6 (int type)")

    def set_heart_time_out(self, heart_interval: Union[int, float]) -> None:
        """
        设置心跳发送间隔，默认4秒
        :param heart_interval:
        :return: None
        """
        if not isinstance(heart_interval, (int, float)) or heart_interval <= 0:
            raise ValueError("timeout must be an int or float and greater than 0.")
        self.__heartbeat_interval = heart_interval

    def set_use_heart_thread(self, use_heart_thread: bool) -> None:
        """
        设置是否启动心跳发送线程，默认启动
        :return: None
        """
        if isinstance( use_heart_thread, bool):
            self.__use_heart_thread = use_heart_thread
        else:
            raise ValueError("reuse_flag must be a boolean")

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

    def _send_heart_task(self) -> None:
        """
        心跳发送任务，定期往所有客户端依次发送心跳，默认0.1秒发送一次
        :return: None
        """
        while True:
            try:
                if self.__client_socket is None:
                    time.sleep(self.__heartbeat_interval)
                    continue
                heartbeat_message = {"message": {'client_id': self.client_id, "type": "heartbeat", "params": {}}, "message_id": '', 'ack': ''}
                self.send_message(orjson.dumps(heartbeat_message))
                time.sleep(self.__heartbeat_interval)
            except Exception as e:
                with self._is_reconnect_lock:
                    self._is_reconnect = True

    def _start_send_heart_thread(self) -> None:
        """
        启动发送心跳的线程
        :return: None
        """
        if self.__use_heart_thread:
            self.__heartbeat_thread.start()

    def _check_ack_task(self) -> None:
        """
        ack检查任务, 定期检查ack字典, 对其中的消息进行超时机制处理
        :return: None
        """
        try:
            while True:
                need_del_key_list = []
                # 获取ack字典
                with self.__ack_lock:
                    now_ack_dict = copy.deepcopy(self.__ack_message_manage)
                # 校验ack字典
                now_ack_dict_len = self._dict_length_generator(now_ack_dict)
                if now_ack_dict_len == 0:
                    time.sleep(self.__ack_check_interval)
                    continue
                if now_ack_dict_len > self.__ack_message_max_len:
                    with self.__ack_lock:
                        self.__ack_message_manage.clear()
                # 遍历ack字典进行超时处理
                for message_id, data_dict in now_ack_dict.items():
                    if data_dict['time_out'] <= 0:
                        send_message = data_dict['message']
                        try:
                            if self.__client_socket is not None:
                                self.send_message(orjson.dumps(send_message))
                            data_dict['time_out'] = data_dict['original_time']
                        except Exception as e:
                            need_del_key_list.append(message_id)
                            with self._is_reconnect_lock:
                                self._is_reconnect = True
                    else:
                        data_dict['time_out'] -= 1

                # 进行回收处理
                for del_key in need_del_key_list:
                    if now_ack_dict.get(del_key) is not None:
                        del now_ack_dict[del_key]

                with self.__ack_lock:
                    for del_key in need_del_key_list:
                        if self.__ack_message_manage.get(del_key) is not None:
                            del self.__ack_message_manage[del_key]
                    self.__ack_message_manage.update(now_ack_dict)

                time.sleep(self.__ack_check_interval)
        except Exception as e:
            socket_client_logger.error(f"Manage ack message occur error: {e}")

    def _generate_ack_message(self, message: dict) -> dict:
        """
        生成ack字典消息
        """
        temp_message = copy.deepcopy(message)
        ack_message = {'time_out': self.__ack_timeout, 'original_time': self.__ack_timeout, "message": temp_message, 'ack': ''}
        return ack_message

    def _generate_send_ack_message(self, message: dict, message_id: str) -> dict:
        """
        生成ack发送消息
        """
        ack_message = {"client_id": self.client_id, "message": message, "message_id": message_id, 'ack': ''}
        return ack_message

    def _generate_response_ack_message(self, message: dict, message_id: str) -> dict:
        """
        生成ack回复消息
        """
        ack_message = {"client_id": self.client_id, "message": message, "ack": message_id}
        return ack_message

    def _put_ack_message(self, message: dict, message_id: str) -> None:
        """
        将ack消息加入到ack字典中
        """
        ack_message = self._generate_ack_message(message)
        with self.__ack_lock:
            self.__ack_message_manage[message_id] = ack_message

    def _del_ack_dict(self, message: dict) -> None:
        if message.get('ack') is None:
            return
        if message['ack'] == '':
            return
        with self.__ack_lock:
            if self.__ack_message_manage.get(message['ack']) is not None:
                del self.__ack_message_manage[message['ack']]

    def _send_ack_message(self, message: dict, message_id: str, client_id: str) -> None:
        """
        主动发送ack消息
        """
        if message.get('message_id') is None:
            return
        if message['message_id'] == '':
            return
        ack_send_message = self._generate_send_ack_message(message, message_id)
        self.send_message(orjson.dumps(ack_send_message))

    def _response_ack_message(self, message) -> None:
        """
        回应ack消息
        """
        if message.get('message_id') is None:
            return
        if message['message_id'] == '':
            return
        ack_res_message = self._generate_response_ack_message(message, message['message_id'])
        self.send_message(orjson.dumps(ack_res_message))

    @staticmethod
    def _dict_length_generator(data_dict: dict) -> int:
        return sum(1 for _ in data_dict)

    def send_message(self, message: bytes) -> bool:
        """
        发送消息，格式：头部 + 消息长度(7) + 消息内容 + 尾部
        :param message: 需要发送的消息，字节类型
        :return: None
        """
        # 将消息长度转换为固定长度的字节数组(7位)，并在不足长度时使用前置零进行填充
        length_bytes = str(len(message)).zfill(self.__message_len).encode('utf-8')
        message_with_length = length_bytes + message
        all_message = self.__HEADER + message_with_length + self.__FOOTER
        self.__client_socket.sendall(all_message)
        return True

    def _recv_fixed_len_message(self, message_len: int) -> Optional[bytes]:
        """
        接收指定长度的消息。
        :param message_len: 要接收的消息长度
        :return: 接收到的字节消息，如果接收失败返回 None
        """
        buffer = b""
        while len(buffer) < message_len:
            data_byte = self.__client_socket.recv(message_len - len(buffer))
            if not data_byte:
                return None
            buffer += data_byte
        return buffer

    def recv_message(self) -> List[Union[bool, Union[Optional[str], dict]]]:
        """
        接收并解析消息，校验头部、尾部和消息长度。
        :return: 消息内容（解析后的字典），如果有错误，返回包含错误信息的字典
        """
        # 校验头部
        buffer_header = self._recv_fixed_len_message(len(self.__HEADER))
        if buffer_header == '' or buffer_header is None:
            return [False, None]
        if buffer_header != self.__HEADER:
            return [False, f"Header is not available, got {buffer_header}, expected {self.__HEADER}"]

        # 接收固定长度的长度字段
        length_bytes = self._recv_fixed_len_message(self.__message_len)
        if not length_bytes:
            return [False, f"Message len is not available, got {length_bytes}, expected {self.__message_len}"]

        # 解析消息长度
        message_length = int(length_bytes.decode('utf-8'))
        buffer_message = self._recv_fixed_len_message(message_length)
        if not buffer_message:
            return [False, f"buffer is not available, got {buffer_message}"]

        # 校验尾部
        buffer_footer = self._recv_fixed_len_message(len(self.__FOOTER))
        if buffer_footer != self.__FOOTER:
            return [False, f"Footer is not available, got {buffer_footer}, expected {self.__FOOTER}"]

        # 使用 orjson 解析消息
        return [True, orjson.loads(buffer_message)]
    
    def check_connect_status(self, connect_result):
        if connect_result == errno.ETIMEDOUT:
            socket_client_logger.warning(f"[W] Connect {self.__server_address} timeout!")
        elif connect_result == errno.ECONNREFUSED:
            socket_client_logger.warning(f"[W] {self.__server_address} refuse to accept connection!")
        elif connect_result == errno.EHOSTUNREACH:
            socket_client_logger.warning(f"[W] Ip is available, but can't find actively port, {self.__server_address}")
        elif connect_result == errno.ENETUNREACH:
            socket_client_logger.warning(f"[W] Ip is not available , {self.__server_address}")
        elif connect_result == errno.EALREADY:
            socket_client_logger.warning("[W] Last connection is not completely!")
        else:
            socket_client_logger.warning(f"[W] Connecting {self.__server_address} occur error: {connect_result}")
    
    def connect(self) -> bool:
        """
        连接到服务器。
        :return: 是否连接成功
        """
        try:
            # 进行不断的重连操作，直到连接上服务器，出现连接错误则休眠5秒，等待下次重连
            while True:
                # 创建套接字
                self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                connect_result = self.__client_socket.connect_ex(self.__server_address)  # 非阻塞连接
                if connect_result == 0:
                    socket_client_logger.info(f"Connect to {self.__server_address} successfully!")
                    self._start_send_heart_thread()  # 启动心跳发送线程
                    break
                else:
                    self.check_connect_status(connect_result)
                    time.sleep(self.__connect_error_sleep_time)
            return True
        except socket.error as e:
            socket_client_logger.error(f"Connection error: {e}")
            return False

    def reconnect(self) -> None:
        """
          重新连接到服务器。
          :return: None
        """
        try:
            # 进行不断的重连操作，直到连接上服务器，出现连接错误则休眠5秒，等待下次重连
            while True:
                # 创建套接字
                self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                connect_result = self.__client_socket.connect_ex(self.__server_address)  # 非阻塞连接
                if connect_result == 0:
                    socket_client_logger.info(f"Connect to {self.__server_address} successfully!")
                    break
                else:
                    self.check_connect_status(connect_result)
                    time.sleep(self.__connect_error_sleep_time)
        except socket.error as e:
            socket_client_logger.error(f"Connection error: {e}")

    def _handle(self, message: dict) -> None:
        if message['message']['type'] == 'connect':
            self.client_id = message['message']['params']['client_id']

    def close(self) -> None:
        """
        关闭客户端套接字。
        :return: None
        """
        if self.__client_socket:
            self.__client_socket.close()
            self.__client_socket = None
            socket_client_logger.info("Connection closed.")

    def listen(self) -> None:
        """
        使用 select 模块监听服务器的响应。
        :return: None
        """
        while True:
            try:
                with self._is_reconnect_lock:
                    if self._is_reconnect:
                        self.close()
                        self.reconnect()
                        self._is_reconnect = False
                # 使用 select 监听可读的套接字（客户端套接字）
                readable, _, exceptional = select.select([self.__client_socket], [], [], self.__timeout)

                if readable:
                    try:
                        flag, message = self.recv_message()
                        if flag:
                            socket_client_logger.info(f"Received message: {message}")
                            self._del_ack_dict(message)
                            self._response_ack_message(message)
                            self._handle(message)
                        else:
                            if message:
                                socket_client_logger.warning(f"Received message: {message}")
                    except socket.timeout:
                        # 超时异常处理
                        socket_client_logger.warning(f"Timeout while receiving message from {self.__server_address}")
                    except Exception as e:
                        socket_client_logger.warning(
                            f"Connection with {self.__server_address} occur error: {e}")
                elif exceptional:
                    for s in exceptional:
                        socket_client_logger.error(f"Exceptional socket: {s}")
                        self.close()
                        break
                else:
                    # 如果没有事件发生，说明 select 超时
                    pass

            except Exception as e:
                socket_client_logger.error(f"Error during listening: {e}")
                self.close()
                self.reconnect()


if __name__ == "__main__":
    client = SocketClient(server_address=("127.0.0.1", 8010))

    if client.connect():
        # 监听服务器响应
        client.listen()
        # 关闭连接
        client.close()
