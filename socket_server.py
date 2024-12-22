"""
Author: MaJie
Date: 2024/12/18 16:00
Description:
编写了python版本的socket服务器模板代码, 提供了如下功能:
   1. 日志记录.
   2. 连接释放
   3. 使用orjson替代json进行json与字符串的格式转换
   4. 消息通过头部标识, 消息长度, 尾部标识来防止粘包
   5. 心跳发送机制
   6. 显示ack响应机制
   7. 一对多机制
"""
import socket
import threading
import time
from loguru import logger
import select
from typing import Union, Optional, List
import uuid
import orjson
import copy

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
socket_server_logger = logger.bind(name=f'{__log_name}')


class SocketServer:
    def __init__(self, server_address: tuple):
        """
        初始化服务器，设置默认的服务器地址、套接字类型、协议族和端口复用标志、连接数量。
        """
        self.__server_address = server_address  # 默认绑定到所有可用IP和端口8010
        self.__transmit_type = socket.SOCK_STREAM  # 默认使用TCP协议
        self.__family = socket.AF_INET  # 默认使用IPv4协议族
        self.__allow_port_fast_reuse = True  # 默认允许端口快速复用
        self.__server_socket = None  # 初始化服务器套接字
        self.__listen_num = 5  # 连接数量
        self.__client_sockets = {}  # 用于存储客户端套接字 每个uuid对应一个socket
        self.__client_ids = {}  # 用于存储客户端id， 每个socket对应一个uuid
        self.__client_lock = threading.Lock()  # 给客户端操作加锁
        self.__client_timeouts = {}  # 用于存储每个客户端的超时时间 每个uuid对应其客户端设置的超时时间
        self.__client_address = {}  # 用于存储客户端的ip和端口 每个uuid对应其客户端的ip和address
        self.__HEADER = b"HEADER"  # 消息头
        self.__FOOTER = b"FOOTER"  # 消息尾
        self.__message_len = 7  # 消息长度
        self.__use_heart_thread = True  # 是否启动心跳线程
        self.__heartbeat_interval = 2  # 心跳发送间隔
        self.__heartbeat_thread = threading.Thread(target=self._send_heart_task, daemon=True)  # 心跳发送线程
        self.__use_ack_check = True  # 是否启动ack检查
        self.__ack_message_manage = {}  # ack消息管理字典
        self.__ack_message_max_len = 100000  # ack消息管理字典最多存储多少条消息
        self.__ack_lock = threading.Lock()  # 更新和删除消息字典内容时，必须要加锁
        self.__ack_check_interval = 1  # ack检查机制
        self.__ack_check_thread = threading.Thread(target=self._check_ack_task, daemon=True)  # ack检查线程
        self.__ack_timeout = 3  # ack消息的超时时间，默认3s

    def set_transmit_type(self, transmit_type: int) -> None:
        """
        设置套接字类型，默认soket.SOCK_STREAM(1)代表TCP，socket.SOCK_DGRAM(2)代表UDP
        :param transmit_type: 套接字类型
        :return: None
        """
        if isinstance(transmit_type, int) and transmit_type in [socket.SOCK_STREAM, socket.SOCK_DGRAM]:
            self.__transmit_type = transmit_type
        else:
            raise ValueError("transmit_type must be socket.SOCK_STREAM or socket.SOCK_DGRAM (int type)")

    def set_family(self, family: int) -> None:
        """
        设置协议族，默认socket.AF_INET(IPV4)
        :param family: 协议类型
        :return: None
        """
        if isinstance(family, int) and family in [socket.AF_INET, socket.AF_INET6]:
            self.__family = family
        else:
            raise ValueError("family must be socket.AF_INET or socket.AF_INET6 (int type)")

    def set_allow_port_fast_reuse(self, reuse_flag: bool) -> None:
        """
        设置端口快速复用，默认为True
        :param reuse_flag: 复用标识
        :return: None
        """
        if isinstance(reuse_flag, bool):
            self.__allow_port_fast_reuse = reuse_flag
        else:
            raise ValueError("reuse flag must be a boolean")

    def set_listen_num(self, listen_num: int) -> None:
        """
        设置最大连接数量
        :param listen_num:
        :return: None
        """
        if isinstance(listen_num, int) and listen_num > 0:
            self.__listen_num = listen_num
        else:
            raise ValueError("listen num must be a positive integer and greater than 5!")

    def set_client_time_out(self, time_out: Union[int, float], client_ip: str, client_port: int) -> None:
        """
        设置指定ip和port需要的超时时间，支持int和float类型。默认为4s
        :param client_port: 客户端端口
        :param client_ip: 客户端ip
        :param time_out: 超时时间（可以是int或float）
        :return: None
        """
        if isinstance(time_out, (int, float)):
            self.__client_timeouts[f"{client_ip}:{client_port}"] = time_out
        else:
            raise ValueError("client timeout must be an int or float")

    def set_header_flag(self, header_flag: bytes) -> None:
        """
        设置消息的头部标识，默认为b"HEADER"
        :param header_flag: 头部标识
        :return: None
        """
        if not isinstance(header_flag, bytes):
            raise ValueError("header flag must be of type 'bytes'.")
        self.__HEADER = header_flag

    def set_footer_flag(self, footer_flag: bytes) -> None:
        """
        设置消息的尾部标识，默认为b"FOOTER"
        :param footer_flag: 尾部标识
        :return: None
        """
        if not isinstance(footer_flag, bytes):
            raise ValueError("footer flag must be of type 'bytes'.")
        self.__FOOTER = footer_flag

    def set_fix_message_len(self, fix_message_len) -> None:
        """
        设置消息长度位的最大消息接受长度，默认为7
        :param fix_message_len:
        :return: None
        """
        if not isinstance(fix_message_len, int) or fix_message_len <= 0:
            raise ValueError("fix message_len must be a positive integer and must greater than 0.")
        self.__message_len = fix_message_len

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
        
    def _apply_socket_options(self) -> None:
        """
        应用套接字的配置函数，是否允许端口快速复用。
        :return: None
        """
        if self.__allow_port_fast_reuse:
            # socket.SOL_SOCKET: 是一个 套接字层级 的常量，表示你正在设置的选项属于 套接字本身。
            # socket.SO_REUSEADDR: 表示允许快速复用地址（端口），特别是在套接字关闭后立即重新绑定端口
            # 1: 表示启用该选项，0: 则表示禁用它
            self.__server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def _send_heart_task(self) -> None:
        """
        心跳发送任务，定期往所有客户端依次发送心跳，默认2秒发送一次
        :return: None
        """
        while True:
            need_delete_client_id = []
            with self.__client_lock:
                now_clients = copy.deepcopy(self.__client_sockets)
            for client_id, client_socket in now_clients.items():
                try:
                    heartbeat_message = {"message": {"type": "heartbeat", "params": {}}, "message_id": '', 'ack': ''}
                    self._send_message(client_socket, orjson.dumps(heartbeat_message))
                except Exception as e:
                    need_delete_client_id.append(client_id)
            if len(need_delete_client_id) > 0:
                for client_id in need_delete_client_id:
                    with self.__client_lock:
                        if self.__client_sockets.get(client_id) is not None:
                            del self.__client_sockets[client_id]
            time.sleep(self.__heartbeat_interval)

    
    def _check_ack_task(self) -> None:
        """
        ack检查任务, 定期检查ack字典, 对其中的消息进行超时机制处理
        :return: None
        """
        try:
            while True:
                need_del_client_id_list = []
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
                        client_id = data_dict['client_id']
                        send_message = data_dict['message']
                        try:
                            with self.__client_lock:
                                if self.__client_sockets.get(client_id) is not None:
                                    client_connection = self.__client_sockets[client_id]
                                else:
                                    client_connection = None
                            if client_connection is None:
                                need_del_key_list.append(message_id)
                                continue
                            self._send_message(client_connection, orjson.dumps(send_message))
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

                with self.__ack_lock:
                    for del_key in need_del_key_list:
                        if self.__ack_message_manage.get(del_key) is not None:
                            del self.__ack_message_manage[del_key]
                    self.__ack_message_manage.update(now_ack_dict)

                with self.__client_lock:
                    for client_id in need_del_client_id_list:
                        if self.__client_sockets[client_id] is not None:
                            del self.__client_sockets[client_id]
                            
                time.sleep(self.__ack_check_interval)
        except Exception as e:
                socket_server_logger.error(f"Manage ack message occur error: {e}")
    
    def _generate_ack_message(self, message: dict, client_id: str) -> dict:
        """
        生成ack字典消息
        """
        temp_message = copy.deepcopy(message)
        ack_message = {'time_out': self.__ack_timeout, 'original_time': self.__ack_timeout, "message": temp_message, 'client_id': client_id, 'ack': ''}
        return ack_message

    @staticmethod
    def _generate_send_ack_message( message: dict, message_id: str)-> dict:
        """
        生成ack发送消息
        """
        ack_message = {"message": message, "message_id": message_id, 'ack': ''}
        return ack_message

    @staticmethod
    def _generate_response_ack_message(message: dict, message_id: str)-> dict:
        """
        生成ack回复消息
        """
        ack_message = {"message": message, "message_id": '', "ack": message_id}
        return ack_message

    def _put_ack_message(self, message: dict, message_id: str, client_id: str) -> None:
        """
        将ack消息加入到ack字典中
        """
        ack_message = self._generate_ack_message(message, client_id)
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
        ack_send_message = self._generate_send_ack_message(message, message_id)
        with self.__client_lock:
            client_socket = self.__client_sockets[client_id]
        self._send_message(client_socket, orjson.dumps(ack_send_message))
    
    def _response_ack_message(self, message, client_id) -> None:
        """
        回应ack消息
        """
        if message.get('message_id') is None:
            return
        if message['message_id'] == '':
            return
        ack_res_message = self._generate_response_ack_message(message, message['message_id'])
        with self.__client_lock:
            client_socket = self.__client_sockets[client_id]
        self._send_message(client_socket, orjson.dumps(ack_res_message))

    @staticmethod
    def _dict_length_generator(data_dict: dict) -> int:
        return sum(1 for _,_ in data_dict.items())

    def _start_send_heart_thread(self) -> None:
        """
        启动发送心跳的线程
        :return: None
        """
        if self.__use_heart_thread:
            self.__heartbeat_thread.start()
    
    def _start_ack_check_thread(self) -> None:
        """
        启动ack检查
        :return: None
        """
        if self.__use_ack_check:
            self.__ack_check_thread.start()

    def _send_message(self, client_socket: socket.socket, message: bytes) -> None:
        """
        发送消息，格式：头部 + 消息长度(7) + 消息内容 + 尾部
        :param client_socket: 与客户端的套接字
        :param message: 需要发送的消息，字节类型
        :return: None
        """
        # 将消息长度转换为固定长度的字节数组(7位)，并在不足长度时使用前置零进行填充
        length_bytes = str(len(message)).zfill(self.__message_len).encode('utf-8')
        message_with_length = length_bytes + message
        all_message = self.__HEADER + message_with_length + self.__FOOTER
        client_socket.sendall(all_message)

    @staticmethod
    def _recv_fixed_len_message(client_socket: socket.socket, message_len: int) -> Optional[bytes]:
        """
        接收指定长度的消息。
        :param client_socket: 客户端套接字
        :param message_len: 要接收的消息长度
        :return: 接收到的字节消息，如果接收失败返回 None
        """
        buffer = b""
        while len(buffer) < message_len:
            data_byte = client_socket.recv(message_len - len(buffer))
            if not data_byte:
                # 如果连接关闭或异常中断，返回 None
                return None
            buffer += data_byte
        return buffer

    def _recv_message(self, client_socket: socket.socket) -> List[Union[bool, Union[Optional[str], dict]]]:
        """
        接收并解析消息，校验头部、尾部和消息长度。
        :param client_socket: 客户端套接字
        :return: 消息内容（解析后的字典），如果有错误，返回包含错误信息的字典
        """
        # 校验头部
        buffer_header = self._recv_fixed_len_message(client_socket, len(self.__HEADER))
        if buffer_header == '' or buffer_header is None:
            return [False, None]
        if buffer_header != self.__HEADER:
            return [False, f"Header is not available, got {buffer_header}, expected {self.__HEADER}"]

        # 接收固定长度的长度字段
        length_bytes = self._recv_fixed_len_message(client_socket, self.__message_len)
        if not length_bytes:
            return [False, f"Message len is not available, got {length_bytes}, expected {self.__message_len}"]

        # 解析消息长度
        message_length = int(length_bytes.decode('utf-8'))
        buffer_message = self._recv_fixed_len_message(client_socket, message_length)
        if not buffer_message:
            return [False, f"buffer is not available, got {buffer_message}"]

        # 校验尾部
        buffer_footer = self._recv_fixed_len_message(client_socket, len(self.__FOOTER))
        if buffer_footer != self.__FOOTER:
            return [False, f"Footer is not available, got {buffer_footer}, expected {self.__FOOTER}"]

        # 使用 orjson 解析消息
        return [True, orjson.loads(buffer_message)]

    def start(self) -> None:
        """
        启动服务器，绑定并监听端口。
        :return: None
        """
        try:
            # 创建套接字
            self.__server_socket = socket.socket(self.__family, self.__transmit_type)
            # 应用套接字选项
            self._apply_socket_options()
            # 绑定服务器地址
            self.__server_socket.bind(self.__server_address)
            # 开始监听
            self.__server_socket.listen(self.__listen_num)
            self.__server_socket.setblocking(False)  # 设置为非阻塞模式
            socket_server_logger.info(f"Server started successfully，listening at {self.__server_address}")
            self._start_send_heart_thread()  # 启动心跳发送发送线程
            self._start_ack_check_thread()  # 启动ack检查线程
            self.listen()  # 进入监听状态
        except Exception as e:
            socket_server_logger.error(f"Error starting server: {e}")


    def stop(self) -> None:
        """
        停止服务器，关闭套接字。
        :return: None
        """
        if self.__server_socket:
            self.__server_socket.close()
            self.__server_socket = None
            socket_server_logger.info(f"Stop server successfully!")
        else:
            socket_server_logger.info(f"Server is not start!")

    def _handle(self, message: dict) -> None:
        """
        处理客户端发来的消息
        :return: None
        """
        pass
    
    def _generate_client_id(self) -> str:
        """
        生成唯一的客户端id
        """
        while True:
            client_id = str(uuid.uuid4())
            if self.__client_address.get(client_id) is None:
                return client_id
    
    def _generate_message_id(self) -> str:
        """
        生成唯一的消息id
        """
        while True:
            message_id = str(uuid.uuid4())
            with self.__ack_lock:
                if self.__ack_message_manage.get(message_id) is None:
                    return message_id
              

    def listen(self) -> None:
        """
        使用 select 模块监听连接和消息。
        :return: None
        """
        while True:
            try:    
                """
                    使用 select 监听可读的套接字（客户端套接字 + 服务器套接字）、可写的套接字、和 异常的套接字。
                    服务器套接字 (self.__server_socket)：它用于接受新的客户端连接，所以你需要把它放在监听列表里。
                    客户端套接字: 服务器需要监听它们，以便接收客户端发来的数据。
                    默认最快接收到的消息是0.1秒会接受一条，监测时间设置为0.1秒，代表服务器会每隔0.1秒去检查一次套接字的状态
                    readable：可读的套接字列表，每隔0.1秒都会遍历一遍所有的可读套接字，不会判断套接字里面是否有消息存在
                    writable: 可写的套接字列表
                    exceptional: 异常的套接字列表
                """
                readable, writable, exceptional = select.select([self.__server_socket] + list(self.__client_sockets.values()), [], [], 0.1)
                for s in readable:
                    if s is self.__server_socket:
                        # 有新的连接，存储新连接对应的信息
                        client_socket, client_address = self.__server_socket.accept()
                        client_ip, client_port = client_address
                        client_key = f'{client_ip}:{client_port}'
                        client_timeout = self.__client_timeouts[client_key] if self.__client_timeouts.get(client_key) is not None else 4
                        client_socket.setblocking(False)
                        client_id = self._generate_client_id()  # 使用 UUID 生成唯一标识符
                        client_socket.settimeout(client_timeout)
                        socket_server_logger.info(f"New connection from {client_key}，give id: {client_id}")
                        with self.__client_lock:
                            self.__client_sockets[client_id] = client_socket
                        self.__client_ids[client_socket] = client_id
                        self.__client_address[client_id] = [client_ip, client_port]
                        # 给客户端发放唯一用户标识，且需要进行ack验证，保证用户收到了
                        message_id = self._generate_message_id()
                        message = {'type': 'connect', 'params': {'client_id': client_id}}
                        self._put_ack_message(message, message_id, client_id)
                        self._send_ack_message(message, message_id, client_id)
                    else:
                        client_id = self.__client_ids[s]
                        client_address = self.__client_address[client_id]
                        # 有数据可读，处理客户端消息
                        try:
                            flag, data = self._recv_message(s)
                            if flag:
                                socket_server_logger.info(f"{data}")
                                self._del_ack_dict(data)
                                self._response_ack_message(data, client_id)
                                self._handle(data)
                            else:
                                if data:
                                    socket_server_logger.warning(f"{data}")
                        except socket.timeout:
                            # 超时异常处理
                            socket_server_logger.warning(f"Timeout while receiving message from {client_address[0]}:{client_address[1]}")
                        except Exception as e:
                            socket_server_logger.warning(f"Connection with {client_address[0]}:{client_address[1]} occur error: {e}")
                            with self.__client_lock:
                                if self.__client_sockets.get(client_id) is not None:
                                    del self.__client_sockets[client_id]
            except Exception as e:
                socket_server_logger.error(f"Error during listening: {e}")
                time.sleep(5)


    def get_config(self) -> dict:
        """
        返回服务器的配置信息字典。
        :return: 配置信息字典
        """
        return {
            "server_address": self.__server_address,
            "transmit_type": self.__transmit_type,
            "family": self.__family,
            "all_port_fast_reuse": self.__allow_port_fast_reuse,
            "list_num": self.__listen_num
        }

    def __str__(self) -> str:
        """
        返回服务器的配置信息。
        :return: 配置信息字符串
        """
        # __str__ 是 Python 的内建方法
        # 用于定义类的“字符串表示”形式。当你调用 str() 或者直接打印类的实例时，__str__ 方法会被自动调用，以返回一个易于阅读的字符串形式。
        return (f"服务器地址: {self.__server_address}, "
                f"套接字类型: {self.__transmit_type}, "
                f"协议族: {self.__family}, "
                f"端口快速复用: {self.__allow_port_fast_reuse},"
                f"允许连接数量：{self.__listen_num}")


if __name__ == "__main__":
    socket_server = SocketServer(('0.0.0.0', 8010))
    socket_server.start()