# /usr/bin/env python3
import socket
import json
import requests
import time
import random
import threading

from tools import sign, log
from config import SERVER_PORT, ATTEMPTS, BEAT_TIME, SERVER_URL, OPEN_CHEST_URL, GIVE_BACK_URL


class MachineUDPClient:
    def __init__(self, name, url, port, beat_time=1, attempts=3, bind_address=('0.0.0.0', 0)):

        self.mutex = threading.Lock()                   # 同步锁
        self.MACHINE_ID = name                          # 配置
        self.BEAT_TIME = beat_time                      # 心跳间隔
        self.ATTEMPTS = attempts                        # 尝试重新发送次数
        self.connecting_flag = False                    # 默认与服务器断开连接
        self.req_url = url
        host = socket.gethostbyname(self.req_url)
        port = port
        self.server_address = (host, port)              # 服务器地址
        self.client = ""                                # 保存UDP客户端实例
        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.bind(bind_address)
        # 心跳包
        self.heart_beat = {
            'm_id': self.MACHINE_ID
        }

        # ack package
        self.ack = {
            's_id': "",
            'm_id': self.MACHINE_ID,
            'type': 'ack'
        }
        self.is_first = True                            # 是否为第一次请求
        self.fail_count = 0                             #

    def _msg_handler(self, msg):
        if len(msg.keys()) == 0:
            self.mutex.acquire()
            self.connecting_flag = True
            self.fail_count = 0
            self.mutex.release()
            return None

        if msg.get('code'):
            self._send_ack(msg)
            self._print_code(msg)
            return

        if msg.get('action') and msg['action'] == 'open':
            # process action type message here.
            self._send_ack(msg)
            self._open_chest(msg)

    def _send_ack(self, msg):
        self.ack['s_id'] = msg.get('s_id')
        self.client.sendto(json.dumps(self.ack).encode('utf-8'), self.server_address)

    def _print_code(self, msg):
        # process verification code here.
        print(msg)

    def _open_chest(self, msg):
        # process action type message here.
        print(msg)

        # prepare for return message
        url = OPEN_CHEST_URL
        post_data = {
            'machine_id': self.MACHINE_ID,
            'lock_id': msg.get('lock_id'),
            'status': 'SUCCESS',
        }
        # send...
        t = threading.Thread(target=self._send_msg_to_background, args=(url, post_data))
        t.start()

    def _waiting_msg(self):
        while True:
            msg, address = self.client.recvfrom(1024)
            print(msg, address)
            # self.server_address = address
            msg = json.loads(msg.decode('utf-8'))
            self._msg_handler(msg)

    def _give_back(self, lock_id):
        # write here
        url = GIVE_BACK_URL
        post_data = {
            'machine_id': self.MACHINE_ID,
            'lock_id': lock_id,
            'status': 'SUCCESS',
        }
        # send...
        t = threading.Thread(target=self._send_msg_to_background, args=(url, post_data))
        t.start()

    @staticmethod
    def _send_msg_to_background(url, post_data):
        time_stamp = str(time.time())
        s = sign(post_data, time_stamp)
        header = {
            'timeStamp': time_stamp,
            'sign': s
        }
        fail_count = 3
        while fail_count:
            try:
                resp = requests.post(url, post_data, headers=header)
                resp = resp.json()
                if 'SUCCESS' in resp.get('status'):
                    break
            except Exception as e:
                fail_count -= 1

    def run(self):
        t1 = threading.Thread(target=self._waiting_msg)
        t1.setDaemon(True)
        t1.start()
        while True:
            if not self.is_first:
                if not self.connecting_flag:
                    if self.fail_count < self.ATTEMPTS:
                        self.fail_count += 1
                        log('server can\'t receive heart beat, resend...', self.fail_count)
                        self.is_first = False
                        self.connecting_flag = True
                        continue

                    log('disconnect from server, machine conversion to offline mode!')
                    self.client.close()
                    break

            log('send a heart beat to server..')
            self.client.sendto(json.dumps(self.heart_beat).encode('utf-8'), self.server_address)
            self.connecting_flag = False
            self.is_first = False
            time.sleep(self.BEAT_TIME)


if __name__ == '__main__':
    machine = MachineUDPClient('TestDemo1234', SERVER_URL, SERVER_PORT, BEAT_TIME, ATTEMPTS)
    machine.run()
