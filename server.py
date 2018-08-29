import socket
import threading
import json
import argparse
import random
import multiprocessing


class UDPServer(object):
    """
    UDP server.
    """

    def __init__(self, args):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.bind(('', args.port))
        self.queue = multiprocessing.Queue()
        self.clients = {}
        self.ack = {}

    def handler(self):
        while True:
            client_msg, client_addr = self.queue.get()
            client_msg = json.loads(client_msg.decode('utf-8'))

            # 如果有机器id则覆盖（后台无法覆盖机器地址)
            if client_msg.get('m_id') and client_addr[0] != '127.0.0.1':
                self.clients[client_msg['m_id']] = client_addr

            # 如果是心跳包
            if len(client_msg.keys()) == 1 and client_msg.get('m_id'):
                send_msg = {}
                self.server.sendto(json.dumps(send_msg).encode('utf-8'), client_addr)

            # 如果是后台发过来的消息
            elif client_addr[0] == '127.0.0.1':

                # 随机生成s_id
                s_id = random.randint(10, 99)
                client_msg['s_id'] = s_id
                # 获取m_id
                m_id = client_msg.get('m_id')
                # 没有该机器地址，直接忽略
                if m_id not in self.clients:
                    continue

                machine_addr = self.clients[m_id]
                print('目标地址:', machine_addr)

                self.ack.setdefault(m_id, {})
                self.ack[m_id] = {
                    's_id': s_id,
                    'status': 0
                }
                if machine_addr:
                    client_msg = json.dumps(client_msg).encode('utf-8')
                    print('转发后台消息:', client_msg)
                    self.server.sendto(client_msg, machine_addr)
                    threading.Timer(1, self.timed_task, (m_id, s_id, client_msg, machine_addr, 3)).start()
            elif client_msg.get('m_id') and client_msg.get('s_id') and client_msg.get('type') == 'ack':
                m_id = client_msg['m_id']
                s_id = client_msg['s_id']
                if s_id == self.ack[m_id]['s_id']:
                    self.ack[client_msg['m_id']]['status'] = 1

    def run(self):
        p1 = multiprocessing.Process(target=self.handler)
        p1.start()
        while True:
            data = self.server.recvfrom(8196)
            print(data)
            self.queue.put(data)

    def timed_task(self, machine_id, session, msg, addr, count):
        status = self.ack[machine_id]['status']
        if status == 1:
            return None
        else:
            if count <= 0:
                return None
            count -= 1
            self.server.sendto(msg, addr)
            threading.Timer(1, self.timed_task, (machine_id, session, msg, addr, count)).start()


if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='Use -h to see help')
    parse.add_argument('-p', '--port', help='Add server port', default=8888, type=int)
    parse.add_argument('-w', '--worker', help='Add thread worker', default=10, type=int)
    args = parse.parse_args()
    server = UDPServer(args)
    server.run()
