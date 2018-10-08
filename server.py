import socket
import threading
import json
import os
from signal import SIGTERM
import argparse
import random
import multiprocessing


class UDPServer(object):
    def __init__(self, args, pid_file=None):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.bind(('', args.port))
        self.pid_file = pid_file
        self.queue = multiprocessing.Queue()
        self.clients = {}
        self.ack = {}

    def handler(self):
        while True:
            message, address = self.queue.get()
            ip, port = address
            message = json.loads(message.decode('utf-8'))

            if message.get('m_id') and ip != '127.0.0.1':
                self.clients[message['m_id']] = address

            if len(message.keys()) == 1 and message.get('m_id'):
                send_msg = {}
                self.server.sendto(json.dumps(send_msg).encode('utf-8'), address)

            elif ip == '127.0.0.1' and port != 5678:
                s_id = random.randint(10, 99)
                message['s_id'] = s_id
                m_id = message.get('m_id')
                if m_id not in self.clients:
                    continue

                machine_address = self.clients[m_id]

                self.ack.setdefault(m_id, {})
                self.ack[m_id] = {
                    's_id': s_id,
                    'status': 0
                }
                if machine_address:
                    message = json.dumps(message).encode('utf-8')
                    self.server.sendto(message, machine_address)
                    threading.Timer(1, self.resend, (m_id, s_id, message, machine_address, 3)).start()
            elif message.get('m_id') and message.get('s_id') and message.get('type') == 'ack':
                m_id = message['m_id']
                s_id = message['s_id']
                if s_id == self.ack[m_id]['s_id']:
                    self.ack[message['m_id']]['status'] = 1

    def run(self):
        p1 = multiprocessing.Process(target=self.handler)
        p1.start()
        while True:
            try:
                data = self.server.recvfrom(4096)
                self.queue.put(data)
            except KeyboardInterrupt:
                print('\nquit.')
                exit(0)

    def resend(self, machine_id, session, msg, address, count):
        status = self.ack[machine_id]['status']
        if status == 1:
            return None
        else:
            if count <= 0:
                return None
            count -= 1
            self.server.sendto(msg, address)
            threading.Timer(1, self.resend, (machine_id, session, msg, address, count)).start()

    # def _write_pid(self):
    #     if self.pid_file:
    #         with open(self.pid_file) as f:
    #             pid = int(f.read().strip())
    #
    #     if pid:
    #         os.kill(pid, SIGTERM)


if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='Use -h to see help')
    parse.add_argument('-p', '--port', help='Add server port', default=8888, type=int)
    parse.add_argument('-w', '--worker', help='Add thread worker', default=10, type=int)
    # parse.add_argument('-s', '--stop', help='Kill the process', default=0, type=int)
    args = parse.parse_args()
    server = UDPServer(args)
    # if args.stop:
    #     print('stop the process...')
    #     exit(0)

    server.run()
