import socket
import threading
import json
import os
import sys
from signal import SIGTERM
import random
import multiprocessing


class UDPServer(object):
    def __init__(self, port, pid_file=None, sub_pid_file=None):
        self.server = None
        self.port = port
        self.pid_file = pid_file
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.sub_pid_file = sub_pid_file
        self.queue = multiprocessing.Queue()
        self.clients = {}
        self.ack = {}

    def handler(self):
        self._write_pid(self.sub_pid_file)
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
        self._write_pid(self.pid_file)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.bind(('', self.port))
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

    def _write_pid(self, file):
        if os.path.exists(os.path.join(self.base_dir, file)):
            print('process already running!start fail.')
            exit(0)
        with open(file, 'w') as f:
            f.write(str(os.getpid()))

    def _read_pid(self, file):
        if not os.path.exists(os.path.join(self.base_dir, file)):
            print("%s does not exists." % file)
            exit(0)

        with open(file) as f:
            pid = int(f.read().strip())
        return pid

    @staticmethod
    def _remove_pid(file):
        os.remove(file)

    @property
    def exists(self, file):
        if os.path.exists(file):
            return True
        return False

    def stop(self):
        pid = self._read_pid(self.sub_pid_file)
        if pid:
            os.kill(pid, SIGTERM)
            os.remove(self.sub_pid_file)

        pid = self._read_pid(self.pid_file)
        if pid:
            os.kill(pid, SIGTERM)
            os.remove(self.pid_file)


if __name__ == '__main__':
    args = sys.argv
    if len(args) == 2:
        action = args[1]
    else:
        print('missing parameter.')
        exit(0)

    server = UDPServer(pid_file='udp_main.pid', sub_pid_file='udp_sub.pid', port=8888)
    if action == 'stop':
        print('stop the process...')
        server.stop()
        exit(0)
    if action == 'start':
        server.run()
