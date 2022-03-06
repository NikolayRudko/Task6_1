import re
import socket
import time


class Client:
    def __init__(self, address, port, timeout=None):
        self.address = address
        self.port = port
        self.timeout = timeout
        self.sock = socket.create_connection((self.address, self.port), self.timeout)

    def put(self, metric_type, value, timestamp=None):
        timestamp = int(time.time()) if timestamp is None else timestamp
        data = f"put {metric_type} {value} {timestamp}\n"
        try:
            self.sock.sendall(data.encode("utf8"))
            data = self.sock.recv(1024).decode()
            if "error" in data:
                raise ClientError
        except Exception:
            raise ClientError

    def get(self, args):
        data = f"get {args}\n"
        try:
            self.sock.sendall(data.encode("utf8"))
            data = self.sock.recv(1024).decode()
            if data == "ok\n\n":
                return {}
            pattern = r"(^ok\n)(([\w.]*(\s[\d]*(\.[\d]*)*)\s(\d*)\n)*)(\n$)"
            if not re.match(pattern, data):
                raise ClientError
            m = re.search(pattern, data)
            parce_data = m.group(2)
            parce_data = parce_data.strip().split("\n")
            response = {}
            for i in parce_data:
                key, *val = i.split()
                tmp_data = [(int(timestamp), float(value)) for value, timestamp in zip(val[0::2], val[1::2])]
                if key in response:
                    response[key].extend(tmp_data)
                    response[key] = sorted(response[key], key=lambda x: x[0])
                else:
                    response[key] = tmp_data
            return response
        except Exception:
            raise ClientError


class ClientError(Exception):
    pass
