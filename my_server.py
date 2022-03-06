import asyncio
import re


class ClientServerProtocol(asyncio.Protocol):
    storage = {}

    # def __init__(self):
    #     self.storage = {}

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode())
        self.transport.write(resp.encode())

    def process_data(self, data):
        pattern = r"(^((put)\s([\w.]*)\s([\d]*(\.[\d]*)*)\s([\d]*))|((get)\s(([\w.]*)|(\*)))\n$)"
        ex_pattern = re.compile(pattern)

        if not ex_pattern.match(data):
            return 'error\nwrong command\n\n'

        clients_data = ex_pattern.search(data)
        command = "".join(filter(bool, [clients_data.group(3), clients_data.group(9)]))
        if command == "put":
            put_command = clients_data.group(4)
            value = float(clients_data.group(5))
            timestamp = int(clients_data.group(7))
            return put_command(put_command, value, timestamp)
        elif command == "get":
            get_command = "".join(filter(bool, [clients_data.group(11), clients_data.group(12)]))
            return self.get_data(get_command)
        else:
            print("Unknown error!!!")
            response = 'error\nwrong command\n\n'

        return response

    def put_command(self, put_command: str, value: float, timestamp: int):
        storage = self.storage.setdefault(put_command, [])
        storage.append((value, timestamp))
        storage.sort(key=lambda x: [1])

        return "ok\n\n"

    def get_data(self, command: str):
        if command == "*":
            response = ""
            for k, v in self.storage.items():
                storage_data = [" ".join([str(j) for j in i]) for i in k]
                storage_data = [f"{v} {i}\n" for i in storage_data]
                response += "".join(storage_data)
            response = f"ok\n{response}\n"
        else:
            if command not in self.storage:
                response = "ok\n\n"
            else:
                storage_data = self.storage[command]
                storage_data = [" ".join([str(j) for j in i]) for i in storage_data]
                storage_data = [f"{command} {i}\n" for i in storage_data]
                storage_data = "".join(storage_data)
                response = f"ok\n{storage_data}\n"

        return response


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run_server("127.0.0.1", 8888)
