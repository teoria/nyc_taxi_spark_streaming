import os
import random
import socket
import threading
import time


def send_event(conex, json_data_list):
    [conex.send(f"{data}\n".encode('utf-8')) for data in json_data_list]


def read_dataset_file(file_path: str):
    with open(file_path) as f:
        for line in f:
            yield line.strip()


def flush_data():
    print(f"flush_data")
    global json_data_list
    global conex
    if len(json_data_list) > 0:
        try:
            if conex:
                send_event(conex, json_data_list)
                print(f"flush: {len(json_data_list)}")
                json_data_list = []
        except BrokenPipeError as e:
            print("Broken pipe")
            conex = None
    else:
        print(f"Empty json_data_list")


def main(file_reader_generator, offset, sleep_time):
    print("start taxi event producer")
    count = 0
    global json_data_list
    global param_offset
    global conex
    for row in file_reader_generator:
        count = count + 1
        if count <= offset:
            continue
        if conex is None:
            return
        json_data_list.append(row)
        param_offset = count
        if count % 100 == 0:
            print(count)
        time.sleep(random.uniform(0.01, sleep_time))


def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()

    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


def wait_conex():
    global conex
    global param_offset
    conex, socket_client = tcp.accept()
    print("Received request from: " + str(socket_client))
    main(file_reader_generator=file_reader,
         offset=param_offset,
         sleep_time=.09
         )


if __name__ == '__main__':
    print('Taxi Socket')
    param_offset = int(os.getenv("OFFSET", 1))
    json_data_list = []
    conex = None
    tcp = socket.socket()
    host = "0.0.0.0"
    port = 5557
    file_path = 'data/train.csv'
    file_reader = read_dataset_file(file_path)
    tcp.bind((host, port))
    print('socket is ready')
    tcp.listen(1)
    print('socket is listening')
    set_interval(flush_data, 2)

    while True:
        wait_conex()
