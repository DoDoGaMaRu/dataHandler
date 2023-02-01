from scipy import signal

from sensor import Sensor
from sys import exit
from configparser import ConfigParser
from time import ctime, time

import nidaqmx
import socketio
import asyncio


conf = ConfigParser()
conf.read('resource/config.ini')
raw_directory = conf['csv']['directory']
server_address = conf['server']['address']
machine_namespace = conf['server']['namespace']
send_sampling_rate = int(conf['server']['send_sampling_rate'])


sio = socketio.AsyncClient(logger=False) #TODO socketio Client 는 커스텀 logger를 지원하므로, 로거 사용하자


def sensor_config_load(config: ConfigParser):
    sampling_rate = int(config['sensor']['rate'])
    sensor_buffer_size = sampling_rate * 2

    vib_device = config['vib']['device']
    vib_channel_name = vib_device + "/" + config['vib']['channels']

    temp_device = config['temp']['device']
    temp_channel_name = temp_device + "/" + config['temp']['channels']

    return sampling_rate, sensor_buffer_size, vib_channel_name, temp_channel_name


def sensor_load(config: ConfigParser):
    try:
        return try_sensor_load(config)
    except nidaqmx.errors.DaqError:
        print('잘못된 설정값이 입력 되었습니다. config.ini 파일을 올바르게 수정해 주세요.')
        exit()


def init_sensor(sampling_rate: int, sensor_buffer_size: int, vib_channel_name: str, temp_channel_name: str):
    vib = Sensor.vib(vib_channel_name, sampling_rate, sensor_buffer_size)
    temp = Sensor.temp(temp_channel_name, sampling_rate, sensor_buffer_size)
    return vib, temp


def try_sensor_load(config: ConfigParser):
    rate, buffer_size, vib_channel, temp_channel = sensor_config_load(config)
    vib, temp = init_sensor(rate, buffer_size, vib_channel, temp_channel)
    return vib, temp


async def read_sensor(sensor: Sensor):
    return sensor.task.read(number_of_samples_per_channel=sensor.read_count, timeout=10.0)


async def get_sensor_message(now_time, data_tag_names, data_list):
    message = {
        'time': now_time
    }
    for idx, data in enumerate(data_list):
        message[data_tag_names[idx]] = data

    return message


async def resample_message(message, sampling_rate, data_tag_names):
    me = message.copy()
    for tag in data_tag_names:
        me[tag] = signal.resample(me[tag], sampling_rate).tolist()

    return me


async def try_read(sensor: Sensor, event_name: str, data_tag_names: list):
    now_time = ctime(time())
    data_list = await read_sensor(sensor)
    message = await get_sensor_message(now_time, data_tag_names, data_list)
    resampled_message = await resample_message(message, send_sampling_rate, data_tag_names)

    await sio.sleep(1)
    #await add_data_by_event(event_name, message) # TODO CsvController 만들어서 데이터 저장하고 NAS로 전송해야함
    if sio.connected:
        await sio.emit(event_name, resampled_message, namespace=machine_namespace)


async def read(sensor: Sensor, event_name: str, data_tag_names: list):
    try:
        await try_read(sensor, event_name, data_tag_names)
    except nidaqmx.errors.DaqReadError:
        pass
    except Exception as error:
        print(error)


async def sensor_loop_vib():
    while True:
        await read(sensor_vib, 'vib', ['machine2_left', 'machine2_right', 'machine1_left', 'machine1_right'])


async def sensor_loop_temp():
    while True:
        await read(sensor_temp, 'temp', ['machine2', 'machine1'])


@sio.on('connect', namespace=machine_namespace)
def on_connect():
    print('connection established')


@sio.on('disconnect', namespace=machine_namespace)
def on_connect():
    print('connection closed')


async def socket_connect():
    while True:
        try:
            await sio.connect(url=server_address,
                              namespaces=[machine_namespace],
                              wait_timeout=10)
            await sio.wait()
        except Exception as e:
            print('socket connect error :', e)
            await sio.sleep(10)


if __name__ == '__main__':
    sensor_vib, sensor_temp = sensor_load(conf)
    main_loop = asyncio.get_event_loop()

    sensor_task_vib = sio.start_background_task(sensor_loop_vib)
    sensor_task_temp = sio.start_background_task(sensor_loop_temp)

    try:
        main_loop.run_until_complete(socket_connect())
    except KeyboardInterrupt:
        print('Waiting for application shutdown.')
        sensor_task_vib.cancel()
        sensor_task_temp.cancel()
        print('Application shutdown complete.')
