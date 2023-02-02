import nidaqmx
import socketio
import asyncio
import logger

from scipy import signal
from sys import exit
from time import ctime, time
from configparser import ConfigParser

from sensor import Sensor
from rawController import RawdataController
from logger import LoggerFactory


conf = ConfigParser()
conf.read('resource/config.ini', encoding='utf-8')
raw_directory = conf['csv']['directory']
external_directory = conf['csv']['external_directory']
server_address = conf['socket']['url']
machine_namespace = conf['socket']['namespace']
send_sampling_rate = int(conf['socket']['send_sampling_rate'])
log_path = conf['log']['directory']


''' 
    sys_logger      : 

    rdc             : 
'''


LoggerFactory.init_logger(name='log',
                          save_file=True,
                          save_path=log_path)

sys_logger = LoggerFactory.get_logger()
sio = socketio.AsyncClient()
rdc = RawdataController(raw_directory=raw_directory,
                        external_directory=external_directory)


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
        sys_logger.error('잘못된 설정값이 입력 되었습니다. config.ini 파일을 올바르게 수정해 주세요.')
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


async def add_data_by_event(event_name, message):
    if event_name == 'vib':
        await rdc.add_vib(message)
    elif event_name == 'temp':
        await rdc.add_temp(message)


async def try_read(sensor: Sensor, event_name: str, data_tag_names: list):
    now_time = ctime(time())
    data_list = await read_sensor(sensor)
    message = await get_sensor_message(now_time, data_tag_names, data_list)
    resampled_message = await resample_message(message, send_sampling_rate, data_tag_names)

    await sio.sleep(1)
    await add_data_by_event(event_name, message)
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
    sys_logger.info('connection established')


@sio.on('disconnect', namespace=machine_namespace)
def on_disconnect():
    sys_logger.info('connection closed')


async def socket_connect():
    while True:
        try:
            await sio.connect(url=server_address,
                              namespaces=[machine_namespace],
                              wait_timeout=10)
            await sio.wait()
        except Exception as e:
            sys_logger.error('socket connect error - '+str(e))
            await sio.sleep(30)


if __name__ == '__main__':
    sys_logger.info('sensor initialization.')
    sensor_vib, sensor_temp = sensor_load(conf)
    main_loop = asyncio.get_event_loop()

    sys_logger.info('start background task.')
    sensor_task_vib = sio.start_background_task(sensor_loop_vib)
    sensor_task_temp = sio.start_background_task(sensor_loop_temp)

    try:
        sys_logger.info('start application.')
        main_loop.run_until_complete(socket_connect())
    except KeyboardInterrupt:
        sys_logger.info('Waiting for application shutdown.')
        sensor_task_vib.cancel()
        sensor_task_temp.cancel()
        sys_logger.info('Application shutdown complete.')
