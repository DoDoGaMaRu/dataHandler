import nidaqmx
import nidaqmx.system
from nidaqmx.constants import *
from enum import Enum, auto
from time import ctime, time


class DataType(Enum):
    VIB: int = auto()
    TEMP: int = auto()


class Sensor:
    def __init__(self):
        self.task = nidaqmx.Task()
        self.read_count = 1

    def add_vib_channel(self, channel: str):
        self.task.ai_channels.add_ai_voltage_chan(channel)

    def add_temp_channel(self, channel: str):
        self.task.ai_channels.add_ai_rtd_chan(channel, min_val=0.0, max_val=100.0, rtd_type=RTDType.PT_3750,
                                              resistance_config=ResistanceConfiguration.THREE_WIRE,
                                              current_excit_source=ExcitationSource.INTERNAL, current_excit_val=0.00100)

    def set_timing(self, rate: int, samples_per_channel: int):
        self.task.timing.cfg_samp_clk_timing(rate=rate,
                                             active_edge=nidaqmx.constants.Edge.RISING,
                                             sample_mode=nidaqmx.constants.AcquisitionType.CONTINUOUS,
                                             samps_per_chan=samples_per_channel)

    def set_sample_count(self, count: int):
        self.read_count = count

    @classmethod
    def of(cls, channel: str, rate: int, samples_per_channel: int, sensor_type: int):
        if sensor_type == DataType.VIB:
            return Sensor.vib(channel,
                              rate, samples_per_channel)
        elif sensor_type == DataType.TEMP:
            return Sensor.temp(channel,
                               rate, samples_per_channel)

    @classmethod
    def vib(cls, channel: str, rate: int, samples_per_channel: int):
        instance = cls()
        instance.add_vib_channel(channel)
        instance.set_timing(rate, samples_per_channel)
        instance.set_sample_count(rate)
        return instance

    @classmethod
    def temp(cls, channel: str, rate: int, samples_per_channel: int):
        instance = cls()
        instance.add_temp_channel(channel)
        instance.set_timing(rate, samples_per_channel)
        instance.set_sample_count(rate)
        return instance

    @classmethod
    def dual(cls, vib_channel: str, temp_channel: str, rate: int, buffer_size: int):
        instance = cls()
        instance.add_temp_channel(temp_channel)
        instance.add_vib_channel(vib_channel)
        instance.set_timing(rate, buffer_size)
        return instance

    async def try_read(self, server, event_name: str):
        now_time = ctime(time())
        data = self.task.read(number_of_samples_per_channel=self.read_count, timeout=10.0)

        await server.sleep(1)
        await server.emit(event_name, {'time': now_time, 'data': data})

    async def read(self, server, event_name: str):
        try:
            await self.try_read(server, event_name)
        except nidaqmx.errors.DaqReadError:
            pass
