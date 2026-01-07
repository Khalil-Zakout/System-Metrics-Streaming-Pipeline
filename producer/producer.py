import psutil
import GPUtil
import wmi
import json
from kafka import KafkaProducer
import time

# Constants
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "machine_metrics"

# Object to interact with system data
w = wmi.WMI(namespace="root\\wmi")


def get_metrics():
    cpu_usage = psutil.cpu_percent()
    ram = psutil.virtual_memory()
    disk = psutil.disk_usage("C:\\")
    net = psutil.net_io_counters()
    cpu_sensor = w.MSAcpi_ThermalZoneTemperature()[0]
    gpu = GPUtil.getGPUs()[0]

    battery_full = w.BatteryFullChargedCapacity()[0]
    battery_static = w.BatteryStaticData()[0]
    battery_health = round(battery_full.FullChargedCapacity / battery_static.DesignedCapacity, 2) * 100

    return {
        "cpu_percent": cpu_usage,
        "ram_percent": ram.percent,
        "ram_used_bytes": ram.used,
        "disk_percent": disk.percent,
        "disk_used_bytes": disk.used,
        "cpu_temp_celsius": cpu_sensor.CurrentTemperature / 10 - 273.15,
        "gpu_temp_celsius": round(gpu.temperature, 1),
        "battery_health_percent": battery_health,
        "net_bytes_sent": net.bytes_sent,
        "net_bytes_recv": net.bytes_recv,
        "timestamp": int(time.time())
    }


def main():
    producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    linger_ms=100)  # if more messages arrive within 100 ms, batch them
    
    while True:
        metrics = get_metrics()
        producer.send(topic=KAFKA_TOPIC, value= metrics)
        print("Sent ✅")
        time.sleep(10)


if __name__ == "__main__":
    main()