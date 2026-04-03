"""HardwareProvider — generates fake computer hardware data.

Includes CPU models, GPU models, RAM sizes, storage types,
motherboard form factors, peripheral devices, and hardware specs.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from dataforge.providers.base import BaseProvider

# Data tuples (immutable, module-level for zero per-call overhead)

_CPU_MODELS: tuple[str, ...] = (
    "Core i9-14900K",
    "Core i7-14700K",
    "Core i5-14600K",
    "Core i3-14100",
    "Core i9-13900K",
    "Core i7-13700K",
    "Ryzen 9 7950X",
    "Ryzen 7 7700X",
    "Ryzen 5 7600X",
    "Ryzen 9 7900X",
    "Ryzen Threadripper 7980X",
    "Ryzen 5 5600X",
    "Apple M3 Max",
    "Apple M3 Pro",
    "Apple M3",
    "Apple M2 Ultra",
    "Apple M2 Max",
    "Snapdragon 8 Gen 3",
    "Xeon W-3400",
    "EPYC 9654",
)

_GPU_MODELS: tuple[str, ...] = (
    "NVIDIA RTX 4090",
    "NVIDIA RTX 4080 Super",
    "NVIDIA RTX 4070 Ti Super",
    "NVIDIA RTX 4070 Super",
    "NVIDIA RTX 4060 Ti",
    "NVIDIA RTX 4060",
    "NVIDIA RTX 3090 Ti",
    "NVIDIA RTX 3080 Ti",
    "NVIDIA RTX 3070 Ti",
    "AMD RX 7900 XTX",
    "AMD RX 7900 XT",
    "AMD RX 7800 XT",
    "AMD RX 7700 XT",
    "AMD RX 7600",
    "AMD RX 6950 XT",
    "Intel Arc A770",
    "Intel Arc A750",
    "Intel Arc A580",
    "NVIDIA A100",
    "NVIDIA H100",
)

_RAM_SIZES: tuple[str, ...] = (
    "4 GB",
    "8 GB",
    "16 GB",
    "32 GB",
    "64 GB",
    "128 GB",
    "256 GB",
    "512 GB",
    "1 TB",
    "2 TB",
)

_RAM_TYPES: tuple[str, ...] = (
    "DDR4-2400",
    "DDR4-2666",
    "DDR4-3000",
    "DDR4-3200",
    "DDR4-3600",
    "DDR5-4800",
    "DDR5-5200",
    "DDR5-5600",
    "DDR5-6000",
    "DDR5-6400",
    "DDR5-7200",
    "DDR5-8000",
    "LPDDR5-6400",
    "LPDDR5X-7500",
    "GDDR6",
)

_STORAGE_TYPES: tuple[str, ...] = (
    "256 GB NVMe SSD",
    "512 GB NVMe SSD",
    "1 TB NVMe SSD",
    "2 TB NVMe SSD",
    "4 TB NVMe SSD",
    "256 GB SATA SSD",
    "512 GB SATA SSD",
    "1 TB SATA SSD",
    "1 TB HDD",
    "2 TB HDD",
    "4 TB HDD",
    "8 TB HDD",
    "12 TB HDD",
    "16 TB HDD",
    "1 TB PCIe 5.0 SSD",
)

_FORM_FACTORS: tuple[str, ...] = (
    "ATX",
    "Micro-ATX",
    "Mini-ITX",
    "E-ATX",
    "XL-ATX",
    "Nano-ITX",
    "Pico-ITX",
    "SSI-CEB",
    "SSI-EEB",
    "FlexATX",
)

_PERIPHERALS: tuple[str, ...] = (
    "Mechanical Keyboard",
    "Wireless Mouse",
    "Gaming Headset",
    "Webcam",
    'Monitor 27"',
    'Monitor 32"',
    'Monitor 34" Ultrawide',
    "USB Hub",
    "External SSD",
    "Docking Station",
    "Microphone",
    "Speakers",
    "Drawing Tablet",
    "Game Controller",
    "VR Headset",
    "Trackball",
    "Ergonomic Keyboard",
    "Stream Deck",
    "Capture Card",
    "KVM Switch",
)

_MANUFACTURERS: tuple[str, ...] = (
    "ASUS",
    "MSI",
    "Gigabyte",
    "ASRock",
    "EVGA",
    "Corsair",
    "Kingston",
    "G.Skill",
    "Samsung",
    "Western Digital",
    "Seagate",
    "Crucial",
    "NZXT",
    "Cooler Master",
    "be quiet!",
    "Lian Li",
    "Seasonic",
    "Thermaltake",
    "Razer",
    "Logitech",
)

_PORTS: tuple[str, ...] = (
    "USB-A 3.0",
    "USB-A 3.2",
    "USB-C 3.2",
    "USB-C 4.0",
    "Thunderbolt 4",
    "Thunderbolt 5",
    "HDMI 2.1",
    "DisplayPort 2.1",
    "Ethernet RJ45",
    "3.5mm Audio",
    "USB-C PD",
    "Lightning",
    "PCIe x16",
    "PCIe x4",
    "M.2 NVMe",
)


class HardwareProvider(BaseProvider):
    """Generates fake computer hardware data.

    This provider is locale-independent.

    Parameters
    ----------
    engine : RandomEngine
        The shared random engine instance.
    """

    __slots__ = ()

    _provider_name = "hardware"
    _locale_modules: tuple[str, ...] = ()
    _field_map: dict[str, str] = {
        "cpu": "cpu",
        "cpu_model": "cpu",
        "gpu": "gpu",
        "gpu_model": "gpu",
        "ram_size": "ram_size",
        "ram_type": "ram_type",
        "storage": "storage",
        "storage_type": "storage",
        "form_factor": "form_factor",
        "peripheral": "peripheral",
        "hw_manufacturer": "manufacturer",
        "hw_port": "port",
        "port_type": "port",
    }

    _choice_fields: dict[str, tuple[str, ...]] = {
        "cpu": _CPU_MODELS,
        "gpu": _GPU_MODELS,
        "ram_size": _RAM_SIZES,
        "ram_type": _RAM_TYPES,
        "storage": _STORAGE_TYPES,
        "form_factor": _FORM_FACTORS,
        "peripheral": _PERIPHERALS,
        "manufacturer": _MANUFACTURERS,
        "port": _PORTS,
    }
