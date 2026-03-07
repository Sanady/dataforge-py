"""HardwareProvider — generates fake computer hardware data.

Includes CPU models, GPU models, RAM sizes, storage types,
motherboard form factors, peripheral devices, and hardware specs.
All data is stored as immutable ``tuple[str, ...]`` for cache friendliness.
"""

from typing import Literal, overload

from dataforge.providers.base import BaseProvider

# ------------------------------------------------------------------
# Data tuples (immutable, module-level for zero per-call overhead)
# ------------------------------------------------------------------

_CPU_BRANDS: tuple[str, ...] = (
    "Intel",
    "AMD",
    "Apple",
    "Qualcomm",
    "ARM",
    "NVIDIA",
    "Samsung",
    "MediaTek",
    "IBM",
    "RISC-V",
)

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
    "Core Ultra 9 285K",
    "Ryzen AI 9 HX 370",
    "Dimensity 9300",
    "Snapdragon X Elite",
    "A17 Pro",
    "Core i9-12900K",
    "Ryzen 9 5950X",
    "Xeon Platinum 8480+",
    "EPYC 7763",
    "Core i7-12700K",
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
    "NVIDIA L40S",
    "AMD Instinct MI300X",
    "Apple M3 Max GPU",
    "NVIDIA RTX 5090",
    "NVIDIA RTX 5080",
    "AMD RX 9070 XT",
    "NVIDIA Quadro RTX 6000",
    "AMD Radeon Pro W7900",
    "NVIDIA T4",
    "NVIDIA V100",
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
    "GDDR6X",
    "HBM2e",
    "HBM3",
    "HBM3e",
    "ECC DDR5-4800",
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
    "2 TB PCIe 5.0 SSD",
    "4 TB PCIe 5.0 SSD",
    "256 GB eMMC",
    "128 GB UFS 4.0",
    "512 GB UFS 4.0",
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
    "SteelSeries",
    "HyperX",
    "Sabrent",
    "Noctua",
    "EKWB",
    "Fractal Design",
    "Phanteks",
    "DeepCool",
    "Arctic",
    "Silverstone",
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
    "SATA III",
    "Mini DisplayPort",
    "DVI-D",
    "VGA",
    "Wi-Fi 7",
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @overload
    def cpu(self) -> str: ...
    @overload
    def cpu(self, count: Literal[1]) -> str: ...
    @overload
    def cpu(self, count: int) -> str | list[str]: ...
    def cpu(self, count: int = 1) -> str | list[str]:
        """Generate a CPU model (e.g. ``"Ryzen 9 7950X"``).

        Parameters
        ----------
        count : int
            Number of CPU models to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_CPU_MODELS)
        return self._engine.choices(_CPU_MODELS, count)

    @overload
    def gpu(self) -> str: ...
    @overload
    def gpu(self, count: Literal[1]) -> str: ...
    @overload
    def gpu(self, count: int) -> str | list[str]: ...
    def gpu(self, count: int = 1) -> str | list[str]:
        """Generate a GPU model (e.g. ``"NVIDIA RTX 4090"``).

        Parameters
        ----------
        count : int
            Number of GPU models to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_GPU_MODELS)
        return self._engine.choices(_GPU_MODELS, count)

    @overload
    def ram_size(self) -> str: ...
    @overload
    def ram_size(self, count: Literal[1]) -> str: ...
    @overload
    def ram_size(self, count: int) -> str | list[str]: ...
    def ram_size(self, count: int = 1) -> str | list[str]:
        """Generate a RAM size (e.g. ``"32 GB"``).

        Parameters
        ----------
        count : int
            Number of RAM sizes to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_RAM_SIZES)
        return self._engine.choices(_RAM_SIZES, count)

    @overload
    def ram_type(self) -> str: ...
    @overload
    def ram_type(self, count: Literal[1]) -> str: ...
    @overload
    def ram_type(self, count: int) -> str | list[str]: ...
    def ram_type(self, count: int = 1) -> str | list[str]:
        """Generate a RAM type (e.g. ``"DDR5-5600"``).

        Parameters
        ----------
        count : int
            Number of RAM types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_RAM_TYPES)
        return self._engine.choices(_RAM_TYPES, count)

    @overload
    def storage(self) -> str: ...
    @overload
    def storage(self, count: Literal[1]) -> str: ...
    @overload
    def storage(self, count: int) -> str | list[str]: ...
    def storage(self, count: int = 1) -> str | list[str]:
        """Generate a storage specification (e.g. ``"1 TB NVMe SSD"``).

        Parameters
        ----------
        count : int
            Number of storage specs to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_STORAGE_TYPES)
        return self._engine.choices(_STORAGE_TYPES, count)

    @overload
    def form_factor(self) -> str: ...
    @overload
    def form_factor(self, count: Literal[1]) -> str: ...
    @overload
    def form_factor(self, count: int) -> str | list[str]: ...
    def form_factor(self, count: int = 1) -> str | list[str]:
        """Generate a motherboard form factor (e.g. ``"ATX"``).

        Parameters
        ----------
        count : int
            Number of form factors to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_FORM_FACTORS)
        return self._engine.choices(_FORM_FACTORS, count)

    @overload
    def peripheral(self) -> str: ...
    @overload
    def peripheral(self, count: Literal[1]) -> str: ...
    @overload
    def peripheral(self, count: int) -> str | list[str]: ...
    def peripheral(self, count: int = 1) -> str | list[str]:
        """Generate a peripheral device (e.g. ``"Mechanical Keyboard"``).

        Parameters
        ----------
        count : int
            Number of peripherals to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_PERIPHERALS)
        return self._engine.choices(_PERIPHERALS, count)

    @overload
    def manufacturer(self) -> str: ...
    @overload
    def manufacturer(self, count: Literal[1]) -> str: ...
    @overload
    def manufacturer(self, count: int) -> str | list[str]: ...
    def manufacturer(self, count: int = 1) -> str | list[str]:
        """Generate a hardware manufacturer (e.g. ``"ASUS"``).

        Parameters
        ----------
        count : int
            Number of manufacturer names to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_MANUFACTURERS)
        return self._engine.choices(_MANUFACTURERS, count)

    @overload
    def port(self) -> str: ...
    @overload
    def port(self, count: Literal[1]) -> str: ...
    @overload
    def port(self, count: int) -> str | list[str]: ...
    def port(self, count: int = 1) -> str | list[str]:
        """Generate a port/connector type (e.g. ``"USB-C 4.0"``).

        Parameters
        ----------
        count : int
            Number of port types to generate.

        Returns
        -------
        str or list[str]
        """
        if count == 1:
            return self._engine.choice(_PORTS)
        return self._engine.choices(_PORTS, count)
