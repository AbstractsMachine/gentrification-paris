"""Cartographie et figures."""
from .maps import plot_map, plot_multitemp
from .synthesis import plot_historical_maps, plot_level_typology, plot_trajectory

__all__ = [
    "plot_map",
    "plot_multitemp",
    "plot_level_typology",
    "plot_trajectory",
    "plot_historical_maps",
]
