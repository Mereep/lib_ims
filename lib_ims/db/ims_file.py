import datetime
import json
from pathlib import Path

import pandas as pd


class ImsFile:
    """ Represents a database entry as a pair of telemetry data and static data"""

    path_telemetry: Path
    path_static: Path

    """ JSON representation of `path_static` """
    static_data: dict

    def __init__(self, path_telemetry: Path, path_static: Path):
        assert path_telemetry.exists() and path_static.exists(), "Please make sure the passed file do exist (Code: 482309)"

        self.path_telemetry = path_telemetry
        self.path_static = path_static
        self.prepare()

    def prepare(self):
        """ (re)initializes the data"""
        self.static_data = json.loads(self.path_static.read_bytes())

    @property
    def start_time(self) -> datetime.datetime:
        """ when the recording started?"""
        return datetime.datetime.fromisoformat(self.static_data['startTime'])

    @property
    def player_id(self) -> str:
        """ id of the player"""
        return self.static_data['playerId']

    @property
    def track_id(self) -> str:
        """ id of the played track"""
        return self.static_data['track']['name']

    @property
    def simulator_id(self) -> str:
        return self.static_data['simulator']

    @property
    def n_players(self) -> int:
        """ how many cars are on the track while driving"""
        return self.static_data['numCars']

    @property
    def is_with_ai_players(self) -> bool:
        """ checks if there was more than one player on the race"""
        return self.n_players > 1

    @property
    def telemetry(self) -> pd.DataFrame:
        return pd.read_csv(self.path_telemetry)
