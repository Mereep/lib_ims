import logging
from pathlib import Path
from typing import Iterable, Iterator
from lib_ims.db.ims_file import ImsFile


class ImsDatabase(Iterable):
    """
    Easy access to the IMS Database
    """

    """database base path"""
    path: Path
    recordings: list[ImsFile]

    def __init__(self, path: Path):
        assert path.is_dir(), "Path is no directory (Code: 39482093)"
        self.path = path
        self.refresh()

    def refresh(self):
        """
        will reload the file indices (use his on external changes)
        is called on initialization time
        :return:
        """
        self.recordings = [*self._load_file_index()]

    @property
    def players(self) -> set[str]:
        """ parses all unique player names in the DB"""
        return set(sess.player_id for sess in self.recordings)

    @property
    def tracks(self) -> set[str]:
        """ parses all unique tracks in the DB"""
        return set(sess.track_id for sess in self.recordings)

    @property
    def simulators(self) -> set[str]:
        """ parses all unique tracks in the DB"""
        return set(sess.simulator_id for sess in self.recordings)

    @property
    def n_recordings(self) -> int:
        """ how many single recordings are inside? Be aware this is not necessarily the same as sessions"""
        return len(self.recordings)

    def _load_file_index(self) -> Iterable[ImsFile]:
        for file_path in self.path.iterdir():
            if file_path.name[-4:] == 'json':
                abs_path_str = str(file_path.absolute())
                expected_csv_file_path = Path(abs_path_str[:-4] + 'csv')

                if expected_csv_file_path.exists():
                    yield ImsFile(path_telemetry=expected_csv_file_path, path_static=file_path)
                else:
                    logging.warning(f"Could not find telemetry file `{expected_csv_file_path!s}` "
                                    f"(Code: 983908023)")

    def __iter__(self) -> Iterator[ImsFile]:
        """iterates over all session files ordered by `start_time` """
        return iter(sorted(self.recordings, key=lambda sess: sess.start_time))
