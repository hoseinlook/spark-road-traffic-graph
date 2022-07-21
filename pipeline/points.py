import dataclasses
import geopy.distance
import numpy as np


@dataclasses.dataclass
class Point:
    lat: float
    long: float
    label: str

    @classmethod
    def distance(cls, point1: "Point", point2: "Point") -> int:
        """
        calculate distance between 2 points
        :param point1:
        :param point2:
        :return: distance in meters
        """

        coord_1 = (point1.lat, point1.long)
        coord_2 = (point2.lat, point2.long)
        return int(geopy.distance.geodesic(coord_1, coord_2).km * 1000)


# tehran coordinates

bottom_left = (35.68602496156509, 51.24860651228167)
bottom_right = (35.65696771116405, 51.479952120431854)
top_left = (35.75822235875991, 51.25812285644671)
top_right = (35.756358304600795, 51.479623970633064)

cols = np.linspace(bottom_left[1], bottom_right[1], num=100)
rows = np.linspace(bottom_left[0], top_left[0], num=100)

TEHRAN_POINTS = [Point(lat, long, F"POINT_{counter}") for counter, (lat, long) in enumerate((lat, long) for lat in rows for long in cols)]
