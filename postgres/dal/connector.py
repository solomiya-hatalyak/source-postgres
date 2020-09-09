from dataclasses import dataclass


@dataclass
class Connector:
    cursor: object
    connection: object
    loaded: int = 0
