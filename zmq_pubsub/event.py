import json
from typing import List


class Event:

    def __init__(self, channel: str, header: str, body: dict):
        self.channel = channel
        self.body = body
        self.header = header

    def serialize(self):
        return [self.channel.encode() + b'\0', self.header.encode('charmap'), json.dumps(self.body).encode('charmap')]

    @classmethod
    def deserialize(cls, msg: List[bytes]):
        b_channel, b_header, b_body = msg
        return cls(b_channel[:-1].decode(), b_header.decode('charmap'), json.loads(b_body.decode('charmap')))

    def __str__(self):
        return '<{}> {}'.format(self.header, self.body)
