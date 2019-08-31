import json
from typing import List


class Event:

    def __init__(self, channel: str, header: str, body: dict):
        self.channel = channel
        self.body = body
        self.header = header

    def to_bytes_list(self):
        return [self.channel.encode() + b'\0', self.header.encode('charmap'), json.dumps(self.body).encode('charmap')]

    def to_str_list(self):
        return ['{}\0'.format(self.channel), self.header, json.dumps(self.body)]

    @classmethod
    def from_bytes(cls, msg: List[bytes]):
        b_channel, b_header, b_body = msg
        return cls(b_channel[:-1].decode(), b_header.decode('charmap'), json.loads(b_body.decode('charmap')))

    @classmethod
    def from_str(cls, msg: List[str]):
        channel, header, body = msg
        return cls(channel[:-1], header, json.loads(body))

    def __str__(self):
        return '<{}> {}'.format(self.header, self.body)
