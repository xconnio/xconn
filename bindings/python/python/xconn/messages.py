"""Message views for custom client authenticators and transport codecs."""

from dataclasses import dataclass

from . import _native


def call(name, *args):
    return _native.protocol(name, list(args))


class Message:
    def __init__(self, wire):
        self.TYPE = wire[0]
        self._wire = wire
        self.fields = call("parse", wire[0], wire)

    def marshal(self):
        return call("marshal", self._wire[0], self.fields)


def to_message(wire):
    return Message(wire)


@dataclass
class ChallengeFields:
    authmethod: str
    extra: dict


class Challenge:
    def __init__(self, fields):
        self.authmethod, self.extra = fields.authmethod, fields.extra

    def marshal(self):
        return call("marshal", 4, {"authmethod": self.authmethod, "extra": self.extra})


class Authenticate:
    def __init__(self, signature, extra):
        self.signature, self.extra = signature, extra
