"""Client codec interfaces; all wire encoding and validation run in Rust."""

from .messages import Message, call


class Serializer:
    codec = None

    def serialize(self, message):
        data = call("encode_wire", self.codec, message.marshal())
        return data.decode("utf-8") if self.codec == 1 else data

    def deserialize(self, data):
        if isinstance(data, str):
            data = data.encode("utf-8")

        name, fields = call("decode_message", self.codec, data)
        # Reconstruct through the native schema rather than Python message layouts.
        definitions = call("definitions")
        code = next(spec["code"] for spec in definitions if spec["class"] == name)
        return Message(call("marshal", code, fields))

    def static(self):
        return False


class JSONSerializer(Serializer):
    codec = 1


class MsgPackSerializer(Serializer):
    codec = 2


class CBORSerializer(Serializer):
    codec = 3
