# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers


class ULong(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAsULong(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = ULong()
        x.Init(buf, n + offset)
        return x

    # ULong
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # ULong
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(
                flatbuffers.number_types.Uint64Flags, o + self._tab.Pos
            )
        return 0


def ULongStart(builder):
    builder.StartObject(1)


def ULongAddValue(builder, value):
    builder.PrependUint64Slot(0, value, 0)


def ULongEnd(builder):
    return builder.EndObject()
