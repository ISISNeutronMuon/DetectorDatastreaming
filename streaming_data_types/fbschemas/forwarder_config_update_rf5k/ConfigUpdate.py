# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers


class ConfigUpdate(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAsConfigUpdate(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = ConfigUpdate()
        x.Init(buf, n + offset)
        return x

    # ConfigUpdate
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # ConfigUpdate
    def ConfigChange(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(
                flatbuffers.number_types.Uint16Flags, o + self._tab.Pos
            )
        return 0

    # ConfigUpdate
    def Streams(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from .Stream import Stream

            obj = Stream()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # ConfigUpdate
    def StreamsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0


def ConfigUpdateStart(builder):
    builder.StartObject(2)


def ConfigUpdateAddConfigChange(builder, configChange):
    builder.PrependUint16Slot(0, configChange, 0)


def ConfigUpdateAddStreams(builder, streams):
    builder.PrependUOffsetTRelativeSlot(
        1, flatbuffers.number_types.UOffsetTFlags.py_type(streams), 0
    )


def ConfigUpdateStartStreamsVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def ConfigUpdateEnd(builder):
    return builder.EndObject()
