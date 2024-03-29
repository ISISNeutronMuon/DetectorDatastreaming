# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers
from flatbuffers.compat import import_numpy

np = import_numpy()


class RunStart(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAsRunStart(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = RunStart()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def RunStartBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(
            buf, offset, b"\x70\x6C\x37\x32", size_prefixed=size_prefixed
        )

    # RunStart
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # RunStart
    def StartTime(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(
                flatbuffers.number_types.Uint64Flags, o + self._tab.Pos
            )
        return 0

    # RunStart
    def StopTime(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(
                flatbuffers.number_types.Uint64Flags, o + self._tab.Pos
            )
        return 0

    # RunStart
    def RunName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def InstrumentName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def NexusStructure(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def JobId(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def Broker(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def ServiceId(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(18))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def Filename(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(20))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def NPeriods(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(22))
        if o != 0:
            return self._tab.Get(
                flatbuffers.number_types.Uint32Flags, o + self._tab.Pos
            )
        return 1

    # RunStart
    def DetectorSpectrumMap(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(24))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from .SpectraDetectorMapping import SpectraDetectorMapping

            obj = SpectraDetectorMapping()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # RunStart
    def Metadata(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(26))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStart
    def ControlTopic(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(28))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None


def RunStartStart(builder):
    builder.StartObject(13)


def RunStartAddStartTime(builder, startTime):
    builder.PrependUint64Slot(0, startTime, 0)


def RunStartAddStopTime(builder, stopTime):
    builder.PrependUint64Slot(1, stopTime, 0)


def RunStartAddRunName(builder, runName):
    builder.PrependUOffsetTRelativeSlot(
        2, flatbuffers.number_types.UOffsetTFlags.py_type(runName), 0
    )


def RunStartAddInstrumentName(builder, instrumentName):
    builder.PrependUOffsetTRelativeSlot(
        3, flatbuffers.number_types.UOffsetTFlags.py_type(instrumentName), 0
    )


def RunStartAddNexusStructure(builder, nexusStructure):
    builder.PrependUOffsetTRelativeSlot(
        4, flatbuffers.number_types.UOffsetTFlags.py_type(nexusStructure), 0
    )


def RunStartAddJobId(builder, jobId):
    builder.PrependUOffsetTRelativeSlot(
        5, flatbuffers.number_types.UOffsetTFlags.py_type(jobId), 0
    )


def RunStartAddBroker(builder, broker):
    builder.PrependUOffsetTRelativeSlot(
        6, flatbuffers.number_types.UOffsetTFlags.py_type(broker), 0
    )


def RunStartAddServiceId(builder, serviceId):
    builder.PrependUOffsetTRelativeSlot(
        7, flatbuffers.number_types.UOffsetTFlags.py_type(serviceId), 0
    )


def RunStartAddFilename(builder, filename):
    builder.PrependUOffsetTRelativeSlot(
        8, flatbuffers.number_types.UOffsetTFlags.py_type(filename), 0
    )


def RunStartAddNPeriods(builder, nPeriods):
    builder.PrependUint32Slot(9, nPeriods, 1)


def RunStartAddDetectorSpectrumMap(builder, detectorSpectrumMap):
    builder.PrependUOffsetTRelativeSlot(
        10, flatbuffers.number_types.UOffsetTFlags.py_type(detectorSpectrumMap), 0
    )


def RunStartAddMetadata(builder, metadata):
    builder.PrependUOffsetTRelativeSlot(
        11, flatbuffers.number_types.UOffsetTFlags.py_type(metadata), 0
    )


def RunStartAddControlTopic(builder, controlTopic):
    builder.PrependUOffsetTRelativeSlot(
        12, flatbuffers.number_types.UOffsetTFlags.py_type(controlTopic), 0
    )


def RunStartEnd(builder):
    return builder.EndObject()
