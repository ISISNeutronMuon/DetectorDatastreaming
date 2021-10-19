# automatically generated by the FlatBuffers compiler, do not modify

# namespace: 

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Int64Array(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsInt64Array(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Int64Array()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def Int64ArrayBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(buf, offset, b"\x73\x65\x6E\x76", size_prefixed=size_prefixed)

    # Int64Array
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Int64Array
    def Value(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int64Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 8))
        return 0

    # Int64Array
    def ValueAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int64Flags, o)
        return 0

    # Int64Array
    def ValueLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Int64Array
    def ValueIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

def Int64ArrayStart(builder): builder.StartObject(1)
def Int64ArrayAddValue(builder, value): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0)
def Int64ArrayStartValueVector(builder, numElems): return builder.StartVector(8, numElems, 8)
def Int64ArrayEnd(builder): return builder.EndObject()
