from palm.palm import ProtoBase, is_string, RepeatedSequence, ProtoValueError

_PB_type = type
_PB_finalizers = []



for cname in _PB_finalizers:
    eval(cname)._pbf_finalize()

del _PB_finalizers
