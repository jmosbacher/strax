from streamz import Stream
from tornado import gen

from .chunk import Chunk


@Stream.register_api()
class chunk_by_time(Stream):
    """ rechunk by time interval
    """
    _graphviz_shape = 'diamond'

    def __init__(self, upstream, dt: int, overlap: int=0, **kwargs):
        self.buffer = None
        self.metadata_buffer = {}
        self.dt = int(dt)
        self.overlap = int(overlap)
        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        if self.buffer is None:
            self.buffer = x
        else:
            self.buffer = Chunk.concatenate([self.buffer, x])
        self.metadata_buffer[x.start] = metadata

    @gen.coroutine
    def cb(self):
        while True:
            if self.buffer is None:
                yield gen.sleep(0.1)
                continue
            dt = self.dt+self.overlap
#             if (self.buffer.end-self.buffer.start)<dt:
#                 yield gen.sleep(0.1)
#                 continue
            temit = self.buffer.start+dt
            if temit>self.buffer.end:
                x = self.buffer
                self.buffer = None
            else:
                x, _ = self.buffer.split(temit, allow_overlap=True)
                _, self.buffer = self.buffer.split(temit-self.overlap, allow_overlap=True)
            metadata = [m for t,m in self.metadata_buffer.items() if t<=temit]
            if len(metadata)==1:
                metadata = metadata[0]
            yield self._emit(x, metadata=metadata if metadata else None)
            if metadata:
                self._release_refs(metadata)
                