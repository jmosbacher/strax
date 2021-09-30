import numpy as np
import streamz
from streamz import Stream, Source
from tornado import gen
from collections import defaultdict
import networkx as nx

from .chunk import Chunk
import strax
from strax.utils import exporter
export, __all__ = exporter()

@Stream.register_api()
class chunk_by_time(Stream):
    """ rechunk by time interval
    """
    _graphviz_shape = 'diamond'

    def __init__(self, upstream, dt: int, overlap: int=0, **kwargs):
        self._buffer = None
        self.metadata_buffer = {}
        self.dt = int(dt)
        self.overlap = int(overlap)
        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        if self._buffer is None:
            self._buffer = x
        else:
            self.buffer = Chunk.concatenate([self._buffer, x])
        self.metadata_buffer[x.start] = metadata

    @gen.coroutine
    def _flush(self):
        x = self._buffer
        self._buffer = None
        metadata = list(self.metadata_buffer.values())
        self.metadata_buffer = {}
        yield self._emit(x, metadata=metadata if metadata else None)
        if metadata:
            self._release_refs(metadata)

    @gen.coroutine
    def cb(self):
        while True:
            if self._buffer is None:
                yield gen.sleep(0.1)
                continue
            dt = self.dt+self.overlap
#             if (self.buffer.end-self.buffer.start)<dt:
#                 yield gen.sleep(0.1)
#                 continue
            temit = self._buffer.start+dt
            if temit>self._buffer.end:
                x = self._buffer
                self._buffer = None
            else:
                x, _ = self._buffer.split(temit, allow_overlap=True)
                x.end -= self.overlap
                _, self._buffer = self._buffer.split(temit-self.overlap, allow_overlap=True)
            metadata = [m for t,m in self.metadata_buffer.items() if t<=temit]                
            if len(metadata)==1:
                metadata = metadata[0]
            yield self._emit(x, metadata=metadata if metadata else None)
            if metadata:
                self._release_refs(metadata)

@export     
def chunks_to_kwargs(chunks, add_start_end=False):
    if isinstance(chunks, Chunk):
        chunks = (chunks,)

    by_kind = defaultdict(list)
    for c in chunks:
        start, end = c.start, c.end
        by_kind[c.data_kind].append(c)

    kwargs = {k: Chunk.merge(cs).data for k,cs in by_kind.items()}
    if add_start_end:
        kwargs['start'] = start
        kwargs['end'] = end
    return kwargs

def add_chunk_i(chunk_i, kwargs):
    kwargs['chunk_i'] = chunk_i
    return chunk_i+1, kwargs

@export
class PluginRunner:
    def __str__(self):
        return self.plugin.__class__.__name__
    
    def __init__(self, plugin):
        plugin.log = None
        plugin.deps = None
        plugin.nn = None
        self.plugin = plugin
        self._setup = False
        
    def __reduce__(self):
        return PluginRunner, (self.plugin,)
        
    def empty_result(self):
        if self.plugin.multi_output:
            result = {}
            for r in self.plugin.provides:
                dtype=self.plugin.dtype_for(r)
                result[r] = np.empty(shape=(0,), dtype=dtype)
        else:
            result = np.empty(shape=(0,), dtype=self.plugin.dtype)
        return result
            
    def __call__(self, kwargs):
        if not self._setup:
            self.plugin.setup()
            self._setup = True
        # print(self.plugin.__class__.__name__)
        start, end = kwargs.pop('start', None), kwargs.pop('end', None)
        if self.plugin.compute_takes_start_end:
            kwargs['start'] = start
            kwargs['end'] = end
        if any([len(a)==0 for a in kwargs.values() if isinstance(a, np.ndarray)]):
            result = self.empty_result()
        else:
            result = self.plugin.compute(**kwargs)
        
        return self.plugin._fix_output(result, start, end)

@export
class OverlapPluginRunner(PluginRunner):
    def __call__(self, state, kwargs):
        for k,v in state.items():
            setattr(self.plugin, k,v)
        self.plugin.__dict__.update(state)
        result = super().__call__(kwargs)
        state = dict(self.plugin.__getstate__())
        state.pop('log', None)
        state.pop('nn', None)
        state.pop('deps', None)
        return state, result
    
    def __reduce__(self):
        return OverlapPluginRunner, (self.plugin,)

@export
class SaveChunksStream:
    def __init__(self, savers):
        self.savers = savers

    def __call__(self, chunk_i, chunk):
        for saver in self.savers:
            saver.save(chunk, chunk_i)
        return chunk_i+1, chunk_i
@export
def create_streams(components, scatter=True):
    sources = {name: Source.from_iterable(loader()) for name,loader in components.loaders.items()}
    plugin_dag = nx.DiGraph()
    for data_type,plugin in components.plugins.items():
        for d in plugin.depends_on:
            plugin_dag.add_edge(d, data_type)
    order = list(nx.topological_sort(plugin_dag))
    streams = dict(sources)
    for data_type in order:
        if data_type in streams:
            continue
        plugin = components.plugins[data_type]
        plugin_name = plugin.__class__.__name__
        dep_streams = [streams[d] for d in plugin.depends_on]
        if isinstance(plugin, strax.OverlapWindowPlugin):
            overlap = plugin.get_window_size()
            chunk_length = 500*overlap
            dep_streams = [s.gather().chunk_by_time(chunk_length, overlap=overlap) for s in dep_streams]

        if len(dep_streams)>1:
            dep_stream = streamz.zip(*dep_streams)
        else:
            dep_stream = dep_streams[0]
        if scatter:
            dep_stream = dep_stream.scatter()
        kwargs_stream = dep_stream.map(chunks_to_kwargs, add_start_end=True).buffer(10).gather()
        if plugin.compute_takes_chunk_i:
            kwargs_stream = kwargs_stream.accumulate(add_chunk_i, start=0, returns_state=True)
        if scatter:
            kwargs_stream = kwargs_stream.scatter()
            
        if isinstance(plugin, strax.OverlapWindowPlugin):
            runner = strax.OverlapPluginRunner(plugin)
            state = dict(runner.__dict__)
            state.pop('log', None)
            state.pop('nn', None)
            state.pop('deps', None)
            results_stream = kwargs_stream.accumulate(runner, start={}, returns_state=True).buffer(10).gather()
        else:
            runner = PluginRunner(plugin)
            results_stream = kwargs_stream.map(runner).buffer(10).gather()
        if plugin.multi_output:
            for d in plugin.provides:
                streams[d] = results_stream.gather().pluck(d)
        else:
            streams[data_type] = results_stream
    for data_type, savers in components.savers.items():
        runner = SaveChunksStream(savers)
        streams[data_type+'_saved_chunks'] = streams[data_type].accumulate(runner, start=0, returns_state=True).sink_to_list()
    return streams