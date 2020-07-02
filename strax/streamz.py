"""Context methods for streamz """
import fnmatch
import re
import typing as ty

import numpy as np
import pandas as pd
from tqdm import tqdm
import itertools
from collections import defaultdict
import strax
import time
from tornado.locks import Condition
import streamz
from streamz import Stream, DaskStream
from distributed.client import default_client

export, __all__ = strax.exporter()

class Loader:
    storage: strax.StorageFrontend
    data_key: str
    find_options: dict

    def load(self, chunk=None):
        try:
            # Partial is clunky... but allows specifying executor later
            # Since it doesn't run until later, we must do a find now
            # that we can still handle DataNotAvailable
            chunks = tuple(self.storage.loader(self.key, chunk_number=chunk))
            return chunks
        except strax.DataNotAvailable:
            return ()
            
class ProcessingPipeline:
    target: str
    streams: dict
    loaders: dict
    results: list
    
    def __init__(self, target, streams, loaders):
        self.target = target
        self.streams = streams
        self.loaders = loaders
        self.results = []
        self.submitted = False
        self.end = float("0")
    
    def submit(self, start=0, end=float("inf")):
        generators = {name: loader() for name, loader in self.loaders.items()}
        self.results = self.streams[self.target].gather().sink_to_list()
        
        for i in itertools.count():
            if i<start:
                continue
            if i>=end:
                self.submitted = i
                print(f"Submitted {i+1-start} chunks.")
                break

            for name, gen in generators.items():
                try:
                    chunk = next(gen)
                    self.streams[name].emit(chunk)
                    self.end = max(chunk.start-1, self.end )
                
                except StopIteration:
                    end = i
                    print("Submitted all chunks.")
                    break

            if self.submitted:
                break

        self.submitted = i+1-start
        return self.results

    def ready(self):
        return any([chunk.end>=self.end for chunk in self.results]) or len(self.results)==self.submitted

    def get_iter(self, start=0, end=float("inf")):
        generators = {name: loader() for name, loader in self.loaders.items()}
        collector = self.streams[self.target].gather().collect()
        yielded = 0
        for i in itertools.count():
            if i<start:
                continue
            if i>=end:
                self.submitted = i
                print(f"Submitted {i+1-start} chunks.")
                break

            for name, gen in generators.items():
                try:
                    chunk = next(gen)
                    self.streams[name].emit(chunk)
                    self.end = max(chunk.start-1, self.end)
                
                except StopIteration:
                    end = i
            results = collector.flush()
            if results:
                yielded += len(results)
                yield strax.Chunk.concatenate(sorted(results, key=lambda x: x.start)).data

        for _ in range(10):
            # for name in generators:
            #     self.streams[name].emit(())
            results = collector.flush()
            if results:
                yielded += len(results)
                yield strax.Chunk.concatenate(sorted(results, key=lambda x: x.start)).data
                time.sleep(1)
        print(f"yielded {yielded}")
        self.submitted = i+1-start
        
    def get_array(self, start=0, end=float("inf")):
        results = list(self.get_iter(start=start, end=end))
        if results:
            return strax.Chunk.concatenate(sorted(results, key=lambda x: x.start)).data

    def visualize(self, filename="strax_pipeline.png", **kwargs):
        return self.streams[self.target].visualize(filename, **kwargs)

def picker(name):
    def f(x,*args, **kwargs):
        return x[name]
    f.__name__ = f"Select {name}"
    return f


@strax.Context.add_method
def build_pipeline(self, run_id, target, streams=None, save=False, dask=True, buffer=5):
    if streams is None:
        streams = {}

    components = self.get_components(run_id, target)
    
    streams = {name: streamz.Stream(stream_name=name) for name in components.loaders}
    plugins = dict(components.plugins)

    for _ in range(len(plugins)**2):
        if not len(plugins):
            break
        for name, p in plugins.items():
            if not all([dep in streams for dep in p.depends_on]):
                continue
            stream = p.stream(*[streams[dep] for dep in p.depends_on], dask=dask, buffer=buffer)
            if p.multi_output:
                for prov in p.provides:
                    streams[prov] = stream.map(picker(prov)) #.pluck(prov, stream_name=f"{prov}")
            else:
                streams[name] = stream #.map(picker(name), stream_name=name) #.pluck(name, stream_name=f"{name}")
            plugins.pop(name)
            break
    else:
        raise ValueError("Not possible to produce requested data with existing plugins, data and constraints.")
    if save:
        for name, savers in components.savers.items():
            for saver in savers:
                streams[name].gather().sink(saver.save_from)
    pipeline = ProcessingPipeline(target, streams, components.loaders)

    return pipeline

@Stream.register_api()
class zip_chunks(Stream):
    """ Combine streams together into a stream of dicts
    We emit a new dict once all streams can be split at the same timestamp.
    See also
    --------
    """
    _graphviz_orientation = 180
    _graphviz_shape = 'triangle'

    def __init__(self, *upstreams, **kwargs):
        self.minsize = kwargs.pop('minsize', 1)
        self.maxsize = kwargs.pop('maxsize', float("inf"))
        self.minlength = kwargs.pop('minlength', 1)
        self.maxlength = kwargs.pop('maxlength', float("inf"))
        self.minduration = kwargs.pop('minduration', 1e9)
        self.maxduration = kwargs.pop('maxduration', float("inf"))     
        self.condition = Condition()
        self.literals = [(i, val) for i, val in enumerate(upstreams)
                         if not isinstance(val, Stream)]

        self.buffers = {upstream: None
                        for upstream in upstreams
                        if isinstance(upstream, Stream)}
        self.meta_buffer = {}
        upstreams2 = [upstream for upstream in upstreams if isinstance(upstream, Stream)]

        Stream.__init__(self, upstreams=upstreams2, **kwargs)

    def _add_upstream(self, upstream):
        # Override method to handle setup of buffer for new stream
        self.buffers[upstream] = None
        super(zip, self)._add_upstream(upstream)

    def _remove_upstream(self, upstream):
        # Override method to handle removal of buffer for stream
        self.buffers.pop(upstream)
        super(zip, self)._remove_upstream(upstream)

    @staticmethod
    def merge_by_kind(chunks):
        by_kind = defaultdict(list)
        for chunk in chunks:
            by_kind[chunk.data_kind].append(chunk)
        merged = {data_kind: strax.Chunk.merge(chunks) for data_kind, chunks in by_kind.items()}
        return merged

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        self.meta_buffer[x.start] = metadata
        self.buffers[who] = strax.Chunk.concatenate([self.buffers[who], x])

        if any([c is None for c in self.buffers.values()]):
            return
        
        _end = strax.find_splittable_end(self.buffers.values())
        outputs = []
        new_buffer = {}
        for stream, chunk in self.buffers.items():
            try:
                output, new_buffer[stream] = chunk.split(t=_end, allow_early_split=False)
                if output.end-output.start < self.minduration:
                    break
                if output.nbytes < self.minsize:
                    break
                if len(output.data) < self.minlength:
                    break
                outputs.append(output)
            except strax.CannotSplit:
                break
        else:
            self.buffers = new_buffer
            # merged = self.merge_by_kind(outputs)
            self.condition.notify_all()
            md = [m for start,m in self.meta_buffer.items() if start<_end]
            ret = self._emit(outputs, md)
            self._release_refs(md)
            return ret

        if self.buffers[who].end-self.buffers[who].start > self.maxduration:
            return self.condition.wait()

        if self.buffers[who].nbytes >= self.maxsize:
            return self.condition.wait()

        if len(self.buffers[who].data) >= self.maxlength:
            return self.condition.wait()

@DaskStream.register_api()
class zip_dask_chunks(DaskStream, zip_chunks):
    def update(self, x, who=None, metadata=None):
        client = default_client()
        result = client.submit(super().update, who, x, **self.kwargs)
        if self.returns_state:
            state = client.submit(getitem, result, 0)
            result = client.submit(getitem, result, 1)
        else:
            state = result
        self.state = state
        return self._emit(result, metadata=metadata)

# class ChunkBuffer:
#     def __init__(self):
#         self.chunks = {}

#     def insert(self, new_chunk):
#         for trange, chunk in self.chunks.items():
#             if  

# class ChunkBufferGroup:
    
#     def __init__(self, data_types, duration_range, len_range, size_range):
#         self.duration_range = duration_range
#         self.len_range = len_range
#         self.size_range = size_range
#         self.buffer = {name: {} for name in data_types}
    
#     def concatenate_buffer(self, buffer):
#         unique = []
#         merged = []
#         for chunk1 in sorted(buffer, key=lambda x: x.start):
#             for chunk2 in buffer:
#                 if chunk1.end==chunk2.start:
#                     unique.append(strax.Chunk.concatenate([chunk1, chunk2]))
#                     merged.append(chunk2)
#                     break
#             else:
#                 if chunk1 not in merged:
#                     unique.append(chunk1)
#         if len(unique) == len(buffer):
#             return unique
#         else:
#             return concatenate_buffer(unique)

#     def update(self, new_chunk):
#         self.buffer[new_chunk.data_type]= self.concatenate_buffer(
#                                 self.buffer[new_chunk.data_type]+[new_chunk])
        
#     def get_chunk(self):
#         _start = max([cs[0].start for cs in self.buffer.values()])
#         overlapping = []
#         for chunks in self.buffer.values():
#             for chunk in chunks:
#                 if chunk.start <= _start and chunk.end > _start:
#                     overlapping.append(chunk)
#                     break
#             else:
#                 return
#         _end = max([chunk.end for chunk in overlapping])

#         if _end - _start < self.minduration:
#             return
        
#         for chunk in overlapping:
            
#             if chunk.nbytes < self.minsize:
#                 return
#             if len(chunk.data) < self.minlength:
#                 return

#         outputs = []
#         leftovers = []
#         for chunk in overlapping:
#             try:
#                 output, leftover = chunk.split(t=_end, allow_early_split=False)
#                 if output.end==output.start:
#                     break
#                 outputs.append(output)
#                 leftovers.append(leftover)
#             except strax.CannotSplit:
#                 break
#         for chunk in leftovers:
#             self.update(chunk)

#     def largest_overlap(self):
#         buffers = list(self.buffer.values())
#         first = buffers[0]
#         max_start = 0
#         max_end = 0
#         for chunk1 in first:
#             for rest in zip(*buffers[1:]):
#                 start = max([c.start for c in rest])
#                 end = min([c.end for c in rest])
#                 if end-start > max_end-max_start:
#                     max_start = start
#                     max_end = end
#         return max_start, max_end

#     def get_overlaps(self):
#         overlaps = {}

