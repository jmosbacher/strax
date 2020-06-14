"""Context methods for streamz """
import fnmatch
import re
import typing as ty

import numpy as np
import pandas as pd
from tqdm import tqdm
import itertools
import strax
import time
export, __all__ = strax.exporter()

class ProcessingPipeline:
    target: str
    streams: dict
    loaders: dict
    results: list
    
    def __init__(self, target, streams, loaders):
        self.target = target
        self.streams = streams
        self.loaders = loaders
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
        return any([chunk.start>=self.end for chunk in self.results]) or len(self.results)==self.submitted

    def get_array(self, timeout=20):
        if not self.submitted:
            self.submit()

        if self.ready():
            return strax.Chunk.concatenate(self.results).data
        print("Results not ready.")

    def visualize(self, filename="strax_pipeline.png", **kwargs):
        return self.streams[self.target].visualize(filename, **kwargs)

def picker(name):
    def f(x):
        return x[name]
    return f

@strax.Context.add_method
def build_pipeline(self, run_id, target, streams=None, save=False, dask=True, buffer=5):
    try:
        import streamz
    except ImportError:
        raise ImportError("You need to have streamz installed to use this functionality")
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
                streams[name] = stream.map(picker(name)) #.pluck(name, stream_name=f"{name}")
            plugins.pop(name)
            break
    if save:
        for name, savers in components.savers.items():
            for saver in savers:
                streams[name].gather().sink(saver.save_from)
    pipeline = ProcessingPipeline(target, streams, components.loaders)

    return pipeline