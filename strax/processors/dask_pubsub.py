from dask.distributed import client, Sub, Pub, get_client, wait, fire_and_forget, as_completed
import typing as ty
import strax
import logging
from collections import defaultdict

from .base import ProcessorComponents, BaseProcessor

logger = logging.getLogger(__name__)

MAX_PASSES = 10
PLUGIN_CONFIGS = ["run_id", "config", "lineage", "data_kind","provides", "depends_on"]

def clean_deps(p):
    pnew = p.__class__()
    for k in PLUGIN_CONFIGS:
        setattr(pnew, k, getattr(p, k))
    pnew.deps = {k: clean_deps(p) for k,p in p.deps.items()}
    pnew.fix_dtype()
    return pnew

def merge_by_kind(chunks):
    kinds = defaultdict(list)
    for chunk in chunks:
        kinds[chunk.data_kind].append(chunk)
    results = {kind: strax.Chunk.merge(chunks) for kind,chunks in kinds.items()}
    return results

class PoisonPill:
    exception: Exception
    creator: str
    acknowledged: ty.List

class DaskPluginWorker:
    name: str
    stateful: bool
    plugin: strax.Plugin
    buffers: ty.Dict
    subscriptions: ty.Dict[str,Sub]
    publications: ty.Dict[str,Pub]

    def __init__(self, plugin):
        self.plugin = plugin
        self.plugin.setup()

        self.name = plugin.__class__.__name__ + "Worker"

        self.subscriptions = {
            name: Sub(name) for name in plugin.depends_on
        }

        self.publications = {
            name: Pub(name) for name in plugin.provides
        }

        self.buffers = {t: None for t in self.subscriptions}
        self.chunk_counter = 0
        self.exausted = []

    def priority(self, name):
        buffer = self.buffers[name]
        if isinstance(buffer,strax.Chunk):
            return buffer.end
        else:
            return 0

    @property
    def shortest_buffer(self):
        return min(self.buffers, key=self.priority)

    @property   
    def tmin(self):
        return self.buffers[self.shortest_buffer].end

    def buffers_by_length(self):
        return sorted(self.buffers, key=self.priority)

    def main_loop(self):
        while True:
            if all([name in self.exausted for name in self.subscriptions]):
                break
            for name in self.buffers_by_length():
            # name = self.shortest_buffer
                try:
                    chunk = self.subscriptions[name].get(timeout=1)
                    break
                except Exception:
                    pass
            else:
                self.process_if_ready()
                continue

            if isinstance(chunk, PoisonPill):
                self.exausted.append(name)
                continue
            print(self.name, ": ",chunk.data_kind, ",", chunk.data_type)

            self.append_chunk(name, chunk)
            self.process_if_ready()
        self.cleanup()
        self.kill_subscribers()

    def append_chunk(self, name, chunk):
        try:
            if self.buffers[name] is None:
                self.buffers[name] = chunk
            elif self.buffers[name].end<=chunk.start:
                self.buffers[name] = strax.Chunk.concatenate([self.buffers[name], chunk])
        except Exception as e:
            self.report_exception(e)

    def process_if_ready(self):
        if any([b is None for b in self.buffers.values()]):
            return
        
        try:
            new_buffers = {}
            if len(self.buffers)==1:
                name = list(self.buffers)[0]
                chunk = self.buffers[name]
                inputs_merged = {name: chunk}
                new_buffers[name] = None
            else:
                split_time = self.buffers[self.shortest_buffer].end
                inputs = {}
               
                for d in self.buffers:
                    inputs[d], new_buffers[d] = \
                        self.buffers[d].split(
                            t=split_time,
                            allow_early_split=True)
            
                for _ in range(MAX_PASSES):
                    if len(set([x.end for x in inputs.values()])) <= 1:
                        break
                    split_time = min([x.end for x in inputs.values()]
                                            + [split_time])
                    for d in self.buffers:
                        inputs[d], back_to_buffer = \
                            inputs[d].split(
                                t=split_time,
                                allow_early_split=True)
                        new_buffers[d] = strax.Chunk.concatenate(
                            [back_to_buffer, new_buffers[d]])
                    
                else:
                    return

                # Merge inputs of the same kind
                inputs_merged = {
                        kind: strax.Chunk.merge([inputs[d] for d in deps_of_kind])
                        for kind, deps_of_kind in self.plugin.dependencies_by_kind().items()}
            
            client = get_client()
            if False: #self.plugin.parallel:
                future = client.submit(self._process, 
                                   self.plugin,
                                   self.chunk_counter,
                                   inputs_merged,
                                   )
                fire_and_forget(future)
            else:
                print(self.name, ": Processing ", inputs_merged)
                self._process(self.plugin,
                             self.chunk_counter,
                            inputs_merged,
                                   )
            self.buffers = new_buffers
            self.chunk_counter += 1

        except Exception as e:
            print(self.name,": ",e)
            self.report_exception(e)
            # Current fail policy is to empty buffer.
            # retry policy can be added.
            # self.emtpy_buffers()

   
    def _process(self, plugin, chunk_i, inputs_merged):
        results = plugin.do_compute(chunk_i=chunk_i, **inputs_merged)
        self.publish(results)

    def publish(self, results):
        if isinstance(results, dict):
            for k,v in results.items():
                self.publications[k].put(v)
        else:
            self.publications[self.plugin.provides[0]].put(results)

    def kill_subscribers(self):
        pill = PoisonPill()
        for p in self.publications.values():
            p.put(pill)

    def report_exception(self, e):
        pub = Pub("strax_exceptions")
        pub.put(e)
        logger.error(str(e))

    def empty_buffers(self):
        self.buffers = {t: None for t in self.subscriptions}
        self.chunk_counter = 0
    
    def cleanup(self):
        self.process_if_ready()
        if isinstance(self.plugin, strax.OverlapWindowPlugin):
            if self.plugin.cached_results is not None:
                self.publish(self.plugin.cached_results)
        
class SubscriptionIterator:
    sub: Sub
    results: ty.List

    def __init__(self, target):
        self.sub = Sub(target)
        self.results = []
    
    def collect(self):
        for chunk in self.sub:
            if isinstance(chunk, PoisonPill):
                self.cleanup()
                break
            self.results.append(chunk)
    
    async def aiter(self):
        async for chunk in self.sub:
            if isinstance(chunk, PoisonPill):
                self.cleanup()
                break
            yield chunk
    
    __iter__ = iter
    __aiter__ = aiter

    def cleanup(self):
        pass

def load_data(name, loader):
    pub = Pub(name)
    for chunk in loader():
        pub.put(chunk)
        print(chunk.data_kind)
    pub.put(PoisonPill())

def save_data(name, saver):
    sub = Sub(name)
    for i, chunk in enumerate(sub):
        if isinstance(chunk, PoisonPill):
            break
        saver.save(chunk, chunk_i=i)

class DaskPubSubProcessor(BaseProcessor):
    components: ProcessorComponents
    subscriptions: ty.Dict[str,Sub]

    def __init__(self,
                 components: ProcessorComponents,
                 start=True,
                 **kwargs):

        self.components = components
    
        logger.debug("Processor components are: " + str(components))
        self.subscriptions = {t: Sub(t) for t in self.components.targets}
        self.exceptions = Sub("strax_exceptions")
        if start:
            self.start()
        # produced = set(components.loaders)
        # required = set(components.targets)
        # saved = set(components.savers.keys())
        

    def start(self):
        client = get_client()
     
        # for name,savers in self.components.savers.items():
        #     for saver in savers:
        #         future = client.submit(save_data, name, saver)
        #         fire_and_forget(future)
        workers = []
        for p in set(self.components.plugins.values()):
            # pclass = p.__class__
            # pconfig = {attr:getattr(p, attr) for attr in PLUGIN_CONFIGS}
            # ppicklable = clean_deps(p)
            key = p.__class__.__name__
            future = client.submit(DaskPluginWorker, clean_deps(p), key=key , actor=True)
            workers.append(future)

        for future in as_completed(workers):
            worker = future.result()
            future = worker.main_loop()
            fire_and_forget(future)

        for name, loader in self.components.loaders.items():
            future = client.submit(load_data, name, loader)
            fire_and_forget(future)
        
    def stop(self):
        for name in self.components.loaders:
            Pub(name).put(PoisonPill())
        
    def iter(self):
        for chunk in self.subscription:
            if isinstance(chunk, PoisonPill):
                self.cleanup()
                break
            yield chunk