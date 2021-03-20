from dask.distributed import client, Sub, Pub, get_client
import typing as ty
import strax

from .base import BaseProcessor

logger = logging.getLogger(__name__)

def combined_target(targets):
    return hash(tuple(sorted(targets)))

class PoisonPill:
    exception: Exception
    creator: str
    acknowledged: ty.List

class ChunkBuffer:
    buffers: ty.Dict
    subscriptions: ty.Dict
    publication: Pub

    def __init__(self, targets):
        target = combined_target(targets)
        publication = Pub(comb_target)
        self.subscriptions = {
            name: Sub(t) for t in targets
        }
        self.buffers = {t: None for t in targets}

    @staticmethod
    def priority(buffer):
        if isinstance(buffer,strax.Chunk):
            return buffer.end
        else:
            return 0

    @property
    def shortest_buffer(self):
        return min([(k,v) for k,v in self.buffers.items()], key=lambda x: self.priority(x[1]))
    
    @property
    def tmin(self):
        return self.shortest_buffer[1].end

    def _process(self):
        target, buffer = self.shortest_buffer
        chunk = self.subsciptions[target].get()
        if isinstance(buffer, strax.Chunk):
            self.buffers[target] = strax.Chunk.concatenate([buffer, chunk])
        else:
            self.buffers[target] = chunk

        if any([b is None for b in self.buffers.values()]):
            return
        
        outputs, new_buffers = {}, {}
        
        for target, buffer in self.buffers.items():
            try:
                outputs[target], new_buffers[target] = \
                            buffer.split(
                                t=self.tmin,
                                allow_early_split=False)
            except:
                break
        else:
            self.publication.put(outputs)
            self.buffers = new_buffers

    def process(self):
        while True:
            try:
                self._process()
            except Exception as e:
                self.cleanup()
                break

    def cleanup(self):
        pass

class DaskPluginWorker:
    name: str
    stateful: bool
    plugin: strax.Plugin
    subscription: Sub
    publications: ty.Dict

    def __init__(self, plugin):
        self.plugin = plugin
        self.name = plugin.__name__

        if len(plugin.depends_on)>1:
            dep = hash(plugin.depends_on)
        else:
            dep = plugin.depends_on[0]

        self.subscription = {
            name: Sub(name) for name in 
        }
        self.publications = {
            name: Pub(name) for name in plugin.provides
        }

    def process(self):
        for chunks in self.subscription:
            if isinstance(chunks, PoisonPill):
                self.cleanup(chunks)
                break
            if not isintance(chunks, dict):
                chunks = {self.plugin.depends_on[0]: chunks}
            try:
                self._process(chunks)
            except Exception as e:
                pill = PoisonPill(
                )
                self.cleanup(pill)
                break

    def _process(self, chunks):
        inputs_merged = {
                        kind: strax.Chunk.merge([chunks[d] for d in deps_of_kind])
                        for kind, deps_of_kind in self.plugin.dependencies_by_kind().items()
                        }
        results = self.plugin.do_compute(**inputs_merged)
        for k,v in results.items():
            self.publications[k].put(v)

    def cleanup(self, pill=None):
        if pill is None:
            pill = poison_pill()
        for p in self.publications.values():
            p.put(pill)
    
    @property
    def poison_pill(self, e=None):
        return PoisonPill(
            exception=e,
            creator=self.name,
        )

class SubscriptionIterator:
    sub: Sub

    def __init__(self, target):
        self.sub = Sub(target)

    def iter(self):
        for chunk in sub:
            if isinstance(chunk, PoisonPill):
                self.cleanup()
                break
            yield chunk

    async def aiter(self):
        async for chunk in sub:
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
    for chunk in loader:
        pub.put(chunk)

def save_data(name, saver):
    sub = Sub(name)
    for i, chunk in enumerate(sub):
        if isinstance(chunk, PoisonPill):
            break
        saver.save(chunk, chunk_i=i)

class DaskProcessor(BaseProcessor):

    def __init__(self,
                 components: ProcessorComponents,
                 allow_rechunk=True, 
                 allow_shm=False,
                 allow_multiprocess=False,
                 allow_lazy=True,
                 max_workers=None,
                 max_messages=4,
                 timeout=60, 
                 **kwargs):

        self.components = components

        logger.debug("Processor components are: " + str(components))
        
        produced = set(components.loaders)
        required = set(components.targets)
        saved = set(components.savers.keys())
        client = get_client()
        workers = []
        for p in components.plugins.values():
            worker = DaskPluginWorker(p)
            workers.append(worker)
            client.submit(worker.process)
       
    def iter(self):
        return SubscriptionIterator(self.components.targets[0]).iter()
    