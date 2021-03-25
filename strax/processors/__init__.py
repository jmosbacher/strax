from .base import ProcessorComponents, BaseProcessor
from .threaded_mailbox_processor import ThreadedMailboxProcessor
from .dask_pubsub import DaskPubSubProcessor

PROCESSORS = {
    "default": ThreadedMailboxProcessor,
    "threaded_mailbox_processor": ThreadedMailboxProcessor,
    "dask_pubsub": DaskPubSubProcessor,
}

# FIXME: add entrypoint logic for processor plugins.