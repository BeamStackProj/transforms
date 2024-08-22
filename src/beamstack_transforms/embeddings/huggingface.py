import logging

from apache_beam import DoFn, PTransform, ParDo
from beamstack_transforms.utils import import_package, ImportParams, install_package

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

REQUIRED_PACKAGES = ["sentence-transformers", "numpy"]


class CreateEmbeddings(PTransform):
    def __init__(self, embed_model: str, encode_kwargs: dict = {}, label: str | None = None) -> None:
        super().__init__(label)
        self.embed_model = embed_model
        self.encode_kwargs = encode_kwargs

    def expand(self, pcol):
        class createEmbedding(DoFn):

            def __init__(self, embed_model, encode_kwargs: dict = {}):
                self.embed_model = embed_model
                self.encode_kwargs = encode_kwargs

            def start_bundle(self):
                try:
                    install_package(REQUIRED_PACKAGES)
                    SentenceTransformer, self.np = import_package(
                        modules=[
                            ImportParams(
                                module="sentence_transformers",
                                objects=["SentenceTransformer"]
                            ),
                            ImportParams(
                                module="numpy"
                            )
                        ]
                    )
                except Exception as e:
                    logger.error(e)
                    quit()
                self.embedder = SentenceTransformer(self.embed_model)

            def process(self, element):
                if hasattr(element, '_asdict'):
                    embeddings = {key: self.embedder.encode(
                        str(value), **self.encode_kwargs).astype(self.np.float32).tolist()
                        for key, value in element._asdict().items()
                    }
                else:
                    embeddings = self.embedder.encode(
                        str(element)).astype(self.np.float32).tolist()
                yield embeddings

        return pcol | ParDo(createEmbedding(self.embed_model, self.encode_kwargs))
