import logging
from uuid import uuid4

from apache_beam import DoFn, PTransform, ParDo
from apache_beam.pvalue import PCollection
from beamstack_transforms.utils import ImportParams, import_package, install_package

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

REQUIRED_PACKAGES = ["openai"]


class CreateEmbeddings(PTransform):
    def __init__(self, embed_model: str, api_key: str, embed_indexes: list[str] = [], doc_id_index: str = None, metadata_indexes: list[str] = [], create_kwargs: dict = {}, label: str | None = None) -> None:
        super().__init__(label)
        self.embed_model = embed_model
        self.create_kwargs = create_kwargs
        self.api_key = api_key
        self.metadata_indexes = metadata_indexes
        self.embed_indexes = embed_indexes
        self.doc_id_index = doc_id_index

    def expand(self, pcoll: PCollection):
        return (
            pcoll
            | "Validate Colection" >> ParDo(self._ValidateCol(self.embed_indexes, self.metadata_indexes, self.doc_id_index))
            | "Create Embedding" >> ParDo(self._createEmbedding(self.embed_model, self.api_key, self.create_kwargs))
        )

    class _ValidateCol(DoFn):
        def __init__(self, embed_indexes: list[str] = [], metadata_indexes: list[str] = [], doc_id_index: str = None):
            self.embed_indexes = embed_indexes
            self.metadata_indexes = metadata_indexes
            self.doc_id_index = doc_id_index

        def process(self, element):
            doc = {
                "id": str(uuid4()),
                "metadata": {}
            }

            if hasattr(element, '_asdict'):
                element_dict = element._asdict()
                text_parts = []

                for key, value in element_dict.items():
                    if key in self.embed_indexes:
                        text_parts.append(value)
                    elif key in self.metadata_indexes:
                        doc["metadata"][key] = value
                    elif key == self.doc_id_index:
                        doc["_id"] = value

                doc["text"] = "\n".join(text_parts)
            else:
                doc["text"] = str(element)

            if "text" in doc:
                yield doc

    class _createEmbedding(DoFn):

        def __init__(self, embed_model: str, api_key: str, create_kwargs: dict = {}):
            self.embed_model = embed_model
            self.create_kwargs = create_kwargs
            self.api_key = api_key

        def start_bundle(self):
            try:
                install_package(REQUIRED_PACKAGES)
                Client, Embeddings = import_package(
                    modules=[
                        ImportParams(
                            module="openai",
                            objects=["Client"]
                        ),
                        ImportParams(
                            module="openai.resources.embeddings",
                            objects=["Embeddings"]
                        )
                    ]
                )
            except Exception as e:
                logger.error("ERROR IMPORTING PACKAGE")
                logger.error(e)
                quit()

            self.client = Client(
                api_key=self.api_key)
            self.embedder = Embeddings(self.client)

        def process(self, element):
            doc = {
                "embedding": self.embedder.create(
                    input=str(element["text"]), model=self.embed_model, **self.create_kwargs).data[0].embedding,
                "text": element["text"],
                "extra_info": element["metadata"]
            }
            yield doc
