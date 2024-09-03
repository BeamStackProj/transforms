import logging
from apache_beam import DoFn, PTransform, ParDo
from apache_beam.pvalue import PCollection
from beamstack_transforms.utils import ImportParams, import_package, install_package

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

REQUIRED_PACKAGES = [
    "llama-index-core==0.10.67",
    "llama-index-vector-stores-elasticsearch==0.2.5"
]


class WriteToElasticsearchVectorStore(PTransform):
    def __init__(self, index_name: str, es_url: str = None, es_api_key: str = None, es_cloud_id: str = None, client_kwargs: dict = {}, store_kwargs: dict = {}, label: str | None = None) -> None:
        super().__init__(label)
        self.es_url = es_url
        self.client_kwargs = client_kwargs
        self.store_kwargs = store_kwargs
        self.es_api_key = es_api_key
        self.es_cloud_id = es_cloud_id
        self.index_name = index_name

    def expand(self, pcoll: PCollection):
        return (
            pcoll
            | "Prepare for Elasticsearch" >> ParDo(self._PrepareDoc())
            | "Write to Elasticsearch" >> ParDo(self._WriteToElasticsearch(self.index_name, self.es_url, self.es_api_key, self.es_cloud_id, self.client_kwargs, self.store_kwargs))
        )

    class _PrepareDoc(DoFn):
        def process(self, element):
            if "embedding" not in element:
                yield None

            doc = {
                'embedding': element['embedding']
            }
            doc['text'] = element['text'] if 'text' in element else ""
            doc['extra_info'] = element['extra_info'] if 'extra_info' in element else {}
            yield doc

    class _WriteToElasticsearch(DoFn):

        def __init__(self, index_name: str = None, es_url: str = None, es_api_key: str = None, es_cloud_id: str = None, client_kwargs: dict = {}, store_kwargs: dict = {}):
            self.es_url = es_url
            self.client_kwargs = client_kwargs
            self.store_kwargs = store_kwargs
            self.es_api_key = es_api_key
            self.es_cloud_id = es_cloud_id
            self.index_name = index_name
            self.prev_node_id = ""

        def start_bundle(self):
            try:
                install_package(REQUIRED_PACKAGES)
                ClientObj, ElasticsearchStoreObj, self.TextNodeObj, self.NodeRelationshipObj, self.RelatedNodeInfoObj = import_package(
                    modules=[
                        ImportParams(
                            module="elasticsearch",
                            objects=["AsyncElasticsearch"]
                        ),
                        ImportParams(
                            module="llama_index.vector_stores.elasticsearch",
                            objects=["ElasticsearchStore"]
                        ),
                        ImportParams(
                            module="llama_index.core.schema",
                            objects=[
                                "TextNode",
                                "NodeRelationship",
                                "RelatedNodeInfo"
                            ]
                        )
                    ]
                )

            except Exception as e:
                logger.error("ERROR IMPORTING PACKAGE")
                logger.error(e)
                quit()
            client = ClientObj(
                self.es_url,
                cloud_id=self.es_cloud_id,
                api_key=self.es_api_key,
                **self.client_kwargs
            )
            self.es_vector_store = ElasticsearchStoreObj(
                index_name=self.index_name,
                es_client=client,
                **self.store_kwargs
            )

        def process(self, element):
            logger.info(f"previous node id {self.prev_node_id}")
            node = self.TextNodeObj(
                **element
            )
            if self.prev_node_id != "":
                node.relationships[self.NodeRelationshipObj.PREVIOUS] = self.RelatedNodeInfoObj(
                    node_id=self.prev_node_id
                )
            self.prev_node_id = node.node_id
            self.es_vector_store.add(
                nodes=[node], create_index_if_not_exists=True)
