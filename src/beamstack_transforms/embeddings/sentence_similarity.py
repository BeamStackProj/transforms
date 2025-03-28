import apache_beam as beam
from typing import Tuple, List
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

class SentenceSimilarityTransform(beam.PTransform):
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Initializes the transform for sentence similarity.

        :param model_name (str): Pre-trained sentence embedding model from Hugging Face.
        """
        super().__init__()
        self.model_name = model_name

    def expand(self, pcoll):
        return (
            pcoll
            | "Compute Sentence Similarity" >> beam.ParDo(self._ComputeSimilarityFn(self.model_name))
        )

    class _ComputeSimilarityFn(beam.DoFn):
        def __init__(self, model_name: str):
            """
            Initializes the function to compute similarity.

            :param model_name (str): Pre-trained sentence embedding model name.
            """
            self.model_name = model_name
            self.model = None

        def setup(self):
            """Load the sentence embedding model."""
            self.model = SentenceTransformer(self.model_name)

        def process(self, element: Tuple[str, str]):
            """
            Computes the similarity between sentences.

            :param element (Tuple[str, str]): A pair of sentences to compare.
            :yield (Tuple[str, str, float]): Sentences and their similarity score.
            """
            sentences = element
            embeddings = self.model.encode(sentences, convert_to_tensor=True)
            similarity_matrix = cosine_similarity(embeddings)

            for i in range(len(sentences)):
                for j in range(i + 1, len(sentences)):
                    yield (sentences[i], sentences[j], similarity_matrix[i][j])