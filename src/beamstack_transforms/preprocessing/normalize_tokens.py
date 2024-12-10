import apache_beam as beam
from nltk.stem import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from typing import List
import nltk

nltk.download('wordnet')
nltk.download('omw-1.4')


class NormalizeTokens(beam.PTransform):
    def __init__(self, lemmatize: bool = True, stem: bool = False):
        """
        Initializes transform class for normalizing tokens.

        :param lemmatize (bool): Whether to apply lemmatization. Default is True.
        :param stem (bool): Whether to apply stemming. Default is False.
        """
        super().__init__()
        self.lemmatize = lemmatize
        self.stem = stem

    def expand(self, pcoll):
        return pcoll | "Normalize Tokens" >> beam.ParDo(
            self._NormalizeTokensFn(lemmatize=self.lemmatize, stem=self.stem)
        )

    class _NormalizeTokensFn(beam.DoFn):
        def __init__(self, lemmatize: bool, stem: bool):
            """
            Initialize class for normalizing tokens using lemmatization and/or stemming.

            :param lemmatize (bool): Whether to apply lemmatization.
            :param stem (bool): Whether to apply stemming.
            """
            self.lemmatize = lemmatize
            self.stem = stem
            self.lemmatizer = WordNetLemmatizer() if lemmatize else None
            self.stemmer = PorterStemmer() if stem else None

        def process(self, element: List[str]):
            """
            Normalizes tokens in the input element.

            Args:
            :param element (List[str]): List of tokens.
            :param yield (List[str]): Normalized tokens.
            """
            normalized_tokens = []
            for token in element:
                word = token
                if self.lemmatize:
                    word = self.lemmatizer.lemmatize(word)
                if self.stem:
                    word = self.stemmer.stem(word)
                normalized_tokens.append(word)
            yield normalized_tokens