import apache_beam as beam
import re
from typing import List
from nltk.corpus import stopwords
import nltk

nltk.download('stopwords')

class CleanText(beam.PTransform):
    def __init__(self, stop_words: List[str] = None, additional_patterns: List[str] = None):
        """
        Initializes the Transform class for cleaning texts.

        :param stop_words: List of custom stop words to add to the default list.
        :param additional_patterns: List of regex patterns to remove from text.
        """
        super().__init__()
        self.stop_words = stop_words
        self.additional_patterns = additional_patterns

    def expand(self, pcoll):
        return (
            pcoll
            | "Remove Patterns" >> beam.ParDo(self._RemovePatternsFn(self.additional_patterns))
            | "Remove Stop Words" >> beam.ParDo(self._RemoveStopWordsFn(self.stop_words))
        )
    
    class _RemovePatternsFn(beam.DoFn):
        def __init__(self, additional_patterns: List[str] = None):
            """
            Initializes the class to remove regex patterns from text.

            :param additional_patterns: List of regex patterns to remove from text.
            """
            self.additional_patterns = additional_patterns if additional_patterns else []

        def process(self, element: str):
            """
            Removes specified regex patterns from the text.

            :param element: Input text.
            :yield: Text with patterns removed.
            """
            text = re.sub(r'[^a-zA-Z0-9\s]', '', element)  # Remove non-alphanumeric characters.
            for pattern in self.additional_patterns:
                text = re.sub(pattern, '', text)
            yield text

    class _RemoveStopWordsFn(beam.DoFn):
        def __init__(self, stop_words: List[str] = None):
            """
            Initializes the transform class for removing stop words from text.

            :param stop_words: List of custom stop words to add to the default list.
            """
            nltk_stop_words = set(stopwords.words('english'))
            self.stop_words = nltk_stop_words.union(set(stop_words)) if stop_words else nltk_stop_words

        def process(self, element: str):
            """
            Removes stop words from the text.

            :param element: Input text.
            :yield: Text without stop words.
            """
            text = element.lower()
            words = text.split()
            text = ' '.join(word for word in words if word not in self.stop_words)
            yield text