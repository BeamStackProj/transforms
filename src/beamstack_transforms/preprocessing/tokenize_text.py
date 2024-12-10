import re
import apache_beam as beam
from typing import List, Optional

class TokenizeText(beam.PTransform):
    def __init__(self, lowercase: bool = True, custom_delimiters: Optional[List[str]] = None):
        """
        Initializes transform class for tokenizing text.

        :param lowercase (bool): Whether to lowercase the text before tokenization.
        :param custom_delimiters (Optional[List[str]]): Additional delimiters for tokenization.
        """
        super().__init__()
        self.lowercase = lowercase
        self.custom_delimiters = custom_delimiters

    def expand(self, pcoll):
        return pcoll | "Tokenize Text" >> beam.ParDo(self._TokenizeTextFn(self.lowercase, self.custom_delimiters))
    
    class _TokenizeTextFn(beam.DoFn):
        def __init__(self, lowercase: bool = True, custom_delimiters: Optional[List[str]] = None):
            """
            Initializes transform class for tokenizing text, with optional lowercasing.

            :param lowercase (bool): Whether to convert text to lowercase before tokenization.
            :param custom_delimiters (Optional[List[str]]): Additional delimiters for tokenization.
            """
            self.lowercase = lowercase
            self.custom_delimiters = custom_delimiters or [" ", "\n", "\t", ".", ",", "!", "?"]

        def process(self, element: str):
            """
            Tokenizes the input text.

            :param element (str): Input text.
            :param yield (List[str]): Tokenized words.
            """
            text = element.lower() if self.lowercase else element
            delimiter_pattern = "|".join(map(re.escape, self.custom_delimiters))
            tokens = re.split(delimiter_pattern, text)
            yield [token for token in tokens if token]