import re
import apache_beam as beam
from typing import List, Optional

class TokenizeText(beam.PTransform):
    def __init__(
            self, 
            lowercase: bool = True, 
            custom_delimiters: Optional[List[str]] = None, 
            keep_punctuation: bool = False
        ):
        """
        Initializes transform class for tokenizing text.

        :param lowercase (bool): Whether to lowercase the text before tokenization.
        :param custom_delimiters (Optional[List[str]]): Additional delimiters for tokenization.
        :param keep_punctuation (bool): Whether to keep punctuation as separate tokens.
        """
        super().__init__()
        self.lowercase = lowercase
        self.custom_delimiters = custom_delimiters
        self.keep_punctuation = keep_punctuation

    def expand(self, pcoll):
        return pcoll | "Tokenize Text" >> beam.ParDo(
            self._TokenizeTextFn(self.lowercase, self.custom_delimiters, self.keep_punctuation)
        )
    
    class _TokenizeTextFn(beam.DoFn):
        DEFAULT_DELIMITERS = [" ", "\n", "\t", ".", ",", "!", "?", ":", ";", "(", ")", "-", "_"]

        def __init__(
                self, 
                lowercase: bool, 
                custom_delimiters: Optional[List[str]], 
                keep_punctuation: bool
            ):
            """
            Initializes the tokenization function.

            :param lowercase (bool): Whether to lowercase the text before tokenization.
            :param custom_delimiters (Optional[List[str]]): Additional delimiters for tokenization.
            :param keep_punctuation (bool): Whether to keep punctuation as separate tokens.
            """
            self.lowercase = lowercase
            self.keep_punctuation = keep_punctuation
            self.delimiters = custom_delimiters or self.DEFAULT_DELIMITERS
            self.pattern = self._build_regex_pattern()

        def _build_regex_pattern(self) -> re.Pattern:
            """
            Builds a compiled regex pattern for tokenization.
            """
            if self.keep_punctuation:
                return re.compile(r"(\w+|[" + re.escape("".join(self.delimiters)) + r"])")
            return re.compile(r"|".join(map(re.escape, self.delimiters)))

        def process(self, element: str):
            """
            Tokenizes the input text.

            :param element (str): Input text.
            :return: A list of tokenized words.
            """
            text = element.lower() if self.lowercase else element
            tokens = self.pattern.findall(text) if self.keep_punctuation else re.split(self.pattern, text)
            yield [token for token in tokens if token]