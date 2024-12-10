import apache_beam as beam
from langdetect import detect
from typing import Any, Optional

class LanguageDetection(beam.PTransform):
    def __init__(self, output_key: Optional[str] = None):
        """
        Initializes the transform class for detecting text language.

        :param output_key (Optional[str]): Key to store the detected language in the output element.
        """
        super().__init__()
        self.output_key = output_key

    def expand(self, pcoll):
        return pcoll | "Detect Language" >> beam.ParDo(self._DetectLanguageFn(self.output_key))

    class _DetectLanguageFn(beam.DoFn):
        def __init__(self, output_key: Optional[str]):
            """
            Initializes class for detecting the language of input text.

            :param output_key (Optional[str]): Key to store the detected language in the output element.
            """
            self.output_key = output_key

        def process(self, element: Any):
            """
            Detects the language of the input text.

            :param element: Input text element. Can be plain text or a dictionary containing text.
            :param yield: Output text with detected language.
            """
            text = element if isinstance(element, str) else element.get("text", "")
            detected_language = detect(text)

            if self.output_key:
                element[self.output_key] = detected_language
                yield element
            else:
                yield {"text": text, "language": detected_language}