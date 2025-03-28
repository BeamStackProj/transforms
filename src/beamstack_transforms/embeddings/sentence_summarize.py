import apache_beam as beam
from typing import List
from transformers import pipeline

class SummarizationTransform(beam.PTransform):
    def __init__(self, model_name: str, max_length: int = 130, min_length: int = 30):
        """
        Initializes the transform for summarization.

        :param model_name (str): The name of the summarization model to use.
        :param max_length (int): The maximum length of the generated summary.
        :param min_length (int): The minimum length of the generated summary.
        """
        super().__init__()
        self.model_name = model_name
        self.max_length = max_length
        self.min_length = min_length

    def expand(self, pcoll):
        return pcoll | "Summarize Text" >> beam.ParDo(self._SummarizeTextFn(self.model_name, self.max_length, self.min_length))

    class _SummarizeTextFn(beam.DoFn):
        def __init__(self, model_name: str, max_length: int, min_length: int):
            """
            Initializes the function for summarization.

            :param model_name (str): The name of the summarization model.
            :param max_length (int): The maximum length of the generated summary.
            :param min_length (int): The minimum length of the generated summary.
            """
            self.model_name = model_name
            self.max_length = max_length
            self.min_length = min_length
            self.summarizer = None

        def setup(self):
            """Load the summarization model."""
            self.summarizer = pipeline("summarization", model=self.model_name)

        def process(self, element: str):
            """
            Summarizes a large block of text.

            :param element (str): Input text block.
            :yield (str): The generated summary.
            """
            summary = self.summarizer(
                element,
                max_length=self.max_length,
                min_length=self.min_length,
                do_sample=False
            )
            yield summary[0]["summary_text"]