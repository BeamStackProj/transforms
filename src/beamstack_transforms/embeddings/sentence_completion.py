import apache_beam as beam
from typing import Optional
from transformers import pipeline
import openai

class TextCompletionTransform(beam.PTransform):
    def __init__(self, backend: str, model_name: str, max_length: int = 50, openai_api_key: Optional[str] = None):
        """
        Initializes the transform for text completion.

        :param backend (str): The backend to use ('huggingface' or 'openai').
        :param model_name (str): The model name to use for text completion.
        :param max_length (int): The maximum length of the generated completion.
        :param openai_api_key (Optional[str]): The API key for OpenAI (required if backend is 'openai').
        """
        super().__init__()
        self.backend = backend.lower()
        self.model_name = model_name
        self.max_length = max_length
        self.openai_api_key = openai_api_key

        if self.backend not in ["huggingface", "openai"]:
            raise ValueError("Invalid backend. Choose 'huggingface' or 'openai'.")

    def expand(self, pcoll):
        return pcoll | "Generate Text Completions" >> beam.ParDo(
            self._GenerateCompletionFn(self.backend, self.model_name, self.max_length, self.openai_api_key)
        )

    class _GenerateCompletionFn(beam.DoFn):
        def __init__(self, backend: str, model_name: str, max_length: int, openai_api_key: Optional[str]):
            """
            Initializes the function for text completion.

            :param backend (str): The backend to use ('huggingface' or 'openai').
            :param model_name (str): The model name to use.
            :param max_length (int): The maximum length of the generated completion.
            :param openai_api_key (Optional[str]): The API key for OpenAI (required if backend is 'openai').
            """
            self.backend = backend
            self.model_name = model_name
            self.max_length = max_length
            self.openai_api_key = openai_api_key
            self.generator = None

        def setup(self):
            """Load the model or initialize API connection based on the backend."""
            if self.backend == "huggingface":
                self.generator = pipeline("text-generation", model=self.model_name)
            elif self.backend == "openai":
                if not self.openai_api_key:
                    raise ValueError("OpenAI API key must be provided for the OpenAI backend.")
                openai.api_key = self.openai_api_key

        def process(self, element: str):
            """
            Generates a text completion for the input partial text.

            :param element (str): The partial text to complete.
            :yield (str): The completed text.
            """
            if self.backend == "huggingface":
                completions = self.generator(
                    element,
                    max_length=self.max_length,
                    num_return_sequences=1,
                    do_sample=True
                )
                yield completions[0]["generated_text"]
            elif self.backend == "openai":
                response = openai.Completion.create(
                    engine=self.model_name,
                    prompt=element,
                    max_tokens=self.max_length,
                    temperature=0.7
                )
                yield response.choices[0].text.strip()