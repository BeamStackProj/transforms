import apache_beam as beam
import random
from typing import List, Dict, Any
from nltk.corpus import wordnet

class TextAugmentation(beam.PTransform):
    def __init__(self, techniques: List[str], augment_factor: int = 1):
        """
        Initializes transform class for augmenting text data using specified techniques.

        :param techniques (List[str]): List of augmentation techniques to apply (e.g., 'synonym_replacement', 'back_translation').
        :param augment_factor (int): Number of augmented examples to generate per input. Default is 1.
        """
        super().__init__()
        self.techniques = techniques
        self.augment_factor = augment_factor

    def expand(self, pcoll):
        return pcoll | "Augment Text" >> beam.ParDo(
            self._TextAugmentationFn(self.techniques, self.augment_factor)
        )

    class _TextAugmentationFn(beam.DoFn):
        def __init__(self, techniques: List[str], augment_factor: int):
            """
            A DoFn for applying text augmentation techniques.

            :param techniques (List[str]): List of techniques for augmentation.
            :param augment_factor (int): Number of augmented examples to generate per input.
            """
            self.techniques = techniques
            self.augment_factor = augment_factor

        def process(self, element: Dict[str, Any]):
            """
            Augments the input text using specified techniques.

            Args:
            :param element (Dict[str, Any]): Input dictionary containing the text to augment.
            :param yield (Dict[str, Any]): Augmented examples.
            """
            text = element.get("text", "")
            for _ in range(self.augment_factor):
                augmented_text = self._apply_augmentation(text)
                augmented_element = element.copy()
                augmented_element["text"] = augmented_text
                yield augmented_element

        def _apply_augmentation(self, text: str) -> str:
            """
            Applies augmentation techniques to the input text.

            :param text (str): Original text.

            Returns:
                str: Augmented text.
            """
            if "synonym_replacement" in self.techniques:
                text = self._synonym_replacement(text)
            # Additional techniques can be added here.
            return text

        def _synonym_replacement(self, text: str) -> str:
            """
            Replaces random words with synonyms.

            Args:
                text (str): Original text.

            Returns:
                str: Text with synonyms replaced.
            """
            words = text.split()
            for i, word in enumerate(words):
                synonyms = wordnet.synsets(word)
                if synonyms:
                    synonym = random.choice(synonyms).lemmas()[0].name()
                    words[i] = synonym
            return " ".join(words)