import logging

from prompt_toolkit.completion import Completion
from text import Text


# pylint: disable=too-few-public-methods
class Intellisense:

    def get_context_items(self, last_token, last_char, state_machine):

        logger = logging.getLogger(__name__)

        text = Text()

        next_states = state_machine.get_next_states()

        if next_states is None:
            raise ValueError

        if last_char == " ":
            start_position = 0
        else:
            start_position = -len(last_token)

        logger.debug(list(text.textual(next_state) for next_state in next_states))

        return [
            Completion(completion, start_position=start_position)
            for next_state in next_states
            for completion in text.textual(next_state)
            if completion.startswith(last_token) or last_char in [" ", ",", "(", ")"]
        ]
