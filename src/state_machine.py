import logging
import re
from enum import Enum

NAME_REGEX = r"^[A-Za-z]\w*$"
OPERAND_REGEX = r"[A-Za-z0-9_\-\.\" ]+$"


class SolrSQLState(Enum):
    INITIAL = 1
    SELECT = 2
    FROM = 3
    WHERE = 4
    GROUP_BY = 5
    ORDER_BY = 6
    EXPRESSION_LIST = 7
    EXPRESSION = 8
    COLUMN_NAME = 9
    FUNCTION = 10
    CONDITION = 11
    OPERATOR = 12
    COLUMN_NAME_LIST = 13
    DIRECTION = 14
    COLLECTION_NAME = 15
    LIMIT = 16
    LIMIT_COUNT = 17
    AS = 18


class SolrSQLStateMachine:
    def __init__(self):
        self.current_state = SolrSQLState.INITIAL
        self.token_buffer = []
        self.next_states = [SolrSQLState.SELECT]
        self.state_stack = []

        self.logger = logging.getLogger(__name__)

    def get_next_states(self) -> list[SolrSQLState]:
        return self.next_states

    def get_current_state(self) -> SolrSQLState:
        return self.current_state

    def reset(self):
        self.current_state = SolrSQLState.INITIAL
        self.token_buffer = []
        self.next_states = [SolrSQLState.SELECT]
        self.state_stack = []

    def process_token(self, token):
        token = token.upper()
        self.logger.debug(token)
        if self.current_state == SolrSQLState.INITIAL:
            if token == "SELECT":
                self.current_state = SolrSQLState.SELECT

        elif self.current_state == SolrSQLState.SELECT:
            if token in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                self.state_stack.append(self.current_state)
                self.token_buffer.append(token)
                self.current_state = SolrSQLState.FUNCTION
            elif token in [",", "*"]:
                self.current_state = SolrSQLState.EXPRESSION_LIST
            elif re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.EXPRESSION
                self.token_buffer.append(token)

        elif self.current_state == SolrSQLState.FROM:
            if re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.COLLECTION_NAME
                self.token_buffer.append(token)

        elif self.current_state == SolrSQLState.WHERE:
            if token in ["AND", "OR", "NOT"]:
                self.current_state = SolrSQLState.CONDITION
            elif re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.CONDITION
            elif token in ["=", "!=", "<", ">", "<=", ">="]:
                self.current_state = SolrSQLState.OPERATOR

        elif self.current_state == SolrSQLState.GROUP_BY:
            if re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.COLUMN_NAME
                self.token_buffer.append(token)

        elif self.current_state == SolrSQLState.ORDER_BY:
            if token in ["ASC", "DESC"]:
                self.current_state = SolrSQLState.DIRECTION
            elif re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.COLUMN_NAME

        elif self.current_state == SolrSQLState.EXPRESSION_LIST:
            if token == ",":
                self.current_state = SolrSQLState.EXPRESSION_LIST
            elif token in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                self.current_state = SolrSQLState.FUNCTION
                self.state_stack.append(self.current_state)
                self.token_buffer.append(token)
            elif token == "FROM":
                self.current_state = SolrSQLState.FROM
            elif token == "WHERE":
                self.current_state = SolrSQLState.WHERE
            elif token == "GROUP BY":
                self.current_state = SolrSQLState.GROUP_BY
            elif token == "ORDER BY":
                self.current_state = SolrSQLState.ORDER_BY
            elif re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.EXPRESSION_LIST
                self.token_buffer.append(token)

        elif self.current_state == SolrSQLState.EXPRESSION:
            if token in [",", "*"]:
                self.current_state = SolrSQLState.EXPRESSION_LIST
            elif token == "FROM":
                self.current_state = SolrSQLState.FROM
            elif token == "WHERE":
                self.current_state = SolrSQLState.WHERE
            elif token == "GROUP BY":
                self.current_state = SolrSQLState.GROUP_BY
            elif token == "ORDER BY":
                self.current_state = SolrSQLState.ORDER_BY

        elif self.current_state == SolrSQLState.COLUMN_NAME:
            if token == "FROM":
                self.current_state = SolrSQLState.FROM
            elif token == "GROUP BY":
                self.current_state = SolrSQLState.GROUP_BY
            elif token == "ORDER BY":
                self.current_state = SolrSQLState.ORDER_BY
            elif token in ["ASC", "DESC"]:
                self.current_state = SolrSQLState.DIRECTION
            elif re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.COLUMN_NAME_LIST

        elif self.current_state == SolrSQLState.FUNCTION:
            if re.match(NAME_REGEX, token):
                self.current_state = self.state_stack.pop()
            elif token == "*":
                self.current_state = SolrSQLState.EXPRESSION

        elif self.current_state == SolrSQLState.CONDITION:
            if token in ["AND", "OR", "NOT"]:
                self.current_state = SolrSQLState.CONDITION
            elif token in ["=", "!=", "<", ">", "<=", ">="]:
                self.current_state = SolrSQLState.OPERATOR
            elif token == "GROUP BY":
                self.current_state = SolrSQLState.GROUP_BY
            elif token == "ORDER BY":
                self.current_state = SolrSQLState.ORDER_BY
            elif token == "LIMIT":
                self.current_state = SolrSQLState.LIMIT
            elif re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.CONDITION

        elif self.current_state == SolrSQLState.OPERATOR:
            if re.match(OPERAND_REGEX, token):
                self.current_state = SolrSQLState.CONDITION

        elif self.current_state == SolrSQLState.COLUMN_NAME_LIST:
            if re.match(NAME_REGEX, token):
                self.current_state = SolrSQLState.COLUMN_NAME_LIST
            elif token == "FROM":
                self.current_state = SolrSQLState.FROM
            elif token == "GROUP BY":
                self.current_state = SolrSQLState.GROUP_BY
            elif token == "ORDER BY":
                self.current_state = SolrSQLState.ORDER_BY

        elif self.current_state == SolrSQLState.DIRECTION:
            if token == "LIMIT":
                self.current_state = SolrSQLState.LIMIT

        elif self.current_state == SolrSQLState.COLLECTION_NAME:
            if token == "LIMIT":
                self.current_state = SolrSQLState.LIMIT
            elif token == "WHERE":
                self.current_state = SolrSQLState.WHERE
            elif token == "GROUP BY":
                self.current_state = SolrSQLState.GROUP_BY
            elif token == "ORDER BY":
                self.current_state = SolrSQLState.ORDER_BY

        elif self.current_state == SolrSQLState.LIMIT:
            if token.isnum():
                self.current_state = SolrSQLState.LIMIT_COUNT
                self.token_buffer.append(token)

        if self.current_state == SolrSQLState.INITIAL:
            self.next_states = [SolrSQLState.SELECT]
        elif self.current_state == SolrSQLState.SELECT:
            self.next_states = [SolrSQLState.EXPRESSION_LIST]
        elif self.current_state == SolrSQLState.FROM:
            self.next_states = [SolrSQLState.COLLECTION_NAME]
        elif self.current_state == SolrSQLState.COLLECTION_NAME:
            self.next_states = [
                SolrSQLState.WHERE,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
                SolrSQLState.LIMIT,
            ]
        elif self.current_state == SolrSQLState.WHERE:
            self.next_states = [SolrSQLState.CONDITION, SolrSQLState.OPERATOR]
        elif self.current_state == SolrSQLState.GROUP_BY:
            self.next_states = [SolrSQLState.COLUMN_NAME_LIST]
        elif self.current_state == SolrSQLState.ORDER_BY:
            self.next_states = [SolrSQLState.COLUMN_NAME]
        elif self.current_state == SolrSQLState.EXPRESSION_LIST:
            self.next_states = [
                SolrSQLState.EXPRESSION_LIST,
                SolrSQLState.FROM,
                SolrSQLState.WHERE,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
            ]
        elif self.current_state == SolrSQLState.EXPRESSION:
            self.next_states = [
                SolrSQLState.EXPRESSION_LIST,
                SolrSQLState.FROM,
                SolrSQLState.WHERE,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
            ]
        elif self.current_state == SolrSQLState.COLUMN_NAME:
            self.next_states = [
                SolrSQLState.COLUMN_NAME_LIST,
                SolrSQLState.FROM,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
                SolrSQLState.DIRECTION,
                SolrSQLState.LIMIT,
            ]
        elif self.current_state == SolrSQLState.FUNCTION:
            self.next_states = [
                SolrSQLState.EXPRESSION_LIST,
                SolrSQLState.FROM,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
            ]
        elif self.current_state == SolrSQLState.CONDITION:
            self.next_states = [
                SolrSQLState.CONDITION,
                SolrSQLState.OPERATOR,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
                SolrSQLState.LIMIT,
            ]
        elif self.current_state == SolrSQLState.OPERATOR:
            self.next_states = [SolrSQLState.CONDITION]
        elif self.current_state == SolrSQLState.COLUMN_NAME_LIST:
            self.next_states = [
                SolrSQLState.COLUMN_NAME_LIST,
                SolrSQLState.FROM,
                SolrSQLState.GROUP_BY,
                SolrSQLState.ORDER_BY,
                SolrSQLState.DIRECTION,
                SolrSQLState.LIMIT,
            ]
        elif self.current_state == SolrSQLState.DIRECTION:
            self.next_states = [SolrSQLState.LIMIT]
        else:
            self.next_states = []

    def is_valid(self):
        return (
            self.current_state == SolrSQLState.FROM
            or self.current_state == SolrSQLState.WHERE
            or self.current_state == SolrSQLState.GROUP_BY
            or self.current_state == SolrSQLState.ORDER_BY
        )
