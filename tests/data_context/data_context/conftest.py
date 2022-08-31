from typing import Optional

import pytest
from great_expectations.data_context.store import ExpectationsStore

from great_expectations.core import ExpectationSuite


class MockFileDataContext:
    def __init__(self):
        self.expectation_store = ExpectationsStore()
        self._evaluation_parameter_dependencies_compiled = False





@pytest.fixture
def mock_file_data_context() -> MockFileDataContext:
    """"""
    return MockFileDataContext()