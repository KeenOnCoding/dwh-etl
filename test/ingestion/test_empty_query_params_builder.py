from unittest import TestCase

from query_params_builders.empty_query_param_builder \
    import EmptyQueryParamsBuilder


class TestEmptyQueryParamsBuilder(TestCase):
    def test_build(self):
        # Arrange

        # Act
        actual = EmptyQueryParamsBuilder().build(None)

        # Assert
        expected = ''
        self.assertEqual(actual, expected)
