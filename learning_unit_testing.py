import unittest

# The class to be tested
class NumberList:
    def __init__(self):
        self.numbers = []

    def add_number(self, number):
        """Adds a number to the list."""
        self.numbers.append(number)

    def remove_number(self, number):
        """Removes a number from the list if it exists."""
        if number in self.numbers:
            self.numbers.remove(number)

    def get_numbers(self):
        """Returns the list of numbers."""
        return self.numbers


# The unit test class
class TestNumberList(unittest.TestCase):

    def setUp(self):
        """Creates a new NumberList object for each test."""
        self.number_list = NumberList()

    def test_add_number(self):
        """Test adding a number to the list."""
        self.number_list.add_number(10)
        self.assertIn(10, self.number_list.get_numbers(), "10 should be in the list.")

    def test_remove_number(self):
        """Test removing a number from the list."""
        self.number_list.add_number(20)
        self.number_list.remove_number(20)
        self.assertNotIn(20, self.number_list.get_numbers(), "20 should be removed from the list.")

    def test_remove_number_not_in_list(self):
        """Test removing a number that is not in the list."""
        self.number_list.add_number(30)
        self.number_list.remove_number(40)  # 40 is not in the list
        self.assertIn(30, self.number_list.get_numbers(), "30 should still be in the list.")

    def test_get_numbers_empty(self):
        """Test getting numbers from an empty list."""
        self.assertEqual(self.number_list.get_numbers(), [], "The list should be empty.")

if __name__ == '__main__':
    # Run the tests
    unittest.main()


