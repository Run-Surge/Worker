import os
import subprocess
import sys
import unittest

class TestRunFile(unittest.TestCase):
    def setUp(self):
        self.test_dir = os.path.join(os.path.dirname(__file__), '..', 'worker_app', 'vm', 'shared', '5')
        self.test_file = os.path.join(self.test_dir, 'test.py')

    def test_run_python_file(self):
        # Ensure we're in the correct directory
        original_dir = os.getcwd()
        os.chdir(self.test_dir)

        try:
            # Run the Python file and capture output
            result = subprocess.run([sys.executable, self.test_file], 
                                 capture_output=True,
                                 text=True)
            
            # Check the output contains the expected text
            print(result.stdout)
            self.assertEqual(result.returncode, 0)

        finally:
            # Restore original directory
            os.chdir(original_dir)

if __name__ == '__main__':
    unittest.main()
