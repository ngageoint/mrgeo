from unittest import TestSuite, defaultTestLoader, main


def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for all_test_suite in defaultTestLoader.discover('.', pattern='*tests.py'):
        for test_suite in all_test_suite:
            suite.addTests(test_suite)

    return suite

if __name__ == '__main__':
    # verbosity levels:  0 - quiet, 1 = "normal", 2 = verbose
    main(verbosity=1)
