def merge_dictionaries(job_dict, global_dict) -> dict:
    """This merge fucntion uses simple python 3.7
    code to merge two dictionaries into one.

    Returns:
        dict: this class returns a combined dictionary.
    """
    output = {**job_dict, **global_dict}
    return output


# Question to ask: How does this merge function perform? What scenarios can we test with this?
"""
Scenarios:
1) Overlaping keys: Which dict takes precedence? (global_dict takes precidence)
2) Make a test case to check if the output is as expected.(Use unit-tests)
* Try testing with just the method, no need for class during testing. *
* Review the python documentation to find the behavior of line 25 *
"""
