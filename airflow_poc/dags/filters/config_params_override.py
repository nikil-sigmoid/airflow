import ast

def return_first_matching_result(*argv):
    # for i in argv:
    # #     return i
    # argv = ast.literal_eval(argv[0])
    # argv = argv[0]
    # return type(argv)
    # # for i in argv[0]:
    # #     return i
    return next(item for item in argv if item != "" and item is not None and item != "None")
    # return next(item for item in argv if item != "" and item is not None)
    # return "Hey o0u708"

    # return type(argv[0])
