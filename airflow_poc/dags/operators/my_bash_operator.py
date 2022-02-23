from airflow.operators.bash import BashOperator

class MyBashOperator(BashOperator):
    # pass

    # super().__init__()
    def __init__(self, *, bash_command: str, **kwargs):
        # print(kwargs)

        for key in list(kwargs['params'].keys()):
            if (kwargs['params'][key] == "" or kwargs['params'][key] is None):
                # print(kwargs['params'][key])
                print(kwargs['params'][key])
                del kwargs['params'][key]
        # print(f"Params: {kwargs['params']['Name']}")
        # for key, item in kwargs.items():
        #     print(key, "-----", item)
        super().__init__(bash_command=bash_command, **kwargs)
        # print(kwargs)
        # print("In BashOperator")