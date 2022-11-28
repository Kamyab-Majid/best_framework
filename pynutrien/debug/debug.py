import sys
import inspect
import functools


def begin_debug_trace():
    sys.settrace(debug_trace)


def debug_trace(frame, event, arg):
    global current_frame
    current_frame = frame
    # print(frame, event, arg)
    if event == "call":
        func = inspect.getframeinfo(current_frame).function
        sig = inspect.signature(current_frame.f_globals[func])
        arg_string = ", ".join(
            [f"{key}={current_frame.f_locals[key]}" for key in sig.parameters]
        )
        print(f"Calling: {func}({arg_string})")
    elif event == "line":
        pass
    elif event == "return":
        func = inspect.getframeinfo(current_frame).function
        sig = inspect.signature(current_frame.f_globals[func])
        arg_string = ", ".join([f"{key}" for key in sig.parameters])
        print(f"Return: {func}({arg_string}) -> {arg!r}")
    elif event == "opcode":
        pass
    elif event == "exception":
        exception, value, traceback = arg
        print(exception)
        end_debug_trace()
        return

    return debug_trace


def end_debug_trace():
    sys.settrace(None)


if __name__ == "__main__":

    def f2(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)

        return wrapper

    @f2
    def f(x):
        return x**2

    def g(x):
        # raise Exception
        x += 1
        return x + 1

    begin_debug_trace()
    print(g(f(3)))

    end_debug_trace()
