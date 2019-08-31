from time import sleep
def wait_until_shutdown(itr):
    i = 0
    while i < 200 and not itr.is_shutdown():
        sleep(0.01)
        i += 1
    return itr.is_shutdown()