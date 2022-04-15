import datetime
from datetime import datetime
from InterpolParser import InterpolParser


def main():
    pI = InterpolParser(max_threads=5)
    ts = datetime.now()
    pI.get_all_rednotice_data()
    print(pI.URL_REQUEST_COUNTER, 'requests')
    te = datetime.now()
    print(te - ts, '*'*50)


if __name__ == '__main__':
    main()
