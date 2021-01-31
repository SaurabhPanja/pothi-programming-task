from streamer import start_stream, generate_report_task_1
import schedule
import time
import os
import sys

if __name__ == '__main__':
    try:
        print("Enter keyword to track")
        keyword = input()
        keyword_list = [keyword]
        start_stream(keyword_list)
        # print("Executed...")
        schedule.every(1).minutes.do(generate_report_task_1)
        while 1:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)