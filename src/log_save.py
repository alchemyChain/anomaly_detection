import json
import sys

from input_logged_process import InputProcessor
from log_process import LogProcessor


def main():
    config_pro, batch_events, stream_events_generator = InputProcessor.process(sys.argv[1], sys.argv[2])
    log_process = LogProcessor()
    log_process.process(config_pro, batch_events, stream_events_generator)
    with open(sys.argv[3],"w+") as output:
        for event in log_process.flagged_events:
            json.dump(event, output)
            output.write("\n")
        output.close()

if __name__ == "__main__":
    main()