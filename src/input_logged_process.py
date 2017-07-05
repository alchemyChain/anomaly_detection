import json

from config_pro import ProcessorConfig


class InputProcessor:
    @staticmethod
    def process(batch_file, data_file):
        config_pro, batch_events = InputProcessor.process_batch_file(batch_file)
        stream_events_generator = InputProcessor.data_file_process(data_file)
        return config_pro, batch_events, stream_events_generator

    @staticmethod
    def process_batch_file(batch_flle):
        line_one = True
        with open(batch_flle_name) as f:
            config_pro = None
            events = []
            for line in f:
                line_data = json.loads(line)
                if line_one:
                    config_pro = ProcessorConfig(int(line_data['D']), int(line_data['T']))
                    line_one = False
                else:
                    events.append(line_data)
            return config_pro, events
    @staticmethod
    def data_file_process(data_file):
        with open(data_file_name) as f:
            for line in f:
                yield json.loads(line)
