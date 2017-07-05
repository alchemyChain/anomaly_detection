import heapq
from collections import defaultdict

import numpy


class LogProcessor:

    def __init__(self):
        self.graph = defaultdict(set)
        self.graph_stats = defaultdict(dict)
        self.graph_purchase = defaultdict(list)
        self.self_purchase = defaultdict(list)
        self.flagged_events = []

    def process(self, config_pro, batch_events, stream_events_generator):
        self.process_batch_events(batch_events, config_pro)
        self.process_stream_event(stream_events_generator, config_pro)

    def process_batch_events(self, batch_events, config_pro):
        for batch_event in batch_events:
            if batch_event['event_type'] == 'befriend':
                self.graph[batch_event['id1']].add(batch_event['id2'])
                self.graph[batch_event['id2']].add(batch_event['id1'])
            if batch_event['event_type'] == 'unfriend':
                self.graph[batch_event['id1']].remove(batch_event['id2'])
                self.graph[batch_event['id2']].remove(batch_event['id1'])
            if batch_event['event_type'] == 'purchase':
                self.purchase_event_track(self.self_purchase[batch_event['id']], config_pro.purchase_network, batch_event)
        for id in self.graph:
            connected_ids = {id}
            self.define_network(id, config_pro.deg_diff, connected_ids)
            connected_ids.remove(id)
            for connected_id in connected_ids:
                for event in self.self_purchase[connected_id]:
                    self.purchase_event_track(self.graph_purchase[id], config_pro.purchase_network, event[1])
            if len(self.graph_purchase[id]) > 2:
                amounts = [float(purchase[1]['amount']) for purchase in self.graph_purchase[id]]
                mean = numpy.mean(amounts)
                std = numpy.std(amounts)
                self.graph_stats[id]['mean'] = mean
                self.graph_stats[id]['std'] = std

    def process_stream_event(self, stream_events, config_pro):
        for stream_event in stream_events:
            if stream_event['event_type'] == 'befriend':
                self.graph[stream_event['id1']].add(stream_event['id2'])
                self.graph[stream_event['id2']].add(stream_event['id1'])
            if stream_event['event_type'] == 'unfriend':
                self.graph[stream_event['id1']].remove(stream_event['id2'])
                self.graph[stream_event['id2']].remove(stream_event['id1'])
            if stream_event['event_type'] == 'purchase':
                if self.graph_stats[stream_event['id']]['mean'] != None and self.graph_stats[stream_event['id']]['std'] != None:
                    if self.graph_stats[stream_event['id']]['mean'] + 3 * self.graph_stats[stream_event['id']]['std'] < float(stream_event['amount']):
                        flagged_event = self.graph_stats[stream_event['id']].copy()
                        flagged_event.update(stream_event)
                        self.flagged_events.append(flagged_event)
                connected_ids = {id}
                self.define_network(id, config_pro.deg_diff, connected_ids)
                connected_ids.remove(id)
                for connected_id in connected_ids:
                    self.purchase_event_track(self.graph_purchase[connected_id], config_pro.purchase_network, stream_event)
                    if len(self.graph_purchase[connected_id]) > 2:
                        amounts = [float(purchase[1]['amount']) for purchase in self.graph_purchase[connected_id]]
                        mean = numpy.mean(amounts)
                        std = numpy.std(amounts)
                        self.graph_stats[connected_id]['mean'] = mean
                        self.graph_stats[connected_id]['std'] = std

    def define_network(self, id, deg_diff, connected_ids):
        if deg_diff == 0:
            return
        for connected_id in self.graph[id]:
            if connected_id not in connected_ids:
                connected_ids.add(connected_id)
                self.define_network(connected_id, deg_diff - 1, connected_ids)

    def purchase_event_track(self, purchases, size, event):
        heapq.heappush(purchases, (event['timestamp'], event))
        if len(purchases) > size:
            heapq.heappop(purchases)
