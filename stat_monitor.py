import json

from webob import Response
from ryu.base import app_manager
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.ofproto import ether
from ryu.ofproto import inet
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.topology.api import get_switch
from ryu.lib import hub

import block_data
import stat_data
from helper import ofp_helper
# from route import urls
# from config import settings

stat_monitor_instance_name = 'stat_monitor_api_app'
syn_flow = {
    'ip_src': '192.168.11.23',
    'ip_dst': '140.114.71.177',
    'port_src': 5566,
    'port_dst': 9292
}
group_ammount = 100

class StatMonitor(app_manager.RyuApp):

    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(StatMonitor, self).__init__(*args, **kwargs)
        self.switches = {}
        wsgi = kwargs['wsgi']
        wsgi.register(StatMonitorController,
                      {stat_monitor_instance_name: self})
        self.topology_api_app = self
        self.monitor_thread = hub.spawn(self._monitor)

    def reset_counter(self):
        switch_list = get_switch(self.topology_api_app, None)
        for switch in switch_list:
            datapath = switch.dp
            self._reset_flow(datapath)

    def _reset_flow(self, datapath):
        parser = datapath.ofproto_parser
        syn_ack_match = parser.OFPMatch(eth_type=ether.ETH_TYPE_IP,
                                        ip_proto=inet.IPPROTO_TCP,
                                        ipv4_src=syn_flow.get('ip_dst'),
                                        tcp_src=syn_flow.get('port_dst'))

        actions = [parser.OFPActionOutput(13)]

        # to reset counter, delete flow first
        ofp_helper.del_flow(datapath, syn_ack_match, 100)
        ofp_helper.add_flow(datapath, 100, syn_ack_match, actions)

    def _monitor(self):
        while True:
            self._request_stats()
            hub.sleep(0.00001)

    def _request_stats(self):
        switch_list = get_switch(self.topology_api_app, None)

        for switch in switch_list:
            datapath = switch.dp
            self.logger.debug('send stats request: %016x', datapath.id)

            parser = datapath.ofproto_parser
            req = parser.OFPFlowStatsRequest(datapath)
            datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self._reset_flow(datapath)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        block_data.blocking_flow = []
        body = ev.msg.body
        for stat in body:
            if (self._is_syn_ack_rule(stat.match)):
                stat_data.packet_count = stat.byte_count / 64
                stat_data.duration_msec = stat.duration_sec * 1000 + stat.duration_nsec / 1000000

                if (stat_data.packet_count == 0):
                    stat_data.prev_duration_msec = stat_data.duration_msec

                if ((stat_data.packet_count >= group_ammount) & (stat_data.is_count == 0)):
                    diff_time = stat_data.duration_msec - stat_data.prev_duration_msec
                    stat_data.diff_avg = diff_time
                    stat_data.is_count = 1
            else:
                pass

    def _is_syn_ack_rule(self, match):
        is_ip_dst = (match.get('ipv4_src') == syn_flow.get('ip_dst'))
        is_tcp_dst = (match.get('tcp_src') == syn_flow.get('port_dst'))

        return is_ip_dst & is_tcp_dst


class StatMonitorController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(StatMonitorController, self).__init__(req, link, data, **config)
        self.stat_monitor_spp = data[stat_monitor_instance_name]

    @route('statistic', '/stat', methods=['GET'])
    def req_stat(self, req, **kwargs):
        try:
            dic = {
                'packet_count': stat_data.packet_count,
                'duration_msec': stat_data.duration_msec,
                'average_rtt': stat_data.diff_avg,
                'avg_arr': stat_data.diff_arr,
                'is_count': stat_data.is_count
                }
            body = json.dumps(dic)
        except:
            return Response(status=400)

        return Response(status=200, content_type='application/json', body=body)

    @route('statistic', '/stat/init', methods=['PUT'])
    def stat_init(self, req, **kwargs):
        try:
            stat_monitor = self.stat_monitor_spp
            stat_monitor.reset_counter()

            stat_data.diff_arr = []
            stat_data.diff_avg = 0
            stat_data.is_count = 0
            stat_data.prev_packet_count = 0
            stat_data.prev_duration_msec = 0
        except:
            return Response(status=406)
        return Response(status=202)

    @route('packet_count', '/packet_count', methods=['GET'])
    def packet_count(self, req, **kwargs):
        dic = {'packet_count': stat_data.packet_count}
        body = json.dumps(dic)
        return Response(status=200, content_type='application/json', body=body)
