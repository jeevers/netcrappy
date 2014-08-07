import sys
import time
import re
import copy

from NaServer import NaServer
from NaElement import NaElement

import ontap7mode

class Cluster(ontap7mode.Filer):

    """Docstring for cluster. """

    def __init__(self, filer_name, user, password, transport_type='HTTPS'):
        """@todo: to be defined1. """
        ontap7mode.Filer.__init__(self,
                                 filer_name,
                                 user,
                                 password,
                                 transport_type='HTTPS')
        self.vserver_objs = {}
        #for vserver in self.get_vservers():
        #    vserver_obj = copy.deepcopy(self)
        #    vserver_obj.set_vserver(vserver)
        #    self.vserver_objs[vserver] = vserver_obj
    
    def set_vserver(self, vserver):
        """@todo: Docstring for set_vserver.

        :vserver: @todo
        :returns: @todo

        """
        self.conn.set_vserver(vserver)

    def vserver(self, vserver_name):
        """@todo: Docstring for vserver.

        :vserver_name: @todo
        :returns: @todo

        """
        #set_vserver sets the vserver for the Cluster object, so
        #copy the cluster object and set it on the copy
        try:
            #pull a cached vserver object
            vserver_obj = self.vserver_objs[vserver_name]
        except KeyError:
            #or generate a new object and place in a dict for later use
            vserver_obj = copy.deepcopy(self)
            vserver_obj.set_vserver(vserver_name)
            self.vserver_objs[vserver_name] = vserver_obj
        return vserver_obj

    def api_get_iter(self,  iter_api):
        """@todo: Docstring for api_get_iter.

        :iter_api: @todo
        :returns: @todo

        """
        obj_list = []
        objs = self.invoke(iter_api)
        obj_list = obj_list + objs.child_get('attributes-list').children_get()
        next_tag = objs.child_get_string('next-tag')
        while next_tag is not None:
            objs = self.invoke(iter_api, 'tag', next_tag)
            obj_list = obj_list + objs.child_get('attributes-list').children_get()
            next_tag = objs.child_get_string('next-tag')
        return obj_list

    def get_vservers(self):
        """@todo: Docstring for get_vservers.
        :returns: @todo

        """
        vserver_list = self.api_get_iter('vserver-get-iter')
        vserver_dict = {}
        for vserver in vserver_list:
            name = vserver.child_get_string('vserver-name')
            state = vserver.child_get_string('state')
            type = vserver.child_get_string('vserver-type')
            if vserver.child_get('allowed-protocols') is not None:
                allowed_protocols = []
                for proto in vserver.child_get('allowed-protocols').children_get():
                    #it should work like this:
                    #allowed_protocols.append(proto.child_get_string('protocol'))
                    #but NOOOOOOOOOOOOOOOO
                    inside_xml = re.compile('<protocol>(.*)</protocol>')
                    allowed_protocols.append(inside_xml.findall(proto.sprintf())[0])
            else:
                allowed_protocols = None
            if vserver.child_get('vserver-aggr-info-list') is not None:
                #This will only return data if an aggr has been delegated 
                #to the vserver
                aggr_dict = {}
                for aggr in vserver.child_get('vserver-aggr-info-list').children_get():
                    aggr_name = aggr.child_get_string('aggr-name')
                    aggr_avail = aggr.child_get_int('aggr-availsize')
                    aggr_dict[aggr_name] = {'aggr-availsize': aggr_avail}
            else:
                aggr_dict = None

            vserver_dict[name] = {'state': state,
                                  'type': type,
                                  'allowed-protocols': allowed_protocols,
                                  'vserver-aggr-info': aggr_dict
                                 }
        return vserver_dict

    def get_volumes(self, vserver=None, max_records=20):
        """@todo: Docstring for get_volumes.

        :vserver: @todo
        :returns: @todo

        """
        if vserver:
            orig_vserver = self.conn.get_vserver()
            self.set_vserver(vserver)
        volume_list = self.api_get_iter('volume-get-iter')
        if vserver:
            self.set_vserver(orig_vserver)
        volumes_dict = {}
        for volume in volume_list:
            name = volume.child_get('volume-id-attributes').child_get_string('name')
            owning_vserver = volume.child_get('volume-id-attributes').child_get_string('owning-vserver-name')
            state = volume.child_get('volume-state-attributes').child_get_string('state')
            volumes_dict[name] = { 'state': state,
                                  'owning-vserver-name': owning_vserver
                                 }
        return volumes_dict

    def create_vol(self, name, aggr, size, vserver_name=None):
        """@todo: Docstring for create_vol.

        :name: @todo
        :aggr: @todo
        :size: @todo
        :returns: @todo

        """
        if (vserver_name == None) and (self.conn.get_vserver() == ''):
            raise ontap7mode.NetCrAPIOut('Vserver name not specified')
            pass
        elif vserver_name != None:
            vserver_obj = self.vserver(vserver_name)
        else:
            vserver_obj = self
        vol = ClusterVolume(vserver_obj, name)
        vol.create(aggr, size)
        return vol
        

class ClusterVolume(ontap7mode.Volume):
    def __init__(self, filer_inst, name):
        ontap7mode.Volume.__init__(self, filer_inst, name)

    def get_info(self):
        """@todo: Docstring for get_info.
        :returns: @todo

        """
        volume_info = {'volume-autosize-attributes': {
                           'maximum-size': 'integer',
                           'increment-size': 'integer',
                           'is-enabled': 'boolean'
                       },
                       'volume-id-attributes': {
                           'containing-aggregate-name': 'string',
                           'type': 'string',
                           'owning-vserver-name': 'string'
                       },
                       'volume-inode-attributes': {
                           'files-total': 'integer',
                           'files-used': 'integer',
                           'block-type': 'string'
                       },
                       'volume-state-attributes': {
                           'state': 'string'
                       },
                       'volume-space-attributes': {
                           'percentage-size-used': 'integer',
                           'size-total': 'integer',
                           'size-available': 'integer',
                           'size-used': 'integer',
                           'size-used-by-snapshots': 'integer',
                           'space-guarantee': 'string',
                           'size': 'integer',
                           'percentage-snapshot-reserve': 'integer',
                           'percentage-fractional-reserve': 'integer',
                       },
                       'volume-sis-attributes': {
                           'is-sis-volume': 'boolean',
                           'deduplication-space-saved': 'integer',
                           'compression-space-saved': 'integer',
                           'total-space-saved': 'integer'
                       }
                      }
        vol_get_iter = NaElement('volume-get-iter')
        query = NaElement('query')
        vol_query_attrs = NaElement('volume-attributes')
        vol_id_attrs = NaElement('volume-id-attributes')
        vol_get_iter.child_add(query)
        query.child_add(vol_query_attrs)
        vol_query_attrs.child_add(vol_id_attrs)
        vol_id_attrs.child_add_string('name', self.name)
        out = self.invoke_elem(vol_get_iter)
        attributes_list = out.child_get('attributes-list').children_get()[0]
        #print attributes_list.sprintf()
        vol_info_dict = {}
        for vol_attribute_type, attributes in volume_info.iteritems():
            attribute_group = attributes_list.child_get(vol_attribute_type)
            for attr_name, data_type in attributes.iteritems():
                #print vol_attribute_type
                #print attribute_group
                #print attr_name
                #print data_type
                if data_type == 'string':
                    data = attribute_group.child_get_string(attr_name)
                    vol_info_dict[attr_name] = data
                elif data_type == 'integer':
                    data = attribute_group.child_get_int(attr_name)
                    vol_info_dict[attr_name] = data
                elif data_type == 'boolean':
                    data = (attribute_group.child_get_string(attr_name) == 'true')
                    vol_info_dict[attr_name] = data
        return vol_info_dict

    def offline(self):
        """@todo: Docstring for offline.
        :returns: @todo

        """
        self.invoke('volume-offline',
                         'name', self.name,
                         )

    def set_snapshot_schedule(self):
        pass

    def sis_status(self):
        """@todo: Docstring for sis_status.
        :returns: @todo

        """
        out = self.invoke('sis-status'
                          'path', "/vol/%s" % self.name
                         )
        sis_object = out.child_get('sis-object')
        status = sis_object.child_get_string('status')
        return status

    def get_qtree_security(self):
        """@todo: Docstring for get_qtree_security.
        :returns: @todo

        """
        out = self.invoke_cli("qtree security /vol/%s" % self.name)
        output_message = out.child_get_string("cli-output")
        #output_code = out.child_get_int("cli-result-value")
        qtree_pattern = re.compile("^.*has (mixed|ntfs|unix) security style and oplocks are (enabled|disabled).\n$")
        match = qtree_pattern.match(output_message)
        if match:
            security_style = match.groups()[0]
            return security_style
        else:
            raise ontap7mode.NetCrAPIOut(output_message.strip())

    def set_qtree_security(self, security):
        """@todo: Docstring for set_qtree_security.

        :security: qtree security value, can be unix, ntfs, or mixed
        :returns: @todo

        """
        if security not in ['unix', 'ntfs', 'mixed']:
            raise ontap7mode.NetCrAPIOut(
                'The security style can only be unix, ntfs, or mixed.')
        else:
            out = self.invoke_cli('qtree security /vol/%s %s' %
                                  (self.name, security)
                                 )
            output_message = out.child_get_string('cli-output')
            if not output_message == '':
                raise ontap7mode.NetCrAPIOut(output_message.strip())
    
    def get_autosize(self):
        """@todo: Docstring for get_autosize.
        :returns: @todo

        """
        autosize_params = self.get_info()['autosize']
        return autosize_params
