import sys
import time
import re

from NaServer import NaServer
from NaElement import NaElement

class NetCrAPIOut(Exception):
    '''
    Custom exception for the NetApp API.
    '''
    def __init__(self, errmsg):
        self.errmsg = errmsg

    def __str__(self):
        return repr(self.errmsg)


def check_zapi_error(output, errormsg='An error occured: %s'):
    """@todo: Docstring for check_zapi_error.

    :output: @todo
    :errormsg: @todo
    :returns: @todo

    """
    if output and output.results_errno() != 0:
        reason = output.results_reason()
        raise NetCrAPIOut(errormsg % reason)


class filer:
    '''
    Class for interacting with Filers or Clusters
    '''
    def __init__(self, filer, user, password, transport_type='HTTPS'):
        '''
        Creates the connection to the filer. Transport_type defaults to 'HTTPS'.
        Todo:
            Allow different connection styles?
        '''
        conn = NaServer(filer, 1, 3)
        out = conn.set_transport_type(transport_type)
        check_zapi_error(out, "connection to filer failed: %s")
        out = conn.set_style("LOGIN")
        check_zapi_error(out, "connection to filer failed: %s")
        out = conn.set_admin_user(user, password)
        check_zapi_error(out, "connection to filer failed: %s")
        self.conn = conn
        
    def invoke(self, *args):
        """@todo: Docstring for invoke.

        :args: Arguments passed to NaServer.invoke() 
        :returns: API output object

        """
        out = self.conn.invoke(*args)
        check_zapi_error(out)
        return out

    def invoke_elem(self,  naelem):
        """@todo: Docstring for invoke_elem.

        :naelem: NaElement object
        :returns: output object

        """
        out = self.conn.invoke_elem(naelem)
        check_zapi_error(out)
        return out

    def get_filer_api_list(self):
        '''
         @todo: return list of ONTAP APIs
        '''
        #list_in = NaElement("system-api-list")
        pass

    def get_perf_objects(self):
        '''
        Lists all performance objects along with privilege levels.
        '''
        list_in = NaElement("perf-object-list-info")
        out = self.invoke_elem(list_in)
        check_zapi_error(out)
        obj_info = out.child_get("objects")
        result = obj_info.children_get()
        obj_list = []
        for obj in result:
            obj_name = obj.child_get_string("name")
            priv = obj.child_get_string("privilege-level")
            obj_list.append([obj_name, priv])
        return obj_list
    def get_instance_list(self, perf_obj):
        '''
        Lists instances associated with the supplied performance object.
        Some objects actually have instances, like 'volume' and 'aggregate', while
        others only have themselves (so sad...) like 'system' or 'cifs'
        '''
        list_in = NaElement("perf-object-instance-list-info")
        list_in.child_add_string("objectname", perf_obj)

        out = self.invoke_elem(list_in)
        check_zapi_error(out)
        inst_info = out.child_get("instances")
        result = inst_info.children_get()

        instance_names = []
        for inst in result:
            inst_name = inst.child_get_string("name")
            instance_names.append(inst_name)
        
        return instance_names

    def get_counter_list(self, perf_obj):
        '''
        Returns the counters associated with a performance object.
        This function also outputs the 'base counter', 'privilege level' and 
        'unit' of the counter.
        '''
        list_in = NaElement("perf-object-counter-list-info")
        list_in.child_add_string("objectname", perf_obj)
        out = self.invoke_elem(list_in)
        check_zapi_error(out)
        counter_info = out.child_get("counters")
        result = counter_info.children_get()
        counter_list = []
        for counter in result:
            counter_name = counter.child_get_string("name")
            if counter.child_get_string("base-counter"):
                base_counter = counter.child_get_string("base-counter")
            else:
                base_counter = "None"

            privilege_level = counter.child_get_string("privilege-level")

            if counter.child_get_string("unit"):
                unit = counter.child_get_string("unit")
            else:
                unit = "None"

            if counter.child_get_string("properties"):
                counter_type = counter.child_get_string("properties")
            else:
                counter_type = "None"
            counter_list.append([counter_name,
                                 base_counter,
                                 counter_type,
                                 privilege_level,
                                 unit])
        return counter_list

    def get_volumes(self):
        """@todo: Docstring for get_volumes.
        :returns: @todo

        """
        pass

    def get_aggrs(self):
        """@todo: Docstring for get_aggrs.
        :returns: @todo

        """
        out = self.invoke('aggr-list-info')
        check_zapi_error(out)
        aggr_info_items = {"name": "string",
                           "state": "string",
                           "size-total": "integer",
                           "size-used": "integer",
                           "size-available": "integer",
                           "volume-count": "integer",
                           "has-local-root": "string"
                          }
        aggr_list = out.child_get('aggregates').children_get()
        aggr_info = {}
        for aggr_obj in aggr_list:
            aggr_name = aggr_obj.child_get_string('name')
            aggr_info[aggr_name] = {}
            #print aggr_name
            for item_name, item_type in aggr_info_items.iteritems():
                #print("name: %s, type: %s" % (item_name, item_type))
                if item_type == 'string':
                    aggr_info[aggr_name][item_name] = aggr_obj.child_get_string(item_name)
                elif item_type == 'integer':
                    aggr_info[aggr_name][item_name] = aggr_obj.child_get_int(item_name)
        return aggr_info            

        pass

    def create_vol(self, name, aggr, size):
        """@todo: Docstring for create_vol.

        :name: Name of volume
        :aggr: Containing aggregate
        :size: Size in the form of <number>[kmgt]
        :returns: 'volume' object

        """
        vol = volume(self, name)
        vol.create(aggr, size)
        return vol

    def system_info(self):
        """@todo: Docstring for system_info.
        :returns: @todo
        This method does not work in cmode

        """
        out = self.invoke('system-get-info')
        check_zapi_error(out)
        system_info_objs = {"backplane-part-number": "string", 
                            "backplane-revision": "string",
                            "backplane-serial-number": "string",
                            "board-speed": "integer",
                            "board-type": "string",
                            "controller-address": "string",
                            "cpu-ciob-revision-id": "string",
                            "cpu-firmware-release": "string",
                            "cpu-microcode-version": "string",
                            "cpu-part-number": "string",
                            "cpu-processor-id": "string",
                            "cpu-processor-type": "string",
                            "cpu-revision": "string",
                            "cpu-serial-number": "string",
                            "memory-size": "integer",
                            "number-of-processors": "integer",
                            "partner-system-id": "string",
                            "partner-system-name": "string",
                            "partner-system-serial-number": "string",
                            "prod-type": "string",
                            "supports-raid-array": "string",
                            "system-id": "string",
                            "system-machine-type": "string",
                            "system-model": "string",
                            "system-name": "string",
                            "system-revision": "string",
                            "system-serial-number": "string",
                            "vendor-id": "string"
                           }
        sysinfo_filers = out.children_get()
        sysinfo = {}
        for filer_obj in sysinfo_filers:
            filer_name = filer_obj.child_get_string('system-name')
            sysinfo[filer_name] = {}
            for item_name, item_type in system_info_objs.iteritems():
                if item_type == 'string':
                    sysinfo[filer_name][item_name] = filer_obj.child_get_string(item_name)
                elif item_type == 'integer':
                    sysinfo[filer_name][item_name] = filer_obj.child_get_int(item_name)
        return sysinfo



class volume:
    def __init__(self, filer_inst, name):
        self.invoke = filer_inst.invoke
        self.invoke_elem = filer_inst.invoke_elem
        self.name = name

    def create(self, aggr, size):
        """@todo: Docstring for create.

        :aggr: @todo
        :size: @todo
        :returns: @todo

        """
        list_in = NaElement('volume-create')
        #name
        list_in.child_add_string("volume", self.name)
        #aggr
        list_in.child_add_string("containing-aggr-name", aggr)
        #size
        size_pattern = re.compile('^[1-9][0-9]*[kmgt]')
        if size_pattern.match(size):
            list_in.child_add_string("size", size)
        else:
            raise NetCrAPIOut("Size not valid. Please use <number>k|m|g|t.")
            pass
        #space reservation (none)
        list_in.child_add_string("space-reserve", "none")
        #language (en_US)
        list_in.child_add_string("language-code", "en_US")
        out = self.invoke_elem(list_in)
        check_zapi_error(out)

        pass
    def online(self):
        """@todo: Docstring for online.
        :returns: @todo

        """
        out = self.invoke('volume-online',
                         'name', self.name
                         )
        check_zapi_error(out)
    def offline(self, cifs_delay=0):
        """@todo: Docstring for offline.
        :cifs_delay: number of minutes to delay offline for cifs users
                     defaults to 0 (immediate termination)
        :returns: @todo

        """
        out = self.invoke('volume-offline',
                         'name', self.name,
                         'cifs-delay', cifs_delay
                         )
        check_zapi_error(out)
    def destroy(self):
        """@todo: Docstring for destroy.
        :returns: @todo

        """
        out = self.invoke('volume-destroy',
                         'name', self.name)
        check_zapi_error(out)
    def get_option(self, option):
        """@todo: Docstring for get_option.

        :option: @todo
        :returns: @todo

        """
        pass
    def set_option(self, option, value):
        """@todo: Docstring for set_option.

        :option: @todo
        :value: @todo
        :returns: @todo

        """
        out = self.invoke('volume-set-option',
                          'volume', self.name,
                          'option-name', option,
                          'option-value', value
                          )
        check_zapi_error(out)
    def get_snapshots(self):
        """@todo: Docstring for get_snapshots.
        :returns: @todo

        """
    #    out = self.invoke("snapshot-list-info",
    #                               "target-name", volume_name,
    #                               "target-type", "volume",
    #                               "terse", "True"
    #                              )

        pass
    def create_snapshot(self, snap_name):
        """@todo: Docstring for create_snapshot.

        :snap_name: @todo
        :returns: @todo

        """
        pass
    def delete_snapshot(self, snap_name):
        """@todo: Docstring for delete_snapshot.

        :snap_name: @todo
        :returns: @todo

        """
        pass
    #def get_snapshots(self):
    #    out = self.invoke("snapshot-list-info",
    #                               "target-name", volume_name,
    #                               "target-type", "volume",
    #                               "terse", "True"
    #                              )

