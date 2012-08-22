"""
Base functionality for the Ganeti RAPI, client-side.

This module provides combinators which are used to provide a full RAPI client.
"""

from gentleman.errors import GanetiApiError
from gentleman.helpers import itemgetters

REPLACE_DISK_PRI = "replace_on_primary"
REPLACE_DISK_SECONDARY = "replace_on_secondary"
REPLACE_DISK_CHG = "replace_new_secondary"
REPLACE_DISK_AUTO = "replace_auto"

NODE_EVAC_PRI = "primary-only"
NODE_EVAC_SEC = "secondary-only"
NODE_EVAC_ALL = "all"

NODE_ROLE_DRAINED = "drained"
NODE_ROLE_MASTER_CANDIATE = "master-candidate"
NODE_ROLE_MASTER = "master"
NODE_ROLE_OFFLINE = "offline"
NODE_ROLE_REGULAR = "regular"

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_WAITING = "waiting"
JOB_STATUS_CANCELING = "canceling"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_CANCELED = "canceled"
JOB_STATUS_SUCCESS = "success"
JOB_STATUS_ERROR = "error"
JOB_STATUS_FINALIZED = frozenset([
  JOB_STATUS_CANCELED,
  JOB_STATUS_SUCCESS,
  JOB_STATUS_ERROR,
  ])
JOB_STATUS_ALL = frozenset([
  JOB_STATUS_QUEUED,
  JOB_STATUS_WAITING,
  JOB_STATUS_CANCELING,
  JOB_STATUS_RUNNING,
  ]) | JOB_STATUS_FINALIZED

# Legacy name
JOB_STATUS_WAITLOCK = JOB_STATUS_WAITING

# Internal constants
_REQ_DATA_VERSION_FIELD = "__version__"
_INST_NIC_PARAMS = frozenset(["mac", "ip", "mode", "link"])
_INST_CREATE_V0_DISK_PARAMS = frozenset(["size"])
_INST_CREATE_V0_PARAMS = frozenset([
    "os", "pnode", "snode", "iallocator", "start", "ip_check", "name_check",
    "hypervisor", "file_storage_dir", "file_driver", "dry_run",
])
_INST_CREATE_V0_DPARAMS = frozenset(["beparams", "hvparams"])

# Feature strings
INST_CREATE_REQV1 = "instance-create-reqv1"
INST_REINSTALL_REQV1 = "instance-reinstall-reqv1"
NODE_MIGRATE_REQV1 = "node-migrate-reqv1"
NODE_EVAC_RES1 = "node-evac-res1"

# Old feature constant names in case they're references by users of this module
_INST_CREATE_REQV1 = INST_CREATE_REQV1
_INST_REINSTALL_REQV1 = INST_REINSTALL_REQV1
_NODE_MIGRATE_REQV1 = NODE_MIGRATE_REQV1
_NODE_EVAC_RES1 = NODE_EVAC_RES1


def GetOperatingSystems(r):
    """
    Gets the Operating Systems running in the Ganeti cluster.

    @rtype: list of str
    @return: operating systems
    """

    return r.request("get", "/2/os")


def GetInfo(r):
    """
    Gets info about the cluster.

    @rtype: dict
    @return: information about the cluster
    """

    return r.request("get", "/2/info")


def RedistributeConfig(r):
    """
    Tells the cluster to redistribute its configuration files.

    @return: job id

    """
    return r.request("put", "/2/redistribute-config")


def ModifyCluster(r, **kwargs):
    """
    Modifies cluster parameters.

    More details for parameters can be found in the RAPI documentation.

    @rtype: int
    @return: job id
    """

    return r.request("put", "/2/modify", content=kwargs)


def GetClusterTags(r):
    """
    Gets the cluster tags.

    @rtype: list of str
    @return: cluster tags
    """

    return r.request("get", "/2/tags")


def AddClusterTags(r, tags, dry_run=False):
    """
    Adds tags to the cluster.

    @type tags: list of str
    @param tags: tags to add to the cluster
    @type dry_run: bool
    @param dry_run: whether to perform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "dry-run": dry_run,
        "tag": tags,
    }

    return r.request("put", "/2/tags", query=query)


def DeleteClusterTags(r, tags, dry_run=False):
    """
    Deletes tags from the cluster.

    @type tags: list of str
    @param tags: tags to delete
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    """

    query = {
        "dry-run": dry_run,
        "tag": tags,
    }

    return r.request("delete", "/2/tags", query=query)


def GetInstances(r, bulk=False):
    """
    Gets information about instances on the cluster.

    @type bulk: bool
    @param bulk: whether to return all information about all instances

    @rtype: list of dict or list of str
    @return: if bulk is True, info about the instances, else a list of instances
    """

    if bulk:
        return r.request("get", "/2/instances", query={"bulk": 1})
    else:
        instances = r.request("get", "/2/instances")
        return r.applier(itemgetters("id"), instances)


def GetInstance(r, instance):
    """
    Gets information about an instance.

    @type instance: str
    @param instance: instance whose info to return

    @rtype: dict
    @return: info about the instance
    """

    return r.request("get", "/2/instances/%s" % instance)


def GetInstanceInfo(r, instance, static=None):
    """
    Gets information about an instance.

    @type instance: string
    @param instance: Instance name
    @rtype: string
    @return: Job ID
    """

    if static is None:
        return r.request("get", "/2/instances/%s/info" % instance)
    else:
        return r.request("get", "/2/instances/%s/info" % instance,
                         query={"static": static})


def CreateInstance(r, mode, name, disk_template, disks, nics,
                   **kwargs):
    """
    Creates a new instance.

    More details for parameters can be found in the RAPI documentation.

    @type mode: string
    @param mode: Instance creation mode
    @type name: string
    @param name: Hostname of the instance to create
    @type disk_template: string
    @param disk_template: Disk template for instance (e.g. plain, diskless,
                                                file, or drbd)
    @type disks: list of dicts
    @param disks: List of disk definitions
    @type nics: list of dicts
    @param nics: List of NIC definitions
    @type dry_run: bool
    @keyword dry_run: whether to perform a dry run
    @type no_install: bool
    @keyword no_install: whether to create without installing OS(true=don't install)

    @rtype: int
    @return: job id
    """

    if _INST_CREATE_REQV1 not in r.GetFeatures():
        raise GanetiApiError("Cannot create Ganeti 2.1-style instances")

    query = {}

    if kwargs.get("dry_run"):
        query["dry-run"] = 1
    if kwargs.get("no_install"):
        query["no-install"] = 1

    # Make a version 1 request.
    body = {
        _REQ_DATA_VERSION_FIELD: 1,
        "mode": mode,
        "name": name,
        "disk_template": disk_template,
        "disks": disks,
        "nics": nics,
    }

    conflicts = set(kwargs.iterkeys()) & set(body.iterkeys())
    if conflicts:
        raise GanetiApiError("Required fields can not be specified as"
                             " keywords: %s" % ", ".join(conflicts))

    kwargs.pop("dry_run", None)
    body.update(kwargs)

    return r.request("post", "/2/instances", query=query, content=body)


def DeleteInstance(r, instance, dry_run=False):
    """
    Deletes an instance.

    @type instance: str
    @param instance: the instance to delete

    @rtype: int
    @return: job id
    """

    return r.request("delete", "/2/instances/%s" % instance,
                     query={"dry-run": dry_run})


def ModifyInstance(r, instance, **kwargs):
    """
    Modifies an instance.

    More details for parameters can be found in the RAPI documentation.

    @type instance: string
    @param instance: Instance name
    @rtype: int
    @return: job id
    """

    return r.request("put", "/2/instances/%s/modify" % instance,
                     content=kwargs)


def ActivateInstanceDisks(r, instance, ignore_size=False):
    """
    Activates an instance's disks.

    @type instance: string
    @param instance: Instance name
    @type ignore_size: bool
    @param ignore_size: Whether to ignore recorded size
    @return: job id
    """

    return r.request("put", "/2/instances/%s/activate-disks" % instance,
                     query={"ignore_size": ignore_size})


def DeactivateInstanceDisks(r, instance):
    """
    Deactivates an instance's disks.

    @type instance: string
    @param instance: Instance name
    @return: job id
    """

    return r.request("put", "/2/instances/%s/deactivate-disks" % instance)


def RecreateInstanceDisks(r, instance, disks=None, nodes=None):
    """Recreate an instance's disks.

    @type instance: string
    @param instance: Instance name
    @type disks: list of int
    @param disks: List of disk indexes
    @type nodes: list of string
    @param nodes: New instance nodes, if relocation is desired
    @rtype: string
    @return: job id
    """

    body = {}

    if disks is not None:
        body["disks"] = disks
    if nodes is not None:
        body["nodes"] = nodes

    return r.request("post", "/2/instances/%s/recreate-disks" % instance,
                     content=body)


def GrowInstanceDisk(r, instance, disk, amount, wait_for_sync=False):
    """
    Grows a disk of an instance.

    More details for parameters can be found in the RAPI documentation.

    @type instance: string
    @param instance: Instance name
    @type disk: integer
    @param disk: Disk index
    @type amount: integer
    @param amount: Grow disk by this amount (MiB)
    @type wait_for_sync: bool
    @param wait_for_sync: Wait for disk to synchronize
    @rtype: int
    @return: job id
    """

    body = {
        "amount": amount,
        "wait_for_sync": wait_for_sync,
    }

    return r.request("post", "/2/instances/%s/disk/%s/grow" %
                             (instance, disk), content=body)


def GetInstanceTags(r, instance):
    """
    Gets tags for an instance.

    @type instance: str
    @param instance: instance whose tags to return

    @rtype: list of str
    @return: tags for the instance
    """

    return r.request("get", "/2/instances/%s/tags" % instance)


def AddInstanceTags(r, instance, tags, dry_run=False):
    """
    Adds tags to an instance.

    @type instance: str
    @param instance: instance to add tags to
    @type tags: list of str
    @param tags: tags to add to the instance
    @type dry_run: bool
    @param dry_run: whether to perform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "tag": tags,
        "dry-run": dry_run,
    }

    return r.request("put", "/2/instances/%s/tags" % instance, query=query)


def DeleteInstanceTags(r, instance, tags, dry_run=False):
    """
    Deletes tags from an instance.

    @type instance: str
    @param instance: instance to delete tags from
    @type tags: list of str
    @param tags: tags to delete
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    """

    query = {
        "tag": tags,
        "dry-run": dry_run,
    }

    return r.request("delete", "/2/instances/%s/tags" % instance, query=query)


def RebootInstance(r, instance, reboot_type=None, ignore_secondaries=False,
                   dry_run=False):
    """
    Reboots an instance.

    @type instance: str
    @param instance: instance to rebot
    @type reboot_type: str
    @param reboot_type: one of: hard, soft, full
    @type ignore_secondaries: bool
    @param ignore_secondaries: if True, ignores errors for the secondary node
            while re-assembling disks (in hard-reboot mode only)
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    """

    query = {
        "ignore_secondaries": ignore_secondaries,
        "dry-run": dry_run,
    }

    if reboot_type:
        if reboot_type not in ("hard", "soft", "full"):
            raise GanetiApiError("reboot_type must be one of 'hard',"
                                 " 'soft', or 'full'")
        query["type"] = reboot_type

    return r.request("post", "/2/instances/%s/reboot" % instance, query=query)


def ShutdownInstance(r, instance, dry_run=False, no_remember=False,
                     timeout=120):
    """
    Shuts down an instance.

    @type instance: str
    @param instance: the instance to shut down
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    @type no_remember: bool
    @param no_remember: if true, will not record the state change
    @rtype: string
    @return: job id
    """

    query = {
        "dry-run": dry_run,
        "no-remember": no_remember,
    }

    content = {
        "timeout": timeout,
    }

    return r.request("put", "/2/instances/%s/shutdown" % instance,
                     query=query, content=content)


def StartupInstance(r, instance, dry_run=False, no_remember=False):
    """
    Starts up an instance.

    @type instance: str
    @param instance: the instance to start up
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    @type no_remember: bool
    @param no_remember: if true, will not record the state change
    @rtype: string
    @return: job id
    """

    query = {
        "dry-run": dry_run,
        "no-remember": no_remember,
    }

    return r.request("put", "/2/instances/%s/startup" % instance, query=query)


def ReinstallInstance(r, instance, os=None, no_startup=False, osparams=None):
    """
    Reinstalls an instance.

    @type instance: str
    @param instance: The instance to reinstall
    @type os: str or None
    @param os: The operating system to reinstall. If None, the instance's
            current operating system will be installed again
    @type no_startup: bool
    @param no_startup: Whether to start the instance automatically
    """

    if _INST_REINSTALL_REQV1 in r.GetFeatures():
        body = {
            "start": not no_startup,
        }
        if os is not None:
            body["os"] = os
        if osparams is not None:
            body["osparams"] = osparams
        return r.request("post", "/2/instances/%s/reinstall" % instance,
                         content=body)

    # Use old request format
    if osparams:
        raise GanetiApiError("Server does not support specifying OS"
                             " parameters for instance reinstallation")

    query = {
        "nostartup": no_startup,
    }

    if os:
        query["os"] = os

    return r.request("post", "/2/instances/%s/reinstall" % instance,
                     query=query)


def ReplaceInstanceDisks(r, instance, disks=None, mode=REPLACE_DISK_AUTO,
                         remote_node=None, iallocator=None, dry_run=False):
    """
    Replaces disks on an instance.

    @type instance: str
    @param instance: instance whose disks to replace
    @type disks: list of ints
    @param disks: Indexes of disks to replace
    @type mode: str
    @param mode: replacement mode to use (defaults to replace_auto)
    @type remote_node: str or None
    @param remote_node: new secondary node to use (for use with
            replace_new_secondary mode)
    @type iallocator: str or None
    @param iallocator: instance allocator plugin to use (for use with
                                         replace_auto mode)
    @type dry_run: bool
    @param dry_run: whether to perform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "mode": mode,
        "dry-run": dry_run,
    }

    if disks:
        query["disks"] = ",".join(str(idx) for idx in disks)

    if remote_node:
        query["remote_node"] = remote_node

    if iallocator:
        query["iallocator"] = iallocator

    return r.request("post", "/2/instances/%s/replace-disks" % instance,
                     query=query)


def PrepareExport(r, instance, mode):
    """
    Prepares an instance for an export.

    @type instance: string
    @param instance: Instance name
    @type mode: string
    @param mode: Export mode
    @rtype: string
    @return: Job ID
    """

    return r.request("put", "/2/instances/%s/prepare-export" % instance,
                     query={"mode": mode})


def ExportInstance(r, instance, mode, destination, shutdown=None,
                   remove_instance=None, x509_key_name=None,
                   destination_x509_ca=None):
    """
    Exports an instance.

    @type instance: string
    @param instance: Instance name
    @type mode: string
    @param mode: Export mode
    @rtype: string
    @return: Job ID
    """

    body = {
        "destination": destination,
        "mode": mode,
    }

    if shutdown is not None:
        body["shutdown"] = shutdown

    if remove_instance is not None:
        body["remove_instance"] = remove_instance

    if x509_key_name is not None:
        body["x509_key_name"] = x509_key_name

    if destination_x509_ca is not None:
        body["destination_x509_ca"] = destination_x509_ca

    return r.request("put", "/2/instances/%s/export" % instance, content=body)


def MigrateInstance(r, instance, mode=None, cleanup=None):
    """
    Migrates an instance.

    @type instance: string
    @param instance: Instance name
    @type mode: string
    @param mode: Migration mode
    @type cleanup: bool
    @param cleanup: Whether to clean up a previously failed migration
    """

    body = {}

    if mode is not None:
        body["mode"] = mode

    if cleanup is not None:
        body["cleanup"] = cleanup

    return r.request("put", "/2/instances/%s/migrate" % instance,
                     content=body)


def FailoverInstance(r, instance, iallocator=None, ignore_consistency=False,
                     target_node=None):
    """Does a failover of an instance.

    @type instance: string
    @param instance: Instance name
    @type iallocator: string
    @param iallocator: Iallocator for deciding the target node for
        shared-storage instances
    @type ignore_consistency: bool
    @param ignore_consistency: Whether to ignore disk consistency
    @type target_node: string
    @param target_node: Target node for shared-storage instances
    @rtype: string
    @return: job id
    """

    body = {
        "ignore_consistency": ignore_consistency,
    }

    if iallocator is not None:
        body["iallocator"] = iallocator
    if target_node is not None:
        body["target_node"] = target_node


    return r.request("put", "/2/instances/%s/failover" % instance,
                     content=body)


def RenameInstance(r, instance, new_name, ip_check, name_check=None):
    """
    Changes the name of an instance.

    @type instance: string
    @param instance: Instance name
    @type new_name: string
    @param new_name: New instance name
    @type ip_check: bool
    @param ip_check: Whether to ensure instance's IP address is inactive
    @type name_check: bool
    @param name_check: Whether to ensure instance's name is resolvable
    """

    body = {
        "ip_check": ip_check,
        "new_name": new_name,
    }

    if name_check is not None:
        body["name_check"] = name_check

    return r.request("put", "/2/instances/%s/rename" % instance, content=body)


def GetInstanceConsole(r, instance):
    """
    Request information for connecting to instance's console.

    @type instance: string
    @param instance: Instance name
    """

    return r.request("get", "/2/instances/%s/console" % instance)


def GetJobs(r):
    """
    Gets all jobs for the cluster.

    @rtype: list of int
    @return: job ids for the cluster
    """

    jobs = r.request("get", "/2/jobs")
    return r.applier(itemgetters("id"), jobs)


def GetJobStatus(r, job_id):
    """
    Gets the status of a job.

    @type job_id: int
    @param job_id: job id whose status to query

    @rtype: dict
    @return: job status
    """

    return r.request("get", "/2/jobs/%s" % job_id)


def WaitForJobChange(r, job_id, fields, prev_job_info, prev_log_serial):
    """
    Waits for job changes.

    @type job_id: int
    @param job_id: Job ID for which to wait
    """

    body = {
        "fields": fields,
        "previous_job_info": prev_job_info,
        "previous_log_serial": prev_log_serial,
    }

    return r.request("get", "/2/jobs/%s/wait" % job_id, content=body)


def CancelJob(r, job_id, dry_run=False):
    """
    Cancels a job.

    @type job_id: int
    @param job_id: id of the job to delete
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    """

    return r.request("delete", "/2/jobs/%s" % job_id,
                             query={"dry-run": dry_run})


def GetNodes(r, bulk=False):
    """
    Gets all nodes in the cluster.

    @type bulk: bool
    @param bulk: whether to return all information about all instances

    @rtype: list of dict or str
    @return: if bulk is true, info about nodes in the cluster,
            else list of nodes in the cluster
    """

    if bulk:
        return r.request("get", "/2/nodes", query={"bulk": 1})
    else:
        nodes = r.request("get", "/2/nodes")
        return r.applier(itemgetters("id"), nodes)


def GetNode(r, node):
    """
    Gets information about a node.

    @type node: str
    @param node: node whose info to return

    @rtype: dict
    @return: info about the node
    """

    return r.request("get", "/2/nodes/%s" % node)


def EvacuateNode(r, node, iallocator=None, remote_node=None, dry_run=False,
                 early_release=False, mode=None, accept_old=False):
    """
    Evacuates instances from a Ganeti node.

    @type node: str
    @param node: node to evacuate
    @type iallocator: str or None
    @param iallocator: instance allocator to use
    @type remote_node: str
    @param remote_node: node to evaucate to
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    @type early_release: bool
    @param early_release: whether to enable parallelization
    @type accept_old: bool
    @param accept_old: Whether caller is ready to accept old-style
        (pre-2.5) results

    @rtype: string, or a list for pre-2.5 results
    @return: Job ID or, if C{accept_old} is set and server is pre-2.5,
        list of (job ID, instance name, new secondary node); if dry_run
        was specified, then the actual move jobs were not submitted and
        the job IDs will be C{None}

    @raises GanetiApiError: if an iallocator and remote_node are both
            specified
    """

    if iallocator and remote_node:
        raise GanetiApiError("Only one of iallocator or remote_node can"
                             " be used")

    query = {
        "dry-run": dry_run,
    }

    if iallocator:
        query["iallocator"] = iallocator
    if remote_node:
        query["remote_node"] = remote_node

    if _NODE_EVAC_RES1 in r.GetFeatures():
        # Server supports body parameters
        body = {
            "early_release": early_release,
        }

        if iallocator is not None:
            body["iallocator"] = iallocator
        if remote_node is not None:
            body["remote_node"] = remote_node
        if mode is not None:
            body["mode"] = mode
    else:
        # Pre-2.5 request format
        body = None

        if not accept_old:
            raise GanetiApiError("Server is version 2.4 or earlier and"
                                 " caller does not accept old-style"
                                 " results (parameter accept_old)")

        # Pre-2.5 servers can only evacuate secondaries
        if mode is not None and mode != NODE_EVAC_SEC:
            raise GanetiApiError("Server can only evacuate secondary instances")

        if iallocator is not None:
            query["iallocator"] = iallocator
        if remote_node is not None:
            query["remote_node"] = remote_node
        if query:
            query["early_release"] = 1

    return r.request("post", "/2/nodes/%s/evacuate" % node, query=query,
                     content=body)


def MigrateNode(r, node, mode=None, dry_run=False, iallocator=None,
                target_node=None):
    """
    Migrates all primary instances from a node.

    @type node: str
    @param node: node to migrate
    @type mode: string
    @param mode: if passed, it will overwrite the live migration type,
            otherwise the hypervisor default will be used
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    @type iallocator: string
    @param iallocator: instance allocator to use
    @type target_node: string
    @param target_node: Target node for shared-storage instances

    @rtype: int
    @return: job id
    """

    query = {
        "dry-run": dry_run,
    }

    if _NODE_MIGRATE_REQV1 in r.GetFeatures():
        body = {}

        if mode is not None:
            body["mode"] = mode
        if iallocator is not None:
            body["iallocator"] = iallocator
        if target_node is not None:
            body["target_node"] = target_node

    else:
        # Use old request format
        if target_node is not None:
            raise GanetiApiError("Server does not support specifying"
                                 " target node for node migration")

        body = None

        if mode is not None:
            query["mode"] = mode

    return r.request("post", "/2/nodes/%s/migrate" % node, query=query,
                     content=body)


def GetNodeRole(r, node):
    """
    Gets the current role for a node.

    @type node: str
    @param node: node whose role to return

    @rtype: str
    @return: the current role for a node
    """

    return r.request("get", "/2/nodes/%s/role" % node)


def SetNodeRole(r, node, role, force=False, auto_promote=False):
    """
    Sets the role for a node.

    @type node: str
    @param node: the node whose role to set
    @type role: str
    @param role: the role to set for the node
    @type force: bool
    @param force: whether to force the role change
    @type auto_promote: bool
    @param auto_promote: Whether node(s) should be promoted to master
        candidate if necessary

    @rtype: int
    @return: job id
    """

    query = {
        "force": force,
        "auto_promote": auto_promote,
    }

    return r.request("put", "/2/nodes/%s/role" % node, query=query,
                     content=role)


def PowercycleNode(r, node, force=False):
    """
    Powercycles a node.

    @type node: string
    @param node: Node name
    @type force: bool
    @param force: Whether to force the operation
    @rtype: string
    @return: job id
    """

    query = {
        "force": force,
    }

    return r.request("post", "/2/nodes/%s/powercycle" % node, query=query)


def ModifyNode(r, node, **kwargs):
    """
    Modifies a node.

    More details for parameters can be found in the RAPI documentation.

    @type node: string
    @param node: Node name
    @rtype: string
    @return: job id
    """

    return r.request("post", "/2/nodes/%s/modify" % node, content=kwargs)


def GetNodeStorageUnits(r, node, storage_type, output_fields):
    """
    Gets the storage units for a node.

    @type node: str
    @param node: the node whose storage units to return
    @type storage_type: str
    @param storage_type: storage type whose units to return
    @type output_fields: str
    @param output_fields: storage type fields to return

    @rtype: int
    @return: job id where results can be retrieved
    """

    query = {
        "storage_type": storage_type,
        "output_fields": output_fields,
    }

    return r.request("get", "/2/nodes/%s/storage" % node, query=query)


def ModifyNodeStorageUnits(r, node, storage_type, name, allocatable=None):
    """
    Modifies parameters of storage units on the node.

    @type node: str
    @param node: node whose storage units to modify
    @type storage_type: str
    @param storage_type: storage type whose units to modify
    @type name: str
    @param name: name of the storage unit
    @type allocatable: bool or None
    @param allocatable: Whether to set the "allocatable" flag on the storage
                                            unit (None=no modification, True=set, False=unset)

    @rtype: int
    @return: job id
    """

    query = {
        "storage_type": storage_type,
        "name": name,
    }

    if allocatable is not None:
        query["allocatable"] = allocatable

    return r.request("put", "/2/nodes/%s/storage/modify" % node, query=query)


def RepairNodeStorageUnits(r, node, storage_type, name):
    """
    Repairs a storage unit on the node.

    @type node: str
    @param node: node whose storage units to repair
    @type storage_type: str
    @param storage_type: storage type to repair
    @type name: str
    @param name: name of the storage unit to repair

    @rtype: int
    @return: job id
    """

    query = {
        "storage_type": storage_type,
        "name": name,
    }

    return r.request("put", "/2/nodes/%s/storage/repair" % node, query=query)


def GetNodeTags(r, node):
    """
    Gets the tags for a node.

    @type node: str
    @param node: node whose tags to return

    @rtype: list of str
    @return: tags for the node
    """

    return r.request("get", "/2/nodes/%s/tags" % node)


def AddNodeTags(r, node, tags, dry_run=False):
    """
    Adds tags to a node.

    @type node: str
    @param node: node to add tags to
    @type tags: list of str
    @param tags: tags to add to the node
    @type dry_run: bool
    @param dry_run: whether to perform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "tag": tags,
        "dry-run": dry_run,
    }

    return r.request("put", "/2/nodes/%s/tags" % node, query=query,
                     content=tags)


def DeleteNodeTags(r, node, tags, dry_run=False):
    """
    Delete tags from a node.

    @type node: str
    @param node: node to remove tags from
    @type tags: list of str
    @param tags: tags to remove from the node
    @type dry_run: bool
    @param dry_run: whether to perform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "tag": tags,
        "dry-run": dry_run,
    }

    return r.request("delete", "/2/nodes/%s/tags" % node, query=query)


def GetGroups(r, bulk=False):
    """
    Gets all node groups in the cluster.

    @type bulk: bool
    @param bulk: whether to return all information about the groups

    @rtype: list of dict or str
    @return: if bulk is true, a list of dictionaries with info about all node
            groups in the cluster, else a list of names of those node groups
    """

    if bulk:
        return r.request("get", "/2/groups", query={"bulk": 1})
    else:
        groups = r.request("get", "/2/groups")
        return r.applier(itemgetters("name"), groups)


def GetGroup(r, group):
    """
    Gets information about a node group.

    @type group: str
    @param group: name of the node group whose info to return

    @rtype: dict
    @return: info about the node group
    """

    return r.request("get", "/2/groups/%s" % group)


def CreateGroup(r, name, alloc_policy=None, dry_run=False):
    """
    Creates a new node group.

    @type name: str
    @param name: the name of node group to create
    @type alloc_policy: str
    @param alloc_policy: the desired allocation policy for the group, if any
    @type dry_run: bool
    @param dry_run: whether to peform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "dry-run": dry_run,
    }

    body = {
        "name": name,
        "alloc_policy": alloc_policy
    }

    return r.request("post", "/2/groups", query=query, content=body)


def ModifyGroup(r, group, **kwargs):
    """
    Modifies a node group.

    More details for parameters can be found in the RAPI documentation.

    @type group: string
    @param group: Node group name
    @rtype: int
    @return: job id
    """

    return r.request("put", "/2/groups/%s/modify" % group, content=kwargs)


def DeleteGroup(r, group, dry_run=False):
    """
    Deletes a node group.

    @type group: str
    @param group: the node group to delete
    @type dry_run: bool
    @param dry_run: whether to peform a dry run

    @rtype: int
    @return: job id
    """

    query = {
        "dry-run": dry_run,
    }

    return r.request("delete", "/2/groups/%s" % group, query=query)


def RenameGroup(r, group, new_name):
    """
    Changes the name of a node group.

    @type group: string
    @param group: Node group name
    @type new_name: string
    @param new_name: New node group name

    @rtype: int
    @return: job id
    """

    body = {
        "new_name": new_name,
    }

    return r.request("put", "/2/groups/%s/rename" % group, content=body)



def AssignGroupNodes(r, group, nodes, force=False, dry_run=False):
    """
    Assigns nodes to a group.

    @type group: string
    @param group: Node gropu name
    @type nodes: list of strings
    @param nodes: List of nodes to assign to the group

    @rtype: int
    @return: job id

    """

    query = {
        "force": force,
        "dry-run": dry_run,
    }

    body = {
        "nodes": nodes,
    }

    return r.request("put", "/2/groups/%s/assign-nodes" % group, query=query,
                     content=body)


def GetGroupTags(r, group):
    """
    Gets tags for a node group.

    @type group: string
    @param group: Node group whose tags to return

    @rtype: list of strings
    @return: tags for the group
    """

    return r.request("get", "/2/groups/%s/tags" % group)


def AddGroupTags(r, group, tags, dry_run=False):
    """
    Adds tags to a node group.

    @type group: str
    @param group: group to add tags to
    @type tags: list of string
    @param tags: tags to add to the group
    @type dry_run: bool
    @param dry_run: whether to perform a dry run

    @rtype: string
    @return: job id
    """

    query = {
        "dry-run": dry_run,
        "tag": tags,
    }

    return r.request("put", "/2/groups/%s/tags" % group, query=query)


def DeleteGroupTags(r, group, tags, dry_run=False):
    """
    Deletes tags from a node group.

    @type group: str
    @param group: group to delete tags from
    @type tags: list of string
    @param tags: tags to delete
    @type dry_run: bool
    @param dry_run: whether to perform a dry run
    @rtype: string
    @return: job id
    """

    query = {
        "dry-run": dry_run,
        "tag": tags,
    }

    return r.request("delete", "/2/groups/%s/tags" % group, query=query)


def Query(r, what, fields, qfilter=None):
    """
    Retrieves information about resources.

    @type what: string
    @param what: Resource name, one of L{constants.QR_VIA_RAPI}
    @type fields: list of string
    @param fields: Requested fields
    @type qfilter: None or list
    @param qfilter: Query filter

    @rtype: string
    @return: job id
    """

    body = {
        "fields": fields,
    }

    if qfilter is not None:
        body["qfilter"] = body["filter"] = qfilter

    return r.request("put", "/2/query/%s" % what, content=body)


def QueryFields(r, what, fields=None):
    """
    Retrieves available fields for a resource.

    @type what: string
    @param what: Resource name, one of L{constants.QR_VIA_RAPI}
    @type fields: list of string
    @param fields: Requested fields

    @rtype: string
    @return: job id
    """

    query = {}

    if fields is not None:
        query["fields"] = ",".join(fields)

    return r.request("get", "/2/query/%s/fields" % what, query=query)
