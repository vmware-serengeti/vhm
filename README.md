VHM
===

vHadoop Runtime Manager

This component is responsible enabling and disabling hadoop compute
nodes based on the demand.  Specifically, VHM implements the "limit"
and "unlimit" Serengeti commands.  The limit command specifies how
many compute nodes should be enabled, and unlimit enables all the
provisioned compute nodes for the given cluster or node group.

When a user issues these commands, the serengeti web server passes the
command to VHM over the rabbit message bus using JSON format.  Here
are the fields in the message:

      int version;          // currently at version 1 
      String cluster_name;  // name of VM folder
      String jobtracker;    // IP address of jobtracker
      int instance_num;     // number of desired instances (-1 for unlimit)
      String[] node_groups; // list of nodegroups (vm folders) on which to apply the setting
      String serengeti_instance; // VM folder for the instance of serengeti that sent the command


VHM first figures out the number of currently enabled hadoop nodes by
checking the vCenter inventory.  It navigates the vCenter VM folder
hierarchy by starting at serengeti_instance folder, then looking for
cluster_name subfolder, and the node_groups subfolders underneath the
cluster_name.  VHM will then either enable or disable hadoop nodes to
meet the desired number (instance_num).  If instance_num is -1 (used
by unlimit command), then all provisioned hadoop nodes for the
specified node_groups are enabled.

VHM supports multiple enable/disable policies, but the default
implementation of disable is to first decommissions the compute node
from the JobTracker, then powers down the VM.  The default
implementation for enable is to recommission the node from JobTracker,
and then power on the VM.

While processing the command, VHM will report percentage done progress
updates to Serengeti, and it will also report any errors that were
encountered in the process.  Here are the fields of the return message
to Serengeti:


   Boolean finished;
   Boolean succeed;
   int progress;
   int error_code;
   String error_msg;
