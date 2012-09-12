package com.vmware.vhadoop.adaptor.vc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.soap.SOAPFaultException;

import com.vmware.vim25.DynamicProperty;
import com.vmware.vim25.InvalidLocaleFaultMsg;
import com.vmware.vim25.InvalidLoginFaultMsg;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.ObjectContent;
import com.vmware.vim25.ObjectSpec;
import com.vmware.vim25.ObjectUpdate;
import com.vmware.vim25.ObjectUpdateKind;
import com.vmware.vim25.PropertyChange;
import com.vmware.vim25.PropertyChangeOp;
import com.vmware.vim25.PropertyFilterSpec;
import com.vmware.vim25.PropertyFilterUpdate;
import com.vmware.vim25.PropertySpec;
import com.vmware.vim25.RuntimeFaultFaultMsg;
import com.vmware.vim25.ServiceContent;
import com.vmware.vim25.TaskInfoState;
import com.vmware.vim25.TraversalSpec;
import com.vmware.vim25.UpdateSet;
import com.vmware.vim25.VimPortType;
import com.vmware.vim25.VimService;
import com.vmware.vim25.VirtualMachinePowerState;

/**
 * Note - to be deleted - this is replaced by VCAdaptor and is here for reference
 * 
 */
public class OldVCConnection {
   private VimService _vimService;
   private VimPortType _vimPort;
   private ServiceContent _serviceContent;
   private VCCredentials _credentials;
   private boolean _connected;
   private ManagedObjectReference _propertyCollector;
   private ManagedObjectReference _rootFolder;
   private ManagedObjectReference _viewManager;
   private ManagedObjectReference _containerView;
   
   public static class VCCredentials {
      String _hostName;
      String _userName;
      String _password;

      public VCCredentials(String hostName, String userName, String password) {
         _hostName = hostName;
         _userName = userName;
         _password = password;
      }
      
      public String getWsURL() {
         return "https://"+_hostName+"/sdk";
      }
   }

   public OldVCConnection(VCCredentials credentials) {
      _credentials = credentials;
   }
   
   private boolean testConnection() {
      /* Test the connection */
      return (_serviceContent.getRootFolder() != null);
   }
   
   private void initPropertyCollector() {
       if (_propertyCollector == null) {
           try {
               _propertyCollector = _serviceContent.getPropertyCollector();
           } catch (Exception e) {
               e.printStackTrace();
           }
       }
   }
   
   private void initRootFolder() {
       if (_rootFolder == null) {
           try {
               _rootFolder = _serviceContent.getRootFolder();
           } catch (Exception e) {
               e.printStackTrace();
           }
       }
   }

   private void initViewManager() {
       if (_viewManager == null) {
           try {
               _viewManager = _serviceContent.getViewManager();
           } catch (Exception e) {
               e.printStackTrace();
           }
       }
   }
   
   // TODO: Move vc inventory-related methods out of VCConnection to new class VCInventory.
   private void createContainerView(ManagedObjectReference container, List<String> entityTypes,
		                           boolean recurse) {
       try {
           _containerView = _vimPort.createContainerView(_viewManager, container, entityTypes, recurse);
       } catch (Exception e) {
           e.printStackTrace();
       }
   }
   
   private void destroyView() {
       try {
           if (_containerView != null) {
               _vimPort.destroyView(_containerView);
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
   }
   
   public class VMDTO {
	   public ManagedObjectReference vmMOR;
	   public String vmGuestHostName;
   }
   
   public class vmLists {
	   public List<VMDTO> poweredOnVms;
	   public List<VMDTO> poweredOffVms;
	   vmLists() {
		   poweredOnVms = new ArrayList<VMDTO>();
		   poweredOffVms = new ArrayList<VMDTO>();
	   }
   }
   
   public class TTVMInfo {
	   public int numPoweredOn;
	   public int numPoweredOff;
	   public Map<String,vmLists> hostInfoMap;
	   TTVMInfo() {
		   hostInfoMap = new HashMap<String,vmLists>();
		   numPoweredOn = 0;
		   numPoweredOff = 0;
	   }
   }
   
   private TTVMInfo createHostInfoMap(
		   Map<ManagedObjectReference,List<DynamicProperty>> vmInfoMap) {
	    TTVMInfo ttVMInfo = new TTVMInfo();
	    for (Map.Entry<ManagedObjectReference, List<DynamicProperty>> entry
	    		: vmInfoMap.entrySet()) {
	        ManagedObjectReference vmMOR = entry.getKey();
			List<DynamicProperty> dps = entry.getValue();
			String host = null;
			VirtualMachinePowerState powerState = null;
			String guestHostName = null;
            if (dps != null) {
                for (DynamicProperty dp : dps) {
                	if (dp.getName().equalsIgnoreCase("runtime.host")) {
                		ManagedObjectReference hostMOR = (ManagedObjectReference)dp.getVal();
                		host = hostMOR.getValue();
                	} else if (dp.getName().equalsIgnoreCase("runtime.powerState")) {
                		powerState = (VirtualMachinePowerState)dp.getVal();
                	} else if (dp.getName().equalsIgnoreCase("guest.hostName")) {
                		guestHostName = (String)dp.getVal();
                	} else {
                		System.out.println("Warning: Ingoring unexpected property: " + dp.getName());
                	}
                }
                if ((host == null) || (powerState == null)) {
                	System.out.println("Warning: host or powerState not set for VM: " + vmMOR.getValue());
                	continue;
                }
                if (guestHostName == null) {
                	System.out.println("Warning: guest.hostName not set for VM: " + vmMOR.getValue());
                }
            	vmLists vml = ttVMInfo.hostInfoMap.get(host);
            	if (vml == null) {
            		vml = new vmLists();
            	}
            	VMDTO vmDTO = new VMDTO();
            	vmDTO.vmGuestHostName = guestHostName;
            	vmDTO.vmMOR = vmMOR;
                if (powerState.name() == "POWERED_ON") {
                	vml.poweredOnVms.add(vmDTO);
                	ttVMInfo.numPoweredOn++;
                } else if (powerState.name() == "POWERED_OFF") {
                	vml.poweredOffVms.add(vmDTO);
                	ttVMInfo.numPoweredOff++;
                } else {
                	System.out.println("Warning: Unexpected powerState for VM: " + vmMOR.getValue());
                }
            	ttVMInfo.hostInfoMap.put(host, vml);
            }
	    }
        return ttVMInfo;
   }
   
   private Map <ManagedObjectReference,List<DynamicProperty>> createVmInfoMap(ManagedObjectReference folderForVm) {
       Map<ManagedObjectReference,List<DynamicProperty>> vmInfoMap =
    		   new HashMap<ManagedObjectReference,List<DynamicProperty>>();
       
	   List<String> entityType = new ArrayList<String>(Arrays.asList("VirtualMachine"));
       createContainerView(folderForVm, entityType, true);

       List<String> props = new ArrayList<String>(Arrays.asList("runtime.host",
    		   "runtime.powerState", "guest.hostName"));
       List<PropertyFilterSpec> pfsa = createPropertyFilterSpecList(props,"VirtualMachine");
       
       try {
           List<ObjectContent> oca = _vimPort.retrieveProperties(_propertyCollector, pfsa);

           if (oca != null) {
               for (ObjectContent oc : oca) {
                   vmInfoMap.put(oc.getObj(),oc.getPropSet());
               }
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
       destroyView();
       return vmInfoMap;
   }

   
   private ManagedObjectReference findSpecifiedFolder(ManagedObjectReference rootForSearch,
		   String specifiedFolderName) {
	   List<String> entityType = new ArrayList<String>(Arrays.asList("Folder"));
       createContainerView(rootForSearch, entityType, true);
       
       ManagedObjectReference folderMOR = null;
       List<String> props = new ArrayList<String>(Arrays.asList("name", "childType"));
       List<PropertyFilterSpec> pfsa = createPropertyFilterSpecList(props,"Folder");

       try {
           List<ObjectContent> oca = _vimPort.retrieveProperties(_propertyCollector, pfsa);

           if (oca != null) {
               for (ObjectContent oc : oca) {
            	   List<DynamicProperty> dps = oc.getPropSet();
                   if (dps != null) {
                       for (DynamicProperty dp : dps) {
                    	   if (dp.getName().equalsIgnoreCase("name")) {
                    		   String folderName = (String)dp.getVal();
                    		   if (folderName.equalsIgnoreCase(specifiedFolderName)) {
                                   folderMOR = oc.getObj();
                    		   }
                    	   }
                       }
                   }
               }
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
       destroyView();
       return folderMOR;
   }
   
   private List<PropertyFilterSpec> createPropertyFilterSpecList(List<String> props, String type) {
       PropertyFilterSpec pfs = null;
       List<PropertyFilterSpec> pfsa = null;

       try {
           pfs = createPropertyFilterSpec(props, type);
           pfsa = new ArrayList<PropertyFilterSpec>(Arrays.asList(pfs));
       } catch (Exception e) {
           e.printStackTrace();
       }
       return pfsa;
   }
   
   public TraversalSpec getTraversalSpecFromView() {
       // Create a traversal spec that starts from the ListView object
       // and traverses to its "view" property containing the managed object references.

       TraversalSpec viewToObject = new TraversalSpec();
       viewToObject.setName("viewToObject");
       viewToObject.setType("ContainerView");
       viewToObject.setPath("view");
       viewToObject.setSkip(false);

       return viewToObject;
   }

   
   public PropertyFilterSpec createPropertyFilterSpec(List<String> props, String type) {
       PropertyFilterSpec propertyFilterSpec = null;
       try {
           // Create Property Spec
           PropertySpec propertySpec = new PropertySpec();
           propertySpec.setAll(Boolean.FALSE);
           for (String tempProp : props) {
               propertySpec.getPathSet().add(tempProp);
       	   }
           propertySpec.setType(type);
           List<PropertySpec> propertySpecs =
        		   new ArrayList<PropertySpec>(Arrays.asList(propertySpec));

           // Now create Object Spec
           ObjectSpec objectSpec = new ObjectSpec();
           objectSpec.setObj(_containerView);
           TraversalSpec tSpec = getTraversalSpecFromView();
           objectSpec.getSelectSet().add(tSpec);
           List<ObjectSpec> objectSpecs = new ArrayList<ObjectSpec>(Arrays.asList(objectSpec));

           // Create PropertyFilterSpec using the PropertySpec and ObjectPec
           // created above.
           propertyFilterSpec = new PropertyFilterSpec();
           for (PropertySpec tempSpecs : propertySpecs) {
               propertyFilterSpec.getPropSet().add(tempSpecs);
           }
           for (ObjectSpec tempObjs : objectSpecs) {
               propertyFilterSpec.getObjectSet().add(tempObjs);
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
       return propertyFilterSpec;
   }


// Returns data structure representing powered-on & powered-off TTVMs per host
   public TTVMInfo getTTVMsPerHost(String vhClusterName) {
	   TTVMInfo ttVMInfo = null;
	   ManagedObjectReference vhFolder = findSpecifiedFolder(_rootFolder, vhClusterName);
	   if (vhFolder == null) {
		   System.out.println("In getTTVMsPerHost "+vhClusterName+" folder not found");
		   return ttVMInfo;
	   }
	   ManagedObjectReference ttFolder = findSpecifiedFolder(vhFolder, "TTVMs");
	   if (ttFolder == null) {
		   System.out.println("In getTTVMsPerHost TTVMs subfolder not found");
		   return ttVMInfo;
	   }
	   Map <ManagedObjectReference,List<DynamicProperty>> vmInfoMap = createVmInfoMap(ttFolder);
	   ttVMInfo = createHostInfoMap(vmInfoMap);
	   return ttVMInfo;
   }
   // End vc inventory methods
   
   // Begin vc vm power-on/off methods
   public boolean powerOffVM(ManagedObjectReference vm) throws Exception {
       ManagedObjectReference rssTask = _vimPort.powerOffVMTask(vm);
       Object[] result =
               waitForValues(
               rssTask, new String[]{"info.state", "info.error", "info.progress"},
               new String[]{"state"}, // info has a property - state for state of the task
               new Object[][]{new Object[]{TaskInfoState.SUCCESS, TaskInfoState.ERROR}});
       if (result[0].equals(TaskInfoState.SUCCESS)) {
           return true;
       }
       System.out.println("Failure: VM Power Off");
       return false;
   }
   
   public boolean powerOnVM(ManagedObjectReference vm) throws Exception {
       ManagedObjectReference cssTask = _vimPort.powerOnVMTask(vm, null);
       Object[] result =
               waitForValues(
               cssTask, new String[]{"info.state", "info.error", "info.progress"},
               new String[]{"state"}, // info has a property - state for state of the task
               new Object[][]{new Object[]{TaskInfoState.SUCCESS, TaskInfoState.ERROR}});
       if (result[0].equals(TaskInfoState.SUCCESS)) {
           return true;
       }
       System.out.println("Failure: VM Power On");
       return false;
   }

   /************************************************************/
   /**
    *  Handle Updates for a single object.
    *  waits till expected values of properties to check are reached
    *  Destroys the ObjectFilter when done.
    *  @param objmor MOR of the Object to wait for</param>
    *  @param filterProps Properties list to filter
    *  @param endWaitProps
    *    Properties list to check for expected values
    *    these be properties of a property in the filter properties list
    *  @param expectedVals values for properties to end the wait
    *  @return true indicating expected values were met, and false otherwise
    */
   public Object[] waitForValues(
           ManagedObjectReference objmor, String[] filterProps,
           String[] endWaitProps, Object[][] expectedVals) throws Exception {
       // version string is initially null
       String version = "";
       Object[] endVals = new Object[endWaitProps.length];
       Object[] filterVals = new Object[filterProps.length];

       PropertyFilterSpec spec = new PropertyFilterSpec();
       ObjectSpec os = new ObjectSpec();
       os.setObj(objmor);
       os.setSkip(Boolean.FALSE);
       spec.getObjectSet().add(os);

       PropertySpec ps = new PropertySpec();
       ps.getPathSet().addAll(Arrays.asList(filterProps));
       ps.setType(objmor.getType());
       spec.getPropSet().add(ps);

       ManagedObjectReference filterSpecRef = _vimPort.createFilter(_propertyCollector, spec, true);

       boolean reached = false;

       UpdateSet updateset = null;
       List<PropertyFilterUpdate> filtupary = null;
       PropertyFilterUpdate filtup = null;
       List<ObjectUpdate> objupary = null;
       ObjectUpdate objup = null;
       List<PropertyChange> propchgary = null;
       PropertyChange propchg = null;
       while (!reached) {
           updateset = _vimPort.waitForUpdates(_propertyCollector, version);
           version = updateset.getVersion();
           if (updateset == null || updateset.getFilterSet() == null) {
               continue;
           }

           // TODO: Make this code more general purpose when PropCol changes later.
           filtupary = updateset.getFilterSet();
           filtup = null;
           for (int fi = 0; fi < filtupary.size(); fi++) {
               filtup = filtupary.get(fi);
               objupary = filtup.getObjectSet();
               objup = null;
               propchgary = null;
               for (int oi = 0; oi < objupary.size(); oi++) {
                   objup = objupary.get(oi);
                   // TODO: Handle all "kind"s of updates.
                   if (objup.getKind() == ObjectUpdateKind.MODIFY ||
                           objup.getKind() == ObjectUpdateKind.ENTER ||
                           objup.getKind() == ObjectUpdateKind.LEAVE) {
                       propchgary = objup.getChangeSet();
                       for (int ci = 0; ci < propchgary.size(); ci++) {
                           propchg = propchgary.get(ci);
                           updateValues(endWaitProps, endVals, propchg);
                           updateValues(filterProps, filterVals, propchg);
                       }
                   }
               }
           }

           Object expctdval = null;
           // Check if the expected values have been reached and exit the loop if done.
           // Also exit the WaitForUpdates loop if this is the case.
           for (int chgi = 0; chgi < endVals.length && !reached; chgi++) {
               for (int vali = 0; vali < expectedVals[chgi].length && !reached; vali++) {
                   expctdval = expectedVals[chgi][vali];
                   reached = expctdval.equals(endVals[chgi]) || reached;
               }
           }
       }

       // Destroy the filter when we are done.
       _vimPort.destroyPropertyFilter(filterSpecRef);
       return filterVals;
   }  
   
   private void updateValues(String[] props, Object[] vals, PropertyChange propchg) {
       for (int findi = 0; findi < props.length; findi++) {
           if (propchg.getName().lastIndexOf(props[findi]) >= 0) {
               if (propchg.getOp() == PropertyChangeOp.REMOVE) {
                   vals[findi] = "";
               } else {
                   vals[findi] = propchg.getVal();
               //System.out.println("Changed value : " + propchg.getVal());
               }
           }
       }
   }

   // End vc vm power-on/off methods
   
   public boolean connect() throws SOAPFaultException {
      if (_connected) {
         if (testConnection()) {
            return true;
         } else {
            /* Refresh */
            disconnect();
         }
      }

      ManagedObjectReference svcInstRef = new ManagedObjectReference();
      svcInstRef.setType("ServiceInstance");
      svcInstRef.setValue("ServiceInstance");

      _vimService = new VimService();
      _vimPort = _vimService.getVimPort();
      Map<String, Object> ctxt = ((BindingProvider) _vimPort).getRequestContext();

      ctxt.put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, _credentials.getWsURL());
      ctxt.put(BindingProvider.SESSION_MAINTAIN_PROPERTY, true);

      try {
         _serviceContent = _vimPort.retrieveServiceContent(svcInstRef);
         _vimPort.login(_serviceContent.getSessionManager(), _credentials._userName, _credentials._password, null);
         _connected = true;
      } catch (RuntimeFaultFaultMsg e) {
         throw new RuntimeException("Unexpected Connect Exception", e);
      } catch (InvalidLocaleFaultMsg e) {
         throw new RuntimeException("Unexpected Connect Exception", e);
      } catch (InvalidLoginFaultMsg e) {
         throw new RuntimeException("Unexpected Connect Exception", e);
      }
      
      initPropertyCollector();
      initRootFolder();
      initViewManager();
      return testConnection();
   }
   
   public void disconnect() throws SOAPFaultException {
      if (_connected) {
         try {
            _vimPort.logout(_serviceContent.getSessionManager());
            _connected = false;
         } catch (RuntimeFaultFaultMsg e) {
            throw new RuntimeException("Unexpected Disconnect Exception", e);
         }
      }
   }
}

